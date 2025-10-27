"""
Pool filtering for arbitrage pipeline.

Implements two-stage pool filtering:
- Stage 1: whitelisted_token + trusted_token pairs (lower liquidity threshold)
- Stage 2: whitelisted_token + any_token (higher threshold, requires trust path)
"""

import asyncio
from decimal import Decimal
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
import logging

from web3 import Web3

logger = logging.getLogger(__name__)

from src.core.storage.postgres import PostgresStorage
from src.config import ConfigManager
from src.whitelist.liquidity_filter import PoolLiquidityFilter


@dataclass
class PoolInfo:
    """Pool information."""
    pool_address: str
    token0: str
    token1: str
    protocol: str
    factory: str
    liquidity: Optional[Decimal] = None
    fee: Optional[int] = None
    tick_spacing: Optional[int] = None
    block_number: Optional[int] = None


@dataclass
class TokenPrice:
    """Token price snapshot."""
    token_address: str
    price_in_trusted: Decimal
    trusted_token: str
    pool_address: str
    liquidity: Decimal


class PoolFilter:
    """Filter DEX pools for arbitrage opportunities."""

    def __init__(
        self,
        storage: PostgresStorage,
        whitelisted_tokens: Set[str],
        trusted_tokens: Dict[str, str],
        config: Optional[ConfigManager] = None,
        rpc_url: Optional[str] = None,
        chain: str = "ethereum"
    ):
        """
        Initialize pool filter.

        Args:
            storage: Database storage instance
            whitelisted_tokens: Set of whitelisted token addresses (lowercase)
            trusted_tokens: Dict mapping token name to address
            config: Configuration manager (optional)
            rpc_url: Web3 RPC endpoint (optional, will use config if not provided)
            chain: Blockchain name (default: "ethereum")
        """
        self.storage = storage
        self.whitelisted_tokens = {addr.lower() for addr in whitelisted_tokens}
        self.trusted_tokens = {
            name: addr.lower() for name, addr in trusted_tokens.items()
        }
        self.trusted_addresses = set(self.trusted_tokens.values())
        self.config = config or ConfigManager()
        self.chain = chain

        # Get chain_id from config for table naming
        chain_config = self.config.chains.get_chain_config(chain)
        self.chain_id = chain_config["chain_id"]

        # Initialize Web3 for liquidity filtering
        if rpc_url is None:
            rpc_url = self.config.chains.get_rpc_url(chain)
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        self.liquidity_filter: Optional[PoolLiquidityFilter] = None

        # Build factory to protocol mapping
        self.factory_to_protocol = self._build_factory_mapping()

    def _build_factory_mapping(self) -> Dict[str, str]:
        """Build mapping of factory addresses to protocol names."""
        mapping = {}

        protocols = ["uniswap_v2", "uniswap_v3", "uniswap_v4"]
        chain = "ethereum"  # We're on ethereum network_1

        for protocol in protocols:
            try:
                factories = self.config.protocols.get_factory_addresses(protocol, chain)
                for factory in factories:
                    mapping[factory.lower()] = protocol
            except Exception as e:
                logger.warning(f"Could not get factories for {protocol}: {e}")

        logger.info(f"Built factory mapping for {len(mapping)} factories")
        return mapping

    def _identify_protocol(self, factory: str) -> str:
        """Identify protocol from factory address."""
        factory_lower = factory.lower()
        return self.factory_to_protocol.get(factory_lower, "unknown")

    async def get_stage1_pools(
        self,
        liquidity_threshold: Decimal
    ) -> List[PoolInfo]:
        """
        Stage 1: Get pools with whitelisted_token + trusted_token pairs.

        Args:
            liquidity_threshold: Minimum liquidity in USD

        Returns:
            List of pools matching criteria
        """
        whitelisted_tuple = tuple(self.whitelisted_tokens)
        trusted_tuple = tuple(self.trusted_addresses)

        # Get all factory addresses for all protocols
        all_factories = []
        for protocol in ["uniswap_v2", "uniswap_v3", "uniswap_v4"]:
            factories = self.config.protocols.get_factory_addresses(protocol, "ethereum")
            all_factories.extend([f.lower() for f in factories])

        factories_tuple = tuple(all_factories)

        # Query BOTH tables: original and cryo-fetched
        # Filter by factory FIRST (most selective), then by tokens to prevent full table scan
        table1 = f"network_{self.chain_id}__dex_pools"
        table2 = f"network_{self.chain_id}_dex_pools_cryo"

        query = f"""
        SELECT DISTINCT
            address as pool_address,
            asset0 as token0,
            asset1 as token1,
            factory,
            fee,
            tick_spacing,
            creation_block as block_number
        FROM (
            SELECT address, asset0, asset1, factory, fee, tick_spacing, creation_block
            FROM {table1}
            WHERE LOWER(factory) = ANY($1)
            AND (
                (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($3))
                OR
                (LOWER(asset0) = ANY($3) AND LOWER(asset1) = ANY($2))
            )
            UNION
            SELECT address, asset0, asset1, factory, fee, tick_spacing, creation_block
            FROM {table2}
            WHERE LOWER(factory) = ANY($1)
            AND (
                (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($3))
                OR
                (LOWER(asset0) = ANY($3) AND LOWER(asset1) = ANY($2))
            )
        ) AS combined_pools
        ORDER BY creation_block DESC
        """

        logger.info(f"🔍 Querying Stage 1 from {table1} + {table2}")
        logger.info(f"   {len(whitelisted_tuple)} whitelisted × {len(trusted_tuple)} trusted tokens across {len(factories_tuple)} factories...")

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query, factories_tuple, whitelisted_tuple, trusted_tuple)

        pools = []
        for row in results:
            protocol = self._identify_protocol(row['factory'])

            pool = PoolInfo(
                pool_address=row['pool_address'],
                token0=row['token0'].lower(),
                token1=row['token1'].lower(),
                protocol=protocol,
                factory=row['factory'].lower(),
                fee=row.get('fee'),
                tick_spacing=row.get('tick_spacing'),
                block_number=row.get('block_number')
            )
            pools.append(pool)

        logger.info(f"📊 Stage 1: Found {len(pools)} pools with whitelisted + trusted tokens")
        return pools

    async def _get_token_info(self, token_addresses: List[str]) -> Tuple[Dict[str, int], Dict[str, str]]:
        """
        Fetch token decimals and symbols from database.

        Args:
            token_addresses: List of token addresses

        Returns:
            Tuple of (decimals dict, symbols dict) - both mapping address to value
        """
        if not token_addresses:
            return {}, {}

        query = """
        SELECT
            LOWER(p.address) as address,
            p.decimals,
            t.symbol
        FROM coingecko_token_platforms p
        JOIN coingecko_tokens t ON p.token_id = t.id
        WHERE p.platform = 'ethereum'
        AND LOWER(p.address) = ANY($1)
        AND p.decimals IS NOT NULL
        """

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query, [addr.lower() for addr in token_addresses])

        decimals = {row['address']: row['decimals'] for row in results}
        symbols = {row['address']: row['symbol'] for row in results}
        return decimals, symbols

    def _pools_to_dict(self, pools: List[PoolInfo]) -> Dict[str, Dict]:
        """
        Convert List[PoolInfo] to Dict format expected by PoolLiquidityFilter.

        Args:
            pools: List of PoolInfo objects

        Returns:
            Dict mapping pool address to pool data
        """
        result = {}
        for pool in pools:
            result[pool.pool_address] = {
                'token0': {'address': pool.token0},
                'token1': {'address': pool.token1},
                'factory': pool.factory,
                'fee': pool.fee,
                'tick_spacing': pool.tick_spacing,
            }
        return result

    def _dict_to_pool(self, pool_address: str, data: Dict) -> PoolInfo:
        """
        Convert Dict format from PoolLiquidityFilter back to PoolInfo.

        Args:
            pool_address: Pool address
            data: Pool data dict

        Returns:
            PoolInfo object
        """
        # Determine protocol from factory
        factory = data.get('factory', '').lower()
        protocol = self.factory_to_protocol.get(factory, 'unknown')

        return PoolInfo(
            pool_address=pool_address,
            token0=data['token0']['address'],
            token1=data['token1']['address'],
            protocol=protocol,
            factory=factory,
            liquidity=Decimal(str(data.get('liquidity', 0))) if data.get('liquidity') else None,
            fee=data.get('fee'),
            tick_spacing=data.get('tick_spacing'),
            block_number=None
        )

    async def filter_by_liquidity(
        self,
        pools: List[PoolInfo],
        threshold: Decimal,
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> List[PoolInfo]:
        """
        Filter pools by liquidity threshold using on-chain data.

        Args:
            pools: List of pools to filter
            threshold: Minimum liquidity in USD
            token_prices: Optional pre-calculated token prices (address -> price in USD)

        Returns:
            Filtered list of pools with liquidity data
        """
        if not pools:
            return []

        logger.info(f"💰 Filtering {len(pools)} pools by liquidity (threshold: ${threshold})")

        # Initialize liquidity filter if needed
        if self.liquidity_filter is None:
            # Use lower threshold for V2 (more pools), higher for V3/V4 (concentrated liquidity)
            v2_threshold = float(threshold) / 5  # V2 threshold is 20% of main threshold
            self.liquidity_filter = PoolLiquidityFilter(
                web3=self.web3,
                min_liquidity_usd=float(threshold),  # For V3/V4
                min_liquidity_v2_usd=v2_threshold,   # Lower for V2
                chain=self.chain
            )
            logger.info(f"  V2 threshold: ${v2_threshold:,.0f}, V3/V4 threshold: ${threshold}")

        # Separate pools by protocol
        v2_pools = []
        v3_pools = []
        v4_pools = []

        for pool in pools:
            protocol = pool.protocol.lower()
            if "v2" in protocol or "aero" in protocol or "pancake" in protocol:
                v2_pools.append(pool)
            elif "v3" in protocol:
                v3_pools.append(pool)
            elif "v4" in protocol:
                v4_pools.append(pool)

        # Fetch Hyperliquid prices (symbol -> price)
        hyperliquid_symbol_prices = await self.liquidity_filter.fetch_hyperliquid_prices()

        # Get token decimals and symbols from database
        all_token_addresses = set()
        for pool in pools:
            all_token_addresses.add(pool.token0.lower())
            all_token_addresses.add(pool.token1.lower())

        token_decimals, token_symbols = await self._get_token_info(list(all_token_addresses))

        # Convert address-based prices to symbol-based prices for compatibility
        # PoolLiquidityFilter expects hyperliquid_symbol_prices (symbol -> price)
        # If token_prices provided, it should be address -> price

        # Filter each protocol's pools
        # Price discovery cascade: Hyperliquid → V2 → V3 → V4
        # Each protocol adds to the discovered_prices dict (address → price)
        filtered_pools = []
        discovered_prices = {}  # address -> price

        if v2_pools:
            logger.info(f"  Filtering {len(v2_pools)} V2 pools...")
            # Convert List[PoolInfo] to Dict[address, data]
            v2_dict = self._pools_to_dict(v2_pools)
            # V2 takes symbol-based Hyperliquid prices and converts to address-based
            filtered_v2_dict, updated_prices = await self.liquidity_filter.filter_v2_pools(
                v2_dict, token_symbols, token_decimals, hyperliquid_symbol_prices
            )
            # Convert back to List[PoolInfo]
            for addr, data in filtered_v2_dict.items():
                pool = self._dict_to_pool(addr, data)
                filtered_pools.append(pool)
            discovered_prices.update(updated_prices)  # Now has address-based prices

        if v3_pools:
            logger.info(f"  Filtering {len(v3_pools)} V3 pools...")
            v3_dict = self._pools_to_dict(v3_pools)
            # V3 receives address-based prices from V2 + Hyperliquid
            filtered_v3_dict, updated_prices = await self.liquidity_filter.filter_v3_pools(
                v3_dict, token_symbols, token_decimals, discovered_prices
            )
            for addr, data in filtered_v3_dict.items():
                pool = self._dict_to_pool(addr, data)
                filtered_pools.append(pool)
            discovered_prices.update(updated_prices)

        if v4_pools:
            logger.info(f"  Filtering {len(v4_pools)} V4 pools...")
            v4_dict = self._pools_to_dict(v4_pools)
            # V4 receives accumulated prices from V2 + V3 + Hyperliquid
            filtered_v4_dict, updated_prices = await self.liquidity_filter.filter_v4_pools(
                v4_dict, token_symbols, token_decimals, discovered_prices
            )
            for addr, data in filtered_v4_dict.items():
                pool = self._dict_to_pool(addr, data)
                filtered_pools.append(pool)
            discovered_prices.update(updated_prices)

        logger.info(f"✅ Filtered to {len(filtered_pools)} pools above ${threshold} liquidity")
        return filtered_pools

    async def calculate_token_prices(
        self,
        stage1_pools: List[PoolInfo]
    ) -> Dict[str, TokenPrice]:
        """
        Calculate snapshot prices for whitelisted tokens from Stage 1 pools.

        Args:
            stage1_pools: Pools from Stage 1

        Returns:
            Dict mapping token address to price info
        """
        token_prices = {}

        for pool in stage1_pools:
            # Determine which token is whitelisted and which is trusted
            whitelisted_token = None
            trusted_token = None

            if pool.token0 in self.whitelisted_tokens:
                whitelisted_token = pool.token0
                trusted_token = pool.token1
            elif pool.token1 in self.whitelisted_tokens:
                whitelisted_token = pool.token1
                trusted_token = pool.token0

            if not whitelisted_token or trusted_token not in self.trusted_addresses:
                continue

            # TODO: Calculate actual price from pool state
            # For now, create placeholder
            if whitelisted_token not in token_prices:
                token_prices[whitelisted_token] = TokenPrice(
                    token_address=whitelisted_token,
                    price_in_trusted=Decimal("0"),  # TODO: Calculate from pool
                    trusted_token=trusted_token,
                    pool_address=pool.pool_address,
                    liquidity=pool.liquidity or Decimal("0")
                )

        logger.info(f"💰 Calculated prices for {len(token_prices)} whitelisted tokens")
        return token_prices

    async def get_stage2_pools(
        self,
        liquidity_threshold: Decimal,
        token_prices: Dict[str, TokenPrice]
    ) -> List[PoolInfo]:
        """
        Stage 2: Get pools with whitelisted_token + any_token.

        The "any_token" must have a pool with a trusted_token above liquidity threshold.

        Args:
            liquidity_threshold: Minimum liquidity (higher than Stage 1)
            token_prices: Token prices from Stage 1

        Returns:
            List of pools with verified trust paths
        """
        whitelisted_tuple = tuple(self.whitelisted_tokens)

        # Get all factory addresses for all protocols
        all_factories = []
        for protocol in ["uniswap_v2", "uniswap_v3", "uniswap_v4"]:
            factories = self.config.protocols.get_factory_addresses(protocol, "ethereum")
            all_factories.extend([f.lower() for f in factories])

        factories_tuple = tuple(all_factories)

        # Query BOTH tables for Stage 2
        table1 = f"network_{self.chain_id}__dex_pools"
        table2 = f"network_{self.chain_id}_dex_pools_cryo"

        query = f"""
        SELECT DISTINCT
            address as pool_address,
            asset0 as token0,
            asset1 as token1,
            factory,
            fee,
            tick_spacing,
            creation_block as block_number
        FROM (
            SELECT address, asset0, asset1, factory, fee, tick_spacing, creation_block
            FROM {table1}
            WHERE LOWER(factory) = ANY($1)
            AND (LOWER(asset0) = ANY($2) OR LOWER(asset1) = ANY($2))
            UNION
            SELECT address, asset0, asset1, factory, fee, tick_spacing, creation_block
            FROM {table2}
            WHERE LOWER(factory) = ANY($1)
            AND (LOWER(asset0) = ANY($2) OR LOWER(asset1) = ANY($2))
        ) AS combined_pools
        ORDER BY creation_block DESC
        LIMIT 10000
        """

        logger.info(f"🔍 Querying Stage 2 from {table1} + {table2}")
        logger.info(f"   {len(whitelisted_tuple)} whitelisted tokens across {len(factories_tuple)} factories (limit 10k)...")

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query, factories_tuple, whitelisted_tuple)

        # Filter to find pools where the "other token" has a trust path
        verified_pools = []

        for row in results:
            token0 = row['token0'].lower()
            token1 = row['token1'].lower()

            # Determine whitelisted and other token
            whitelisted_token = None
            other_token = None

            if token0 in self.whitelisted_tokens:
                whitelisted_token = token0
                other_token = token1
            elif token1 in self.whitelisted_tokens:
                whitelisted_token = token1
                other_token = token0

            # Skip if both are whitelisted or trusted (already in Stage 1)
            if other_token in self.whitelisted_tokens or other_token in self.trusted_addresses:
                continue

            # Verify other_token has a pool with a trusted_token
            has_trust_path = await self._verify_trust_path(other_token, liquidity_threshold)

            if has_trust_path:
                protocol = self._identify_protocol(row['factory'])

                pool = PoolInfo(
                    pool_address=row['pool_address'],
                    token0=token0,
                    token1=token1,
                    protocol=protocol,
                    factory=row['factory'].lower(),
                    fee=row.get('fee'),
                    tick_spacing=row.get('tick_spacing'),
                    block_number=row.get('block_number')
                )
                verified_pools.append(pool)

        logger.info(f"📊 Stage 2: Found {len(verified_pools)} pools with verified trust paths")
        return verified_pools

    async def _verify_trust_path(
        self,
        token: str,
        liquidity_threshold: Decimal
    ) -> bool:
        """
        Verify that a token has a pool with a trusted token above liquidity threshold.

        Args:
            token: Token address to check
            liquidity_threshold: Minimum liquidity required

        Returns:
            True if trust path exists
        """
        trusted_tuple = tuple(self.trusted_addresses)

        # Query BOTH tables for trust path verification
        table1 = f"network_{self.chain_id}__dex_pools"
        table2 = f"network_{self.chain_id}_dex_pools_cryo"

        query = f"""
        SELECT COUNT(*) as count
        FROM (
            SELECT address FROM {table1}
            WHERE (
                (LOWER(asset0) = $1 AND LOWER(asset1) = ANY($2))
                OR
                (LOWER(asset1) = $1 AND LOWER(asset0) = ANY($2))
            )
            UNION
            SELECT address FROM {table2}
            WHERE (
                (LOWER(asset0) = $1 AND LOWER(asset1) = ANY($2))
                OR
                (LOWER(asset1) = $1 AND LOWER(asset0) = ANY($2))
            )
        ) AS combined_pools
        """

        async with self.storage.pool.acquire() as conn:
            result = await conn.fetchval(query, token, trusted_tuple)

        # TODO: Also check liquidity threshold
        return result > 0

    async def run_filtering_pipeline(
        self,
        stage1_liquidity: Decimal,
        stage2_liquidity: Decimal
    ) -> Tuple[List[PoolInfo], List[PoolInfo], Dict[str, TokenPrice]]:
        """
        Run the complete two-stage filtering pipeline.

        Args:
            stage1_liquidity: Liquidity threshold for Stage 1
            stage2_liquidity: Liquidity threshold for Stage 2 (should be higher)

        Returns:
            Tuple of (stage1_pools, stage2_pools, token_prices)
        """
        logger.info("="*70)
        logger.info("🔄 Running Pool Filtering Pipeline")
        logger.info("="*70)

        # Stage 1: Whitelist + Trusted token pairs
        logger.info("📍 Stage 1: Whitelist + Trusted Token Pairs")
        stage1_pools = await self.get_stage1_pools(stage1_liquidity)
        stage1_pools = await self.filter_by_liquidity(stage1_pools, stage1_liquidity)

        # Calculate token prices
        token_prices = await self.calculate_token_prices(stage1_pools)

        # Stage 2: Whitelist + Any token (with trust path)
        # TEMPORARILY DISABLED for faster testing
        logger.info("📍 Stage 2: DISABLED (uncomment to enable)")
        stage2_pools = []
        # stage2_pools = await self.get_stage2_pools(stage2_liquidity, token_prices)
        # stage2_pools = await self.filter_by_liquidity(stage2_pools, stage2_liquidity)

        logger.info("="*70)
        logger.info("✅ Pipeline Complete")
        logger.info(f"  Stage 1 pools: {len(stage1_pools)}")
        logger.info(f"  Stage 2 pools: {len(stage2_pools)} (DISABLED)")
        logger.info(f"  Total pools: {len(stage1_pools) + len(stage2_pools)}")
        logger.info("="*70)

        return stage1_pools, stage2_pools, token_prices
