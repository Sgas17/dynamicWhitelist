"""
Build token whitelist based on multiple criteria:
1. Cross-chain tokens (on 2+ of: base, ethereum, arbitrum) - from coingecko_token_platforms table
2. Tokens listed on perp DEXs (Hyperliquid, Lighter)
3. Top X transferred tokens on Ethereum - from database transfer events
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Set

import aiohttp

logger = logging.getLogger(__name__)

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Parent reference for the whitelist module
WHITELIST_DIR = Path(__file__).parent

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.fetchers.exchange_fetchers import HyperliquidFetcher


class TokenWhitelistBuilder:
    """Build comprehensive token whitelist from multiple sources."""

    def __init__(self, storage: PostgresStorage):
        """Initialize the whitelist builder."""
        self.config = ConfigManager()
        self.storage = storage
        self.whitelist_tokens: Set[str] = set()
        self.token_sources: Dict[str, List[str]] = {}  # token -> [sources]
        self.token_info: Dict[str, Dict] = {}  # token -> {coingecko_id, symbol, etc}
        self.unmapped_hyperliquid: Dict[
            str, str
        ] = {}  # symbol -> name (not found in DB)
        self.unmapped_lighter: Dict[str, str] = {}  # symbol -> name (not found in DB)

    async def get_cross_chain_tokens(self) -> Set[str]:
        """
        Get tokens that exist on 2+ chains (base, ethereum, arbitrum).

        Queries coingecko_token_platforms table to find tokens present on multiple chains.

        Returns:
            Set of token addresses (lowercase) from Ethereum
        """
        print("🔗 Finding cross-chain tokens from coingecko_token_platforms...")

        query = """
        WITH platform_counts AS (
            SELECT
                p.token_id,
                COUNT(DISTINCT p.platform) as chain_count
            FROM coingecko_token_platforms p
            WHERE p.platform IN ('ethereum', 'base', 'arbitrum-one')
            AND p.address IS NOT NULL
            AND p.address != ''
            AND p.address != '0x'
            GROUP BY p.token_id
            HAVING COUNT(DISTINCT p.platform) >= 2
        )
        SELECT
            p.token_id,
            t.symbol,
            p.platform,
            p.address,
            p.decimals,
            pc.chain_count
        FROM coingecko_token_platforms p
        JOIN coingecko_tokens t ON p.token_id = t.id
        JOIN platform_counts pc ON p.token_id = pc.token_id
        WHERE p.platform IN ('ethereum', 'base', 'arbitrum-one')
        AND p.address IS NOT NULL
        AND p.address != ''
        AND p.address != '0x'
        ORDER BY p.token_id, p.platform
        """

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query)

        # Group by token_id to see all chains for each token
        token_data = {}
        for row in results:
            token_id = row["token_id"]
            symbol = row["symbol"]
            platform = row["platform"]
            address = row["address"].lower() if row["address"] else None
            decimals = row["decimals"]
            chain_count = row["chain_count"]

            if token_id not in token_data:
                token_data[token_id] = {
                    "symbol": symbol,
                    "decimals": decimals,
                    "chains": {},
                    "chain_count": chain_count,
                }

            if address:
                token_data[token_id]["chains"][platform] = address

        # Extract Ethereum addresses for tokens on 2+ chains
        cross_chain_tokens = set()
        for token_id, data in token_data.items():
            chains = data["chains"]
            chain_count = data["chain_count"]
            symbol = data["symbol"]
            decimals = data["decimals"]

            # Get Ethereum address as the canonical one
            eth_address = chains.get("ethereum")
            if eth_address:
                cross_chain_tokens.add(eth_address)
                self.token_info[eth_address] = {
                    "coingecko_id": token_id,
                    "symbol": symbol,
                    "decimals": decimals,
                    "chains": list(chains.keys()),
                }
                chain_names = ", ".join(chains.keys())
                logger.debug(
                    f"  ✓ {symbol} ({token_id}): {eth_address[:10]}... (on {chain_count} chains: {chain_names})"
                )

        logger.info(f"  Found {len(cross_chain_tokens)} cross-chain tokens")
        return cross_chain_tokens

    async def get_hyperliquid_tokens(self) -> Set[str]:
        """
        Get tokens from Hyperliquid perp DEX and map to Ethereum addresses.

        Returns:
            Set of Ethereum token addresses
        """
        print("\n💱 Fetching Hyperliquid tokens...")

        try:
            fetcher = HyperliquidFetcher()
            result = await fetcher.fetch_markets(market_type="swap")

            if not result.success:
                logger.error(f"   Failed to fetch Hyperliquid markets: {result.error}")
                return set()

            tokens_data = result.metadata.get("tokens", [])
            hyperliquid_symbols = set()

            for token in tokens_data:
                base = token.base.upper()
                hyperliquid_symbols.add(base)

            logger.info(f"  Found {len(hyperliquid_symbols)} symbols on Hyperliquid")

            # Map symbols to Ethereum addresses using coingecko_token_platforms
            print(f"\n🔄 Mapping Hyperliquid symbols to Ethereum addresses...")

            # Build search symbols: include both original and "k" tokens mapped to base
            search_symbols = set()
            k_token_mapping = {}  # Maps base symbol -> k symbol for logging

            for symbol in hyperliquid_symbols:
                search_symbols.add(symbol)
                # Handle "k" tokens (1000x lots): KSHIB -> SHIB, KPEPE -> PEPE, etc.
                if symbol.startswith("K") and len(symbol) > 1:
                    base_symbol = symbol[1:]  # Remove 'K' prefix
                    search_symbols.add(base_symbol)
                    k_token_mapping[base_symbol] = symbol

            symbols_tuple = tuple(search_symbols)
            query = """
            SELECT DISTINCT
                t.symbol,
                p.address,
                p.token_id,
                p.decimals
            FROM coingecko_token_platforms p
            JOIN coingecko_tokens t ON p.token_id = t.id
            WHERE p.platform = 'ethereum'
            AND UPPER(t.symbol) = ANY($1)
            AND p.address IS NOT NULL
            AND p.address != ''
            AND p.address != '0x'
            """

            async with self.storage.pool.acquire() as conn:
                results = await conn.fetch(query, symbols_tuple)

            hyperliquid_addresses = set()
            mapped_symbols = set()

            for row in results:
                symbol = row["symbol"]
                address = row["address"].lower()
                token_id = row["token_id"]
                decimals = row["decimals"]

                hyperliquid_addresses.add(address)

                # Track both original and k-token as mapped
                mapped_symbols.add(symbol.upper())
                if symbol.upper() in k_token_mapping:
                    k_symbol = k_token_mapping[symbol.upper()]
                    mapped_symbols.add(k_symbol)
                    logger.debug(
                        f"  ✓ {k_symbol} (k-token) -> {symbol} -> {address[:10]}..."
                    )

                if address not in self.token_info:
                    self.token_info[address] = {
                        "coingecko_id": token_id,
                        "symbol": symbol,
                        "decimals": decimals,
                        "chains": ["ethereum"],
                    }

                logger.debug(f"  ✓ {symbol} -> {address[:10]}...")

            unmapped = hyperliquid_symbols - mapped_symbols
            if unmapped:
                # Store unmapped for manual mapping
                for symbol in unmapped:
                    token_data = next(
                        (t for t in tokens_data if t.base.upper() == symbol), None
                    )
                    if token_data:
                        self.unmapped_hyperliquid[symbol] = token_data.base

                print(
                    f"\n   ⚠️  Unmapped symbols ({len(unmapped)}): {', '.join(sorted(list(unmapped))[:20])}{'...' if len(unmapped) > 20 else ''}"
                )

            print(
                f"   Mapped {len(hyperliquid_addresses)} of {len(hyperliquid_symbols)} symbols"
            )
            return hyperliquid_addresses

        except Exception as e:
            logger.error(f"   Error fetching Hyperliquid tokens: {e}")
            import traceback

            traceback.print_exc()
            return set()

    async def get_lighter_tokens(self) -> Set[str]:
        """
        Get tokens from Lighter perp DEX using their API and map to Ethereum addresses.

        API: https://apidocs.lighter.xyz/reference/funding-rates

        Returns:
            Set of Ethereum token addresses
        """
        logger.info("⚡ Fetching Lighter tokens...")

        try:
            # Correct Lighter API endpoint from official docs
            url = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        logger.error(
                            f"Failed to fetch from Lighter API: HTTP {response.status}"
                        )
                        return set()

                    data = await response.json()
                    logger.info(f"✓ Successfully connected to Lighter API")

            lighter_symbols = set()

            # Parse the response - API returns {"code": 200, "funding_rates": [...]}
            if isinstance(data, dict) and "funding_rates" in data:
                funding_rates = data["funding_rates"]
                for item in funding_rates:
                    symbol = item.get("symbol", "")
                    if symbol:
                        # Symbol is already just the base token (e.g., "BTC", "ETH", "LINK")
                        lighter_symbols.add(symbol.upper())

            logger.info(f"  Found {len(lighter_symbols)} symbols on Lighter")

            # Map to Ethereum addresses using coingecko_token_platforms
            if lighter_symbols:
                print(f"\n🔄 Mapping Lighter symbols to Ethereum addresses...")

                # Build search symbols: include both original and lot-based tokens mapped to base
                search_symbols = set()
                lot_token_mapping = {}  # Maps base symbol -> lot symbol for logging

                for symbol in lighter_symbols:
                    search_symbols.add(symbol)

                    # Handle "1000" prefix (1000x lots): 1000SHIB -> SHIB, 1000PEPE -> PEPE, etc.
                    if symbol.startswith("1000") and len(symbol) > 4:
                        base_symbol = symbol[4:]  # Remove '1000' prefix
                        search_symbols.add(base_symbol)
                        lot_token_mapping[base_symbol] = symbol

                    # Handle "K" prefix (1000x lots): KSHIB -> SHIB, KPEPE -> PEPE, etc.
                    elif symbol.startswith("K") and len(symbol) > 1:
                        base_symbol = symbol[1:]  # Remove 'K' prefix
                        search_symbols.add(base_symbol)
                        lot_token_mapping[base_symbol] = symbol

                symbols_tuple = tuple(search_symbols)
                query = """
                SELECT DISTINCT
                    t.symbol,
                    p.address,
                    p.token_id,
                    p.decimals
                FROM coingecko_token_platforms p
                JOIN coingecko_tokens t ON p.token_id = t.id
                WHERE p.platform = 'ethereum'
                AND UPPER(t.symbol) = ANY($1)
                AND p.address IS NOT NULL
                AND p.address != ''
                AND p.address != '0x'
                """

                async with self.storage.pool.acquire() as conn:
                    results = await conn.fetch(query, symbols_tuple)

                lighter_addresses = set()
                mapped_symbols = set()

                for row in results:
                    symbol = row["symbol"]
                    address = row["address"].lower()
                    token_id = row["token_id"]
                    decimals = row["decimals"]

                    lighter_addresses.add(address)

                    # Track both original and lot-based token as mapped
                    mapped_symbols.add(symbol.upper())
                    if symbol.upper() in lot_token_mapping:
                        lot_symbol = lot_token_mapping[symbol.upper()]
                        mapped_symbols.add(lot_symbol)
                        logger.debug(
                            f"  ✓ {lot_symbol} (lot-token) -> {symbol} -> {address[:10]}..."
                        )

                    if address not in self.token_info:
                        self.token_info[address] = {
                            "coingecko_id": token_id,
                            "symbol": symbol,
                            "decimals": decimals,
                            "chains": ["ethereum"],
                        }

                    logger.debug(f"  ✓ {symbol} -> {address[:10]}...")

                unmapped = lighter_symbols - mapped_symbols
                if unmapped:
                    # Store unmapped for manual mapping
                    for symbol in unmapped:
                        self.unmapped_lighter[symbol] = symbol

                    print(
                        f"\n   ⚠️  Unmapped symbols: {', '.join(sorted(list(unmapped))[:10])}{'...' if len(unmapped) > 10 else ''}"
                    )

                print(
                    f"   Mapped {len(lighter_addresses)} of {len(lighter_symbols)} symbols"
                )
                return lighter_addresses

            return set()

        except Exception as e:
            logger.error(f"   Error fetching Lighter tokens: {e}")
            import traceback

            traceback.print_exc()
            return set()

    async def get_top_transferred_tokens(self, top_n: int = 100) -> Set[str]:
        """
        Get top N transferred tokens on Ethereum by ranking score.

        Queries transfers service database (separate microservice) for top tokens
        based on composite ranking that includes transfer counts, unique addresses,
        and activity metrics over 24h and 7d windows.

        Args:
            top_n: Number of top tokens to return

        Returns:
            Set of token addresses (lowercase)
        """
        print(f"\n📊 Getting top {top_n} transferred tokens from transfers service...")

        try:
            import asyncpg

            # Connect to transfers service database (separate from main database)
            transfers_db_config = {
                "host": "localhost",
                "port": 5433,  # Transfers service uses different port
                "database": "transfers",
                "user": "transfers_user",
                "password": "transfers_pass",
            }

            # Query the materialized view for fast performance
            query = """
            SELECT
                token_address,
                transfer_count_24h,
                unique_senders_24h,
                unique_receivers_24h,
                ranking_score,
                last_updated
            FROM top_transferred_tokens
            WHERE rank <= $1
            ORDER BY rank
            """

            conn = await asyncpg.connect(**transfers_db_config)
            try:
                results = await conn.fetch(query, top_n)
            finally:
                await conn.close()

            top_tokens = set()
            for row in results:
                token_address = row["token_address"].lower()
                transfer_count = int(row["transfer_count_24h"])
                ranking_score = float(row["ranking_score"])
                top_tokens.add(token_address)

                # Add to token_info if not already there
                if token_address not in self.token_info:
                    self.token_info[token_address] = {
                        "transfer_count_24h": transfer_count,
                        "unique_senders_24h": int(row["unique_senders_24h"]),
                        "unique_receivers_24h": int(row["unique_receivers_24h"]),
                        "ranking_score": ranking_score,
                        "last_updated": str(row["last_updated"]),
                    }

                logger.debug(
                    f"  ✓ {token_address[:10]}... - "
                    f"{transfer_count} transfers, score: {ranking_score:.2f}"
                )

            logger.info(f"  Found {len(top_tokens)} top transferred tokens")
            return top_tokens

        except Exception as e:
            logger.warning(f"    Error fetching top transferred tokens: {e}")
            print(
                f"   This likely means the transfers service is not running or not reachable"
            )
            print(
                f"   Make sure the transfers-service is running: cd ~/transfers-service && docker-compose up -d"
            )
            import traceback

            traceback.print_exc()
            return set()

    async def _save_unmapped_tokens(self):
        """Save unmapped tokens to file for manual mapping."""
        output_dir = Path("data/unmapped_tokens")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save Hyperliquid unmapped
        if self.unmapped_hyperliquid:
            hyperliquid_path = output_dir / "hyperliquid_unmapped.json"
            with open(hyperliquid_path, "w") as f:
                json.dump(
                    {
                        "count": len(self.unmapped_hyperliquid),
                        "unmapped_symbols": sorted(
                            list(self.unmapped_hyperliquid.keys())
                        ),
                        "details": self.unmapped_hyperliquid,
                    },
                    f,
                    indent=2,
                )
            logger.info(
                f"💾 Saved {len(self.unmapped_hyperliquid)} unmapped Hyperliquid tokens to {hyperliquid_path}"
            )

        # Save Lighter unmapped
        if self.unmapped_lighter:
            lighter_path = output_dir / "lighter_unmapped.json"
            with open(lighter_path, "w") as f:
                json.dump(
                    {
                        "count": len(self.unmapped_lighter),
                        "unmapped_symbols": sorted(list(self.unmapped_lighter.keys())),
                        "details": self.unmapped_lighter,
                    },
                    f,
                    indent=2,
                )
            logger.info(
                f"💾 Saved {len(self.unmapped_lighter)} unmapped Lighter tokens to {lighter_path}"
            )

    async def get_all_pairs_and_tokens(
        self, chain: str = "ethereum", whitelist_tokens: Set[str] = None
    ) -> Dict:
        """
        DEPRECATED: Use PoolFilter class instead for better performance.

        This method queries ALL pools from database then filters in Python,
        which is extremely slow. The PoolFilter class uses optimized SQL queries.

        Query database for pools where BOTH tokens are in the whitelist.

        Args:
            chain: Blockchain to query
            whitelist_tokens: Set of whitelisted token addresses (lowercase)

        Returns dictionary with:
        - all_tokens: {tokenAddr: {address, symbol, decimals}} - only whitelisted tokens in pairs
        - all_pairs: {poolAddr: {token0: {...}, token1: {...}, exchange, fee, tickSpacing, stable}}
        - decimals: {tokenAddr: decimals}
        - symbols: {tokenAddr: symbol}
        - v2_pairs: filtered to V2 only
        - v3_pairs: filtered to V3 only
        - v4_pairs: filtered to V4 only
        """
        logger.warning(
            "get_all_pairs_and_tokens() is DEPRECATED. Use PoolFilter class for better performance."
        )
        logger.info("🔍 Fetching pairs where both tokens are whitelisted...")

        if not whitelist_tokens:
            logger.warning("No whitelist tokens provided, returning empty dictionaries")
            return {
                "all_tokens": {},
                "all_pairs": {},
                "decimals": {},
                "symbols": {},
                "v2_pairs": {},
                "v3_pairs": {},
                "v4_pairs": {},
            }

        # Get protocols for this chain
        protocols = ["uniswap_v2", "uniswap_v3", "uniswap_v4"]

        all_pairs = {}
        all_tokens = {}

        # Get chain_id for this chain
        chain_id = self.config.chains.get_chain_id(chain)
        if not chain_id:
            logger.error(f"Chain ID not found for chain: {chain}")
            return {
                "all_tokens": {},
                "all_pairs": {},
                "decimals": {},
                "symbols": {},
                "v2_pairs": {},
                "v3_pairs": {},
                "v4_pairs": {},
            }

        # Both table names for this chain
        dex_pools_table = f"network_{chain_id}__dex_pools"
        dex_pools_cryo_table = f"network_{chain_id}_dex_pools_cryo"

        for protocol in protocols:
            try:
                factories = self.config.protocols.get_factory_addresses(protocol, chain)

                for factory in factories:
                    factory_lower = factory.lower()

                    # Query both regular and cryo tables
                    for table in [dex_pools_table, dex_pools_cryo_table]:
                        query = f"""
                        SELECT
                            np.address as pool_address,
                            np.asset0 as token0_address,
                            np.asset1 as token1_address,
                            np.fee,
                            np.tick_spacing,
                            np.additional_data,
                            COALESCE(t0.symbol, cg0.token_id, SUBSTRING(np.asset0, 1, 8)) as token0_symbol,
                            cg0.decimals as token0_decimals,
                            COALESCE(t1.symbol, cg1.token_id, SUBSTRING(np.asset1, 1, 8)) as token1_symbol,
                            cg1.decimals as token1_decimals
                        FROM
                            {table} np
                        LEFT JOIN
                            coingecko_token_platforms cg0
                            ON LOWER(np.asset0) = LOWER(cg0.address)
                            AND cg0.platform = $1
                        LEFT JOIN
                            coingecko_tokens t0
                            ON cg0.token_id = t0.id
                        LEFT JOIN
                            coingecko_token_platforms cg1
                            ON LOWER(np.asset1) = LOWER(cg1.address)
                            AND cg1.platform = $1
                        LEFT JOIN
                            coingecko_tokens t1
                            ON cg1.token_id = t1.id
                        WHERE
                            LOWER(np.factory) = $2
                        """

                        try:
                            async with self.storage.pool.acquire() as conn:
                                results = await conn.fetch(query, chain, factory_lower)
                                logger.debug(
                                    f"Fetched {len(results)} pools from {table} for {protocol}/{factory[:10]}..."
                                )

                            # Process results from this table
                            for row in results:
                                pool_addr = row["pool_address"].lower()
                                token0_addr = row["token0_address"].lower()
                                token1_addr = row["token1_address"].lower()

                                # Only include pairs where BOTH tokens are in whitelist
                                if (
                                    token0_addr not in whitelist_tokens
                                    or token1_addr not in whitelist_tokens
                                ):
                                    continue

                                # Skip if we already have this pool
                                if pool_addr in all_pairs:
                                    continue

                                # Build token0 data (symbol should already have fallback from SQL)
                                token0 = {
                                    "address": token0_addr,
                                    "symbol": row["token0_symbol"],
                                    "decimals": int(row["token0_decimals"])
                                    if row["token0_decimals"]
                                    else 18,
                                }

                                # Build token1 data (symbol should already have fallback from SQL)
                                token1 = {
                                    "address": token1_addr,
                                    "symbol": row["token1_symbol"],
                                    "decimals": int(row["token1_decimals"])
                                    if row["token1_decimals"]
                                    else 18,
                                }

                                # Ensure token0 < token1 (canonical ordering)
                                if token0_addr > token1_addr:
                                    token0, token1 = token1, token0
                                    token0_addr, token1_addr = token1_addr, token0_addr

                                # Determine exchange name
                                protocol_lower = protocol.lower().replace("_", "")
                                if "uniswap" in protocol_lower:
                                    if "v2" in protocol_lower:
                                        exchange = "uniswapV2"
                                    elif "v3" in protocol_lower:
                                        exchange = "uniswapV3"
                                    elif "v4" in protocol_lower:
                                        exchange = "uniswapV4"
                                    else:
                                        exchange = protocol
                                else:
                                    exchange = protocol

                                # Parse additional_data and extract stable flag
                                stable = None
                                if row["additional_data"]:
                                    additional_data = row["additional_data"]
                                    # Handle both dict and JSON string
                                    if isinstance(additional_data, str):
                                        try:
                                            import json

                                            additional_data = json.loads(
                                                additional_data
                                            )
                                        except (json.JSONDecodeError, TypeError):
                                            additional_data = {}
                                    if isinstance(additional_data, dict):
                                        stable = additional_data.get("stable")

                                # Build pair data
                                pair_data = {
                                    "token0": token0,
                                    "token1": token1,
                                    "exchange": exchange,
                                    "fee": row["fee"],
                                    "tickSpacing": row["tick_spacing"],
                                    "stable": stable,
                                }

                                # Add to dictionaries
                                all_pairs[pool_addr] = pair_data
                                all_tokens[token0_addr] = token0
                                all_tokens[token1_addr] = token1

                        except Exception as e:
                            # Table might not exist (e.g., cryo table not yet created)
                            logger.debug(f"Could not query {table}: {e}")
                            continue

            except Exception as e:
                logger.warning(f"Error fetching {protocol} pairs: {e}")
                continue

        logger.info(f"  Found {len(all_pairs)} total pairs")
        logger.info(f"  Found {len(all_tokens)} unique tokens")

        # Build helper dictionaries
        decimals = {addr: token["decimals"] for addr, token in all_tokens.items()}
        symbols = {addr: token["symbol"] for addr, token in all_tokens.items()}

        # Filter by protocol
        v2_pairs = {
            addr: pair for addr, pair in all_pairs.items() if "V2" in pair["exchange"]
        }
        v3_pairs = {
            addr: pair for addr, pair in all_pairs.items() if "V3" in pair["exchange"]
        }
        v4_pairs = {
            addr: pair for addr, pair in all_pairs.items() if "V4" in pair["exchange"]
        }

        logger.info(f"  V2 pairs: {len(v2_pairs)}")
        logger.info(f"  V3 pairs: {len(v3_pairs)}")
        logger.info(f"  V4 pairs: {len(v4_pairs)}")

        return {
            "all_tokens": all_tokens,
            "all_pairs": all_pairs,
            "decimals": decimals,
            "symbols": symbols,
            "v2_pairs": v2_pairs,
            "v3_pairs": v3_pairs,
            "v4_pairs": v4_pairs,
        }

    async def build_whitelist(self, top_transfers: int = 100) -> Dict[str, any]:
        """
        Build token whitelist from multiple sources.

        This method ONLY builds the whitelist. Use PoolFilter for pool filtering and liquidity checks.

        Args:
            top_transfers: Number of top transferred tokens to include

        Returns:
            Dictionary with:
            - total_tokens: Count of whitelisted tokens
            - tokens: List of whitelisted token addresses (sorted)
            - token_sources: Dict mapping each token to its sources
            - token_info: Dict with metadata for each token
            - breakdown: Count by source (cross_chain, hyperliquid, lighter, top_transferred)
            - unmapped_hyperliquid: Symbols that couldn't be mapped to Ethereum addresses
            - unmapped_lighter: Symbols that couldn't be mapped to Ethereum addresses
        """
        logger.info("=" * 70)
        print("🏗️  Building Token Whitelist")
        logger.info("=" * 70)

        # Criterion 1: Cross-chain tokens
        cross_chain = await self.get_cross_chain_tokens()
        for token in cross_chain:
            if token not in self.token_sources:
                self.token_sources[token] = []
            self.token_sources[token].append("cross_chain")

        # Criterion 2: Hyperliquid tokens
        hyperliquid_addresses = await self.get_hyperliquid_tokens()
        for token in hyperliquid_addresses:
            if token not in self.token_sources:
                self.token_sources[token] = []
            self.token_sources[token].append("hyperliquid")

        # Criterion 3: Lighter tokens
        lighter_addresses = await self.get_lighter_tokens()
        for token in lighter_addresses:
            if token not in self.token_sources:
                self.token_sources[token] = []
            self.token_sources[token].append("lighter")

        # Criterion 4: Top transferred tokens
        top_transferred = await self.get_top_transferred_tokens(top_transfers)
        for token in top_transferred:
            if token not in self.token_sources:
                self.token_sources[token] = []
            self.token_sources[token].append("top_transferred")

        # Combine all tokens
        self.whitelist_tokens = set(self.token_sources.keys())

        # Print summary
        print("\n" + "=" * 70)
        print("📈 Whitelist Summary")
        logger.info("=" * 70)
        logger.info(f"Total whitelisted tokens: {len(self.whitelist_tokens)}")
        print(f"\nBy source:")
        logger.info(f"  Cross-chain: {len(cross_chain)}")
        logger.info(f"  Hyperliquid: {len(hyperliquid_addresses)}")
        logger.info(f"  Lighter: {len(lighter_addresses)}")
        logger.info(f"  Top transferred: {len(top_transferred)}")

        # Show tokens matched by multiple sources
        multi_source = {
            token: sources
            for token, sources in self.token_sources.items()
            if len(sources) > 1
        }

        if multi_source:
            print(f"\n✨ Tokens from multiple sources ({len(multi_source)}):")
            for token, sources in sorted(
                multi_source.items(), key=lambda x: -len(x[1])
            )[:20]:
                info = self.token_info.get(token, {})
                symbol = info.get("symbol", "?")
                logger.info(f"  {symbol:8s} {token[:10]}... - {', '.join(sources)}")

        # Save unmapped tokens for manual review
        await self._save_unmapped_tokens()

        # Return whitelist only - pool filtering is handled by PoolFilter class
        # NOTE: The rpc_url and min_liquidity parameters are deprecated - use PoolFilter instead
        return {
            "total_tokens": len(self.whitelist_tokens),
            "tokens": sorted(list(self.whitelist_tokens)),
            "token_sources": self.token_sources,
            "token_info": self.token_info,
            "breakdown": {
                "cross_chain": len(cross_chain),
                "hyperliquid": len(hyperliquid_addresses),
                "lighter": len(lighter_addresses),
                "top_transferred": len(top_transferred),
            },
            "unmapped_hyperliquid": self.unmapped_hyperliquid,
            "unmapped_lighter": self.unmapped_lighter,
        }

    def save_whitelist(self, output_path: str):
        """Save whitelist to JSON file."""
        data = {
            "total_tokens": len(self.whitelist_tokens),
            "tokens": sorted(list(self.whitelist_tokens)),
            "token_sources": self.token_sources,
            "token_info": self.token_info,
        }

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\n💾 Saved whitelist to {output_path}")


async def main():
    """Main execution function."""
    # Initialize config and storage
    config = ConfigManager()

    db_config = {
        "host": config.database.POSTGRES_HOST,
        "port": config.database.POSTGRES_PORT,
        "user": config.database.POSTGRES_USER,
        "password": config.database.POSTGRES_PASSWORD,
        "database": config.database.POSTGRES_DB,
        "pool_size": 10,
        "pool_timeout": 10,
    }

    storage = PostgresStorage(config=db_config)
    await storage.connect()

    try:
        builder = TokenWhitelistBuilder(storage)

        # Build whitelist
        result = await builder.build_whitelist(top_transfers=100)

        # Save to file
        output_path = Path(project_root) / "data" / "token_whitelist.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        builder.save_whitelist(str(output_path))

        print("\n✅ Whitelist building complete!")

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
