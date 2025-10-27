"""
Pool Liquidity Filter.

Filters pools based on minimum dollar notional value of reserves/liquidity.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, Set, Tuple, Optional

from web3 import Web3

from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.batchers.uniswap_v2_reserves import UniswapV2ReservesBatcher
from src.batchers.uniswap_v3_data import UniswapV3DataBatcher
from src.batchers.uniswap_v4_data import UniswapV4DataBatcher
from src.fetchers.exchange_fetchers import HyperliquidFetcher

logger = logging.getLogger(__name__)

# Import V3 math functions
from .v3_math import get_amount0_delta, get_amount1_delta, get_sqrt_ratio_at_tick

# V4 uses zero address for native ETH - treat as WETH for pricing/decimals
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # Ethereum mainnet WETH


def normalize_token_for_pricing(address: str) -> str:
    """
    Normalize token address for price/decimal lookup.

    V4 uses zero address for native ETH. For pricing and decimals,
    treat it as WETH since they're functionally equivalent.

    Args:
        address: Token address (may be zero address for native ETH)

    Returns:
        Normalized address for price/decimal lookup
    """
    addr_lower = address.lower()
    if addr_lower == ZERO_ADDRESS.lower():
        return WETH_ADDRESS.lower()
    return addr_lower


class PoolLiquidityFilter:
    """Filter pools by minimum liquidity threshold using real-time prices."""

    def __init__(
        self,
        web3: Web3,
        min_liquidity_v2_usd: float = 2000,
        min_liquidity_v3_usd: float = 2000,
        min_liquidity_v4_usd: float = 2000,
        chain: str = "ethereum"
    ):
        """
        Initialize liquidity filter.

        Args:
            web3: Web3 instance for blockchain calls
            min_liquidity_v2_usd: Minimum liquidity for V2 pools in USD
            min_liquidity_v3_usd: Minimum liquidity for V3 pools in USD
            min_liquidity_v4_usd: Minimum liquidity for V4 pools in USD
            chain: Blockchain name
        """
        self.web3 = web3
        self.min_liquidity_v2_usd = Decimal(str(min_liquidity_v2_usd))
        self.min_liquidity_v3_usd = Decimal(str(min_liquidity_v3_usd))
        self.min_liquidity_v4_usd = Decimal(str(min_liquidity_v4_usd))
        self.chain = chain
        self.prices: Dict[str, Decimal] = {}  # token_address -> price in USD

    async def fetch_missing_decimals(
        self,
        token_addresses: set,
        existing_decimals: Dict[str, int]
    ) -> Dict[str, int]:
        """
        Fetch decimals for tokens missing from existing_decimals dict.

        Args:
            token_addresses: Set of token addresses to check
            existing_decimals: Dict of already-known token decimals

        Returns:
            Combined dict with existing + newly fetched decimals
        """
        from src.batchers.erc20_metadata import ERC20MetadataBatcher
        from src.batchers.base import BatchConfig

        # Find tokens missing decimals
        missing = [addr for addr in token_addresses if addr not in existing_decimals]

        if not missing:
            return existing_decimals

        logger.info(f"🔍 Fetching decimals on-chain for {len(missing)} tokens...")

        # Fetch on-chain
        batch_config = BatchConfig(batch_size=50)
        erc20_batcher = ERC20MetadataBatcher(self.web3, config=batch_config)
        metadata = await erc20_batcher.fetch_metadata_chunked(missing)

        # Combine
        combined = dict(existing_decimals)
        newly_fetched = 0

        for addr, meta in metadata.items():
            if meta and 'decimals' in meta:
                combined[addr.lower()] = meta['decimals']
                newly_fetched += 1

        logger.info(f"   ✅ Fetched {newly_fetched} decimals on-chain")

        return combined

    async def fetch_hyperliquid_prices(self) -> Dict[str, Decimal]:
        """
        Fetch current prices from Hyperliquid perp markets (use as proxy for spot).

        Returns:
            Dictionary mapping token symbols to prices in USD
        """
        logger.info("📊 Fetching Hyperliquid perp prices...")

        try:
            fetcher = HyperliquidFetcher()
            result = await fetcher.fetch_markets(market_type="swap")

            if not result.success:
                logger.error(f"Failed to fetch Hyperliquid prices: {result.error}")
                return {}

            # Extract prices from perp markets using oracle price
            perp_prices = {}
            tokens_data = result.metadata.get("tokens", [])

            for token in tokens_data:
                base_symbol = token.base.upper()
                # Extract oracle price from additional_data
                try:
                    oracle_px = token.additional_data.get('info', {}).get('oraclePx')
                    if oracle_px:
                        price = Decimal(str(oracle_px))
                        perp_prices[base_symbol] = price
                        logger.debug(f"  {base_symbol}: ${price}")
                except (ValueError, TypeError, AttributeError) as e:
                    continue

            logger.info(f"  Fetched {len(perp_prices)} perp prices from Hyperliquid")
            return perp_prices

        except Exception as e:
            logger.error(f"Error fetching Hyperliquid prices: {e}")
            import traceback
            traceback.print_exc()
            return {}

    async def fetch_binance_prices(self) -> Dict[str, Decimal]:
        """
        Fetch current prices from Binance spot markets.

        Returns:
            Dictionary mapping token symbols to prices in USD
        """
        logger.info("💹 Fetching Binance spot prices...")

        try:
            from src.fetchers.exchange_fetchers import BinanceFetcher
            
            fetcher = BinanceFetcher()
            result = await fetcher.fetch_markets(market_type="spot")

            if not result.success:
                logger.error(f"Failed to fetch Binance prices: {result.error}")
                return {}

            # Extract prices from spot markets
            spot_prices = {}
            tokens_data = result.metadata.get("tokens", [])

            # For Binance, we need to fetch current ticker prices
            # The market data just tells us which markets exist
            loop = asyncio.get_event_loop()
            tickers = await loop.run_in_executor(
                None,
                fetcher.ccxt_exchange.fetchTickers
            )

            for token in tokens_data:
                base_symbol = token.base.upper()
                quote_symbol = token.quote.upper()
                symbol = token.symbol  # e.g., "BTC/USDT"

                # Get current ticker price
                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                # Get last traded price
                last_price = ticker.get('last')
                if last_price is None:
                    continue

                try:
                    price = Decimal(str(last_price))
                    
                    # Convert to USD if quote is not USD-based
                    if quote_symbol in ['USDT', 'USDC', 'BUSD', 'USD']:
                        # Already in USD terms
                        spot_prices[base_symbol] = price
                        logger.debug(f"  {base_symbol}: ${price}")
                    elif quote_symbol == 'BTC' and 'BTC' in spot_prices:
                        # Convert BTC-quoted price to USD
                        btc_price = spot_prices['BTC']
                        usd_price = price * btc_price
                        spot_prices[base_symbol] = usd_price
                        logger.debug(f"  {base_symbol}: ${usd_price} (via BTC)")
                    elif quote_symbol == 'ETH' and 'ETH' in spot_prices:
                        # Convert ETH-quoted price to USD
                        eth_price = spot_prices['ETH']
                        usd_price = price * eth_price
                        spot_prices[base_symbol] = usd_price
                        logger.debug(f"  {base_symbol}: ${usd_price} (via ETH)")
                    # else: skip non-USD pairs we can't convert

                except (ValueError, TypeError) as e:
                    logger.debug(f"  Invalid price for {symbol}: {e}")
                    continue

            logger.info(f"  Fetched {len(spot_prices)} spot prices from Binance")
            return spot_prices

        except Exception as e:
            logger.error(f"Error fetching Binance prices: {e}")
            import traceback
            traceback.print_exc()
            return {}

    async def discover_prices_from_v2_pools(
        self,
        storage,
        all_tokens: set,
        initial_prices: Dict[str, Decimal],
        token_decimals: Dict[str, int],
        v2_factories: list,
        max_iterations: int = 10
    ) -> Dict[str, Decimal]:
        """
        Iteratively discover token prices through V2 pools.

        Algorithm:
        1. Start with initial_prices (from exchanges)
        2. Find V2 pools with (unpricedToken, pricedToken)
        3. Fetch reserves and calculate price for unpricedToken
        4. Add to price dictionary
        5. Repeat until no new prices or max_iterations reached

        Args:
            storage: PostgresStorage instance for database queries
            all_tokens: Set of all token addresses we care about
            initial_prices: Dict of token_address -> price (from exchanges)
            token_decimals: Dict of token_address -> decimals (will fetch missing on-chain)
            v2_factories: List of V2 factory addresses
            max_iterations: Maximum number of discovery rounds

        Returns:
            Dict of token_address -> price with all discovered prices
        """
        from src.batchers.uniswap_v2_reserves import UniswapV2ReservesBatcher

        logger.info("🔍 V2 Price Discovery Starting...")
        logger.info(f"   Starting with {len(initial_prices)} prices")

        # Initialize
        prices = dict(initial_prices)
        tokens_with_prices = set(prices.keys())
        tokens_without_prices = all_tokens - tokens_with_prices

        v2_batcher = UniswapV2ReservesBatcher(self.web3)

        for iteration in range(max_iterations):
            if not tokens_without_prices:
                logger.info(f"   ✅ All tokens priced after {iteration} iterations!")
                break

            # Query V2 pools where one token has price, other doesn't
            query = """
            SELECT DISTINCT
                address as pool_address,
                asset0 as token0,
                asset1 as token1
            FROM (
                SELECT address, asset0, asset1 FROM network_1__dex_pools
                WHERE LOWER(factory) = ANY($1)
                UNION
                SELECT address, asset0, asset1 FROM network_1_dex_pools_cryo
                WHERE LOWER(factory) = ANY($1)
            ) pools
            WHERE (
                (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($3))
                OR
                (LOWER(asset0) = ANY($3) AND LOWER(asset1) = ANY($2))
            )
            """

            async with storage.pool.acquire() as conn:
                results = await conn.fetch(
                    query,
                    [f.lower() for f in v2_factories],
                    [addr.lower() for addr in tokens_without_prices],
                    [addr.lower() for addr in tokens_with_prices]
                )

            if not results:
                break

            # Fetch reserves
            pool_addresses = [row['pool_address'].lower() for row in results]
            reserves = await v2_batcher.fetch_reserves_chunked(pool_addresses)

            # Calculate prices
            new_prices_found = 0
            for row in results:
                pool_addr = row['pool_address'].lower()
                token0 = row['token0'].lower()
                token1 = row['token1'].lower()

                reserve_data = reserves.get(pool_addr)
                if not reserve_data:
                    continue

                try:
                    reserve0 = Decimal(str(int(reserve_data['reserve0'], 16)))
                    reserve1 = Decimal(str(int(reserve_data['reserve1'], 16)))

                    if reserve0 == 0 or reserve1 == 0:
                        continue

                    decimals0 = token_decimals.get(token0)
                    decimals1 = token_decimals.get(token1)

                    if decimals0 is None or decimals1 is None:
                        continue

                    # Calculate price using the token that has a known price
                    if token0 in tokens_with_prices and token1 in tokens_without_prices:
                        price0 = prices[token0]
                        # Adjust for decimals and calculate
                        adj_reserve0 = reserve0 / Decimal(10 ** decimals0)
                        adj_reserve1 = reserve1 / Decimal(10 ** decimals1)
                        price1 = (adj_reserve0 / adj_reserve1) * price0

                        prices[token1] = price1
                        tokens_with_prices.add(token1)
                        tokens_without_prices.discard(token1)
                        new_prices_found += 1

                    elif token1 in tokens_with_prices and token0 in tokens_without_prices:
                        price1 = prices[token1]
                        adj_reserve0 = reserve0 / Decimal(10 ** decimals0)
                        adj_reserve1 = reserve1 / Decimal(10 ** decimals1)
                        price0 = (adj_reserve1 / adj_reserve0) * price1

                        prices[token0] = price0
                        tokens_with_prices.add(token0)
                        tokens_without_prices.discard(token0)
                        new_prices_found += 1

                except (ValueError, KeyError, ZeroDivisionError):
                    continue

            if new_prices_found == 0:
                break

        logger.info(f"   ✅ V2 Discovery: {len(prices) - len(initial_prices)} new prices")
        return prices

    async def discover_prices_from_v3_pools(
        self,
        storage,
        all_tokens: set,
        initial_prices: Dict[str, Decimal],
        token_decimals: Dict[str, int],
        v3_factories: list,
        max_iterations: int = 10
    ) -> Dict[str, Decimal]:
        """
        Iteratively discover token prices through V3 pools using sqrtPriceX96.

        Args:
            storage: PostgresStorage instance for database queries
            all_tokens: Set of all token addresses we care about
            initial_prices: Dict of token_address -> price (from exchanges + V2)
            token_decimals: Dict of token_address -> decimals (will fetch missing on-chain)
            v3_factories: List of V3 factory addresses
            max_iterations: Maximum number of discovery rounds

        Returns:
            Dict of token_address -> price with all discovered prices
        """
        from src.batchers.uniswap_v3_data import UniswapV3DataBatcher
        from src.batchers.base import BatchConfig

        logger.info("🔍 V3 Price Discovery Starting...")
        logger.info(f"   Starting with {len(initial_prices)} prices")

        # Initialize
        prices = dict(initial_prices)
        tokens_with_prices = set(prices.keys())
        tokens_without_prices = all_tokens - tokens_with_prices

        batch_config = BatchConfig(batch_size=50)
        v3_batcher = UniswapV3DataBatcher(self.web3, config=batch_config)

        for iteration in range(max_iterations):
            if not tokens_without_prices:
                logger.info(f"   ✅ All tokens priced after {iteration} iterations!")
                break

            # Query V3 pools where one token has price, other doesn't
            # Query BOTH tables to get all V3 pools
            query = """
            SELECT DISTINCT
                address as pool_address,
                asset0 as token0,
                asset1 as token1
            FROM (
                SELECT address, asset0, asset1, factory FROM network_1__dex_pools
                UNION
                SELECT address, asset0, asset1, factory FROM network_1_dex_pools_cryo
            ) pools
            WHERE LOWER(factory) = ANY($1)
            AND (
                (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($3))
                OR
                (LOWER(asset0) = ANY($3) AND LOWER(asset1) = ANY($2))
            )
            """

            async with storage.pool.acquire() as conn:
                results = await conn.fetch(
                    query,
                    [f.lower() for f in v3_factories],
                    [addr.lower() for addr in tokens_without_prices],
                    [addr.lower() for addr in tokens_with_prices]
                )

            if not results:
                break

            # Fetch pool states
            pool_addresses = [row['pool_address'].lower() for row in results]
            pool_states = await v3_batcher.fetch_pools_chunked(pool_addresses)

            # Calculate prices
            new_prices_found = 0
            for row in results:
                pool_addr = row['pool_address'].lower()
                token0 = row['token0'].lower()
                token1 = row['token1'].lower()

                state = pool_states.get(pool_addr)
                if not state:
                    continue

                try:
                    sqrt_price_x96 = int(state['sqrtPriceX96'])
                    if sqrt_price_x96 == 0:
                        continue

                    decimals0 = token_decimals.get(token0)
                    decimals1 = token_decimals.get(token1)

                    if decimals0 is None or decimals1 is None:
                        continue

                    # Calculate price ratio from sqrtPriceX96
                    # price = (sqrtPriceX96 / 2^96)^2 * (10^decimals0 / 10^decimals1)
                    price_ratio = Decimal(sqrt_price_x96 ** 2) / Decimal(2 ** 192)
                    decimals_adjustment = Decimal(10 ** (decimals0 - decimals1))
                    price_token0_in_token1 = price_ratio * decimals_adjustment

                    # Determine which token has price and calculate the other
                    if token0 in tokens_with_prices and token1 in tokens_without_prices:
                        price0 = prices[token0]
                        if price_token0_in_token1 > 0:
                            price1 = price0 / price_token0_in_token1
                            prices[token1] = price1
                            tokens_with_prices.add(token1)
                            tokens_without_prices.discard(token1)
                            new_prices_found += 1

                    elif token1 in tokens_with_prices and token0 in tokens_without_prices:
                        price1 = prices[token1]
                        price0 = price1 * price_token0_in_token1
                        prices[token0] = price0
                        tokens_with_prices.add(token0)
                        tokens_without_prices.discard(token0)
                        new_prices_found += 1

                except (ValueError, KeyError, ZeroDivisionError, TypeError):
                    continue

            if new_prices_found == 0:
                break

        logger.info(f"   ✅ V3 Discovery: {len(prices) - len(initial_prices)} new prices")
        return prices

    async def discover_prices_from_v4_pools(
        self,
        storage,
        all_tokens: set,
        initial_prices: Dict[str, Decimal],
        token_decimals: Dict[str, int],
        v4_factories: list,
        max_iterations: int = 10
    ) -> Dict[str, Decimal]:
        """
        Iteratively discover token prices through V4 pools using sqrtPriceX96.

        V4 uses the same pricing formula as V3, but stores pools by pool_id (hash)
        and uses zero address for native ETH.

        Args:
            storage: PostgresStorage instance for database queries
            all_tokens: Set of all token addresses we care about
            initial_prices: Dict of token_address -> price (from exchanges + V2 + V3)
            token_decimals: Dict of token_address -> decimals
            v4_factories: List of V4 factory addresses
            max_iterations: Maximum number of discovery rounds

        Returns:
            Dict of token_address -> price with all discovered prices
        """
        from src.batchers.uniswap_v4_data import UniswapV4DataBatcher
        from src.batchers.base import BatchConfig

        logger.info("🔍 V4 Price Discovery Starting...")
        logger.info(f"   Starting with {len(initial_prices)} prices")

        # Initialize
        prices = dict(initial_prices)
        tokens_with_prices = set(prices.keys())
        tokens_without_prices = all_tokens - tokens_with_prices

        batch_config = BatchConfig(batch_size=50)
        v4_batcher = UniswapV4DataBatcher(self.web3, config=batch_config)

        for iteration in range(max_iterations):
            if not tokens_without_prices:
                logger.info(f"   ✅ All tokens priced after {iteration} iterations!")
                break

            # Query V4 pools where one token has price, other doesn't
            # Note: V4 stores pool_id in the address column
            # Query BOTH tables to get all V4 pools
            query = """
            SELECT DISTINCT
                address as pool_id,
                asset0 as token0,
                asset1 as token1
            FROM (
                SELECT address, asset0, asset1, factory FROM network_1__dex_pools
                UNION
                SELECT address, asset0, asset1, factory FROM network_1_dex_pools_cryo
            ) pools
            WHERE LOWER(factory) = ANY($1)
            AND (
                (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($3))
                OR
                (LOWER(asset0) = ANY($3) AND LOWER(asset1) = ANY($2))
            )
            """

            async with storage.pool.acquire() as conn:
                results = await conn.fetch(
                    query,
                    [f.lower() for f in v4_factories],
                    [addr.lower() for addr in tokens_without_prices],
                    [addr.lower() for addr in tokens_with_prices]
                )

            if not results:
                break

            # Fetch pool states
            pool_ids = [row['pool_id'].lower() for row in results]
            pool_states = await v4_batcher.fetch_pools_chunked(pool_ids)

            # Calculate prices
            new_prices_found = 0
            for row in results:
                pool_id = row['pool_id'].lower()
                token0 = row['token0'].lower()
                token1 = row['token1'].lower()

                # Normalize zero address to WETH for pricing
                token0_lookup = normalize_token_for_pricing(token0)
                token1_lookup = normalize_token_for_pricing(token1)

                state = pool_states.get(pool_id)
                if not state:
                    continue

                try:
                    sqrt_price_x96 = int(state['sqrtPriceX96'])
                    if sqrt_price_x96 == 0:
                        continue

                    # Get decimals (using normalized addresses for zero address)
                    decimals0 = token_decimals.get(token0_lookup)
                    decimals1 = token_decimals.get(token1_lookup)

                    if decimals0 is None or decimals1 is None:
                        continue

                    # Calculate price ratio from sqrtPriceX96
                    price_ratio = Decimal(sqrt_price_x96 ** 2) / Decimal(2 ** 192)
                    decimals_adjustment = Decimal(10 ** (decimals0 - decimals1))
                    price_token0_in_token1 = price_ratio * decimals_adjustment

                    # Determine which token has price and calculate the other
                    # Use normalized addresses for price lookup
                    if token0_lookup in tokens_with_prices and token1_lookup in tokens_without_prices:
                        price0 = prices[token0_lookup]
                        if price_token0_in_token1 > 0:
                            price1 = price0 / price_token0_in_token1
                            prices[token1_lookup] = price1
                            tokens_with_prices.add(token1_lookup)
                            tokens_without_prices.discard(token1_lookup)
                            new_prices_found += 1

                    elif token1_lookup in tokens_with_prices and token0_lookup in tokens_without_prices:
                        price1 = prices[token1_lookup]
                        price0 = price1 * price_token0_in_token1
                        prices[token0_lookup] = price0
                        tokens_with_prices.add(token0_lookup)
                        tokens_without_prices.discard(token0_lookup)
                        new_prices_found += 1

                except (ValueError, KeyError, ZeroDivisionError, TypeError):
                    continue

            if new_prices_found == 0:
                break

        logger.info(f"   ✅ V4 Discovery: {len(prices) - len(initial_prices)} new prices")
        return prices

    def _map_token_to_price_symbol(
        self,
        token_address: str,
        token_symbols: Dict[str, str]
    ) -> str:
        """
        Map token address to price symbol for lookup.

        Args:
            token_address: Token address (lowercase)
            token_symbols: Dictionary of token address -> symbol

        Returns:
            Symbol for price lookup (uppercase)
        """
        symbol = token_symbols.get(token_address, "").upper()

        # Handle wrapped tokens
        if symbol == "WETH":
            return "ETH"
        elif symbol == "WBTC":
            return "BTC"

        return symbol

    def _calculate_token_price_from_pair(
        self,
        reserve_token: int,
        reserve_other: int,
        decimals_token: int,
        decimals_other: int,
        price_other: Decimal
    ) -> Decimal:
        """
        Calculate token price from pool reserves when we have the other token's price.

        Formula: price_token = (reserves_other / reserves_token) *
                              (10^(decimals_token - decimals_other)) * price_other

        Args:
            reserve_token: Reserves of token to price
            reserve_other: Reserves of other token
            decimals_token: Decimals of token to price
            decimals_other: Decimals of other token
            price_other: Known price of other token in USD

        Returns:
            Calculated price of token in USD
        """
        if reserve_token == 0:
            return Decimal(0)

        # Calculate price ratio
        price_ratio = Decimal(reserve_other) / Decimal(reserve_token)

        # Adjust for decimals
        decimals_adjustment = Decimal(10) ** (decimals_token - decimals_other)

        # Calculate final price
        price_token = price_ratio * decimals_adjustment * price_other

        return price_token

    async def filter_v2_pools(
        self,
        pools: Dict[str, Dict],
        token_symbols: Dict[str, str],
        token_decimals: Dict[str, int],
        hyperliquid_symbol_prices: Dict[str, Decimal]
    ) -> Tuple[Dict[str, Dict], Dict[str, Decimal]]:
        """
        Filter Uniswap V2 pools by minimum liquidity.

        Args:
            pools: Dictionary of pool_address -> pool_data
            token_symbols: Dictionary of token_address -> symbol
            token_decimals: Dictionary of token_address -> decimals
            hyperliquid_symbol_prices: Prices from Hyperliquid (symbol -> price)

        Returns:
            Tuple of (filtered_pools, discovered_token_prices)
        """
        if not pools:
            logger.warning("No V2 pools to filter")
            return {}, {}

        logger.info(f"🔍 Filtering {len(pools)} V2 pools by liquidity...")

        # Initialize prices from Hyperliquid, mapping to token addresses
        token_prices = {}
        for token_addr, symbol in token_symbols.items():
            symbol_upper = self._map_token_to_price_symbol(token_addr, token_symbols)
            if symbol_upper in hyperliquid_symbol_prices:
                token_prices[token_addr] = hyperliquid_symbol_prices[symbol_upper]
                logger.debug(f"  Mapped {symbol} ({token_addr[:10]}...) -> ${token_prices[token_addr]}")

        initial_price_count = len(token_prices)
        logger.info(f"  📍 Starting V2 filtering with {initial_price_count} prices from Hyperliquid")

        # Batch fetch reserves for all pools
        pool_addresses = list(pools.keys())
        batcher = UniswapV2ReservesBatcher(self.web3)

        try:
            reserves_data = await batcher.fetch_reserves_chunked(pool_addresses)
            logger.info(f"  Fetched reserves for {len(reserves_data)} pools")
        except Exception as e:
            logger.error(f"Failed to fetch reserves: {e}")
            return {}, {}

        # Filter pools by liquidity
        filtered_pools = {}
        pools_below_threshold = 0

        for pool_addr, pool_data in pools.items():
            # Get reserves
            reserves = reserves_data.get(pool_addr.lower())
            if not reserves:
                logger.debug(f"  No reserves data for {pool_addr[:10]}...")
                continue

            token0_addr = pool_data['token0']['address']
            token1_addr = pool_data['token1']['address']

            # Parse reserves as integers
            try:
                reserve0 = int(reserves['reserve0'], 16)
                reserve1 = int(reserves['reserve1'], 16)
            except (ValueError, TypeError) as e:
                logger.debug(f"  Invalid reserves for {pool_addr[:10]}...: {e}")
                continue

            if reserve0 == 0 or reserve1 == 0:
                continue

            # Get decimals - skip if we don't have this info
            decimals0 = token_decimals.get(token0_addr)
            decimals1 = token_decimals.get(token1_addr)

            if decimals0 is None or decimals1 is None:
                logger.debug(f"  Missing decimals for V2 pool (token0={decimals0}, token1={decimals1})")
                continue

            # Get or calculate prices
            price0 = token_prices.get(token0_addr)
            price1 = token_prices.get(token1_addr)

            # If we have neither price, skip this pool
            if price0 is None and price1 is None:
                continue

            # If we only have one price, calculate the other from pool reserves
            if price0 is None and price1 is not None:
                price0 = self._calculate_token_price_from_pair(
                    reserve0, reserve1, decimals0, decimals1, price1
                )
                token_prices[token0_addr] = price0
                logger.debug(f"  Calculated price for {token_symbols.get(token0_addr, token0_addr[:8])}: ${price0}")

            elif price1 is None and price0 is not None:
                price1 = self._calculate_token_price_from_pair(
                    reserve1, reserve0, decimals1, decimals0, price0
                )
                token_prices[token1_addr] = price1
                logger.debug(f"  Calculated price for {token_symbols.get(token1_addr, token1_addr[:8])}: ${price1}")

            # Calculate liquidity in USD
            liquidity0_usd = (Decimal(reserve0) / Decimal(10 ** decimals0)) * price0
            liquidity1_usd = (Decimal(reserve1) / Decimal(10 ** decimals1)) * price1
            total_liquidity_usd = liquidity0_usd + liquidity1_usd

            # Filter by minimum liquidity (using V2-specific threshold)
            if total_liquidity_usd >= self.min_liquidity_v2_usd:
                filtered_pools[pool_addr] = pool_data
                logger.debug(f"  ✓ {pool_addr[:10]}... - ${total_liquidity_usd:,.2f} liquidity")
            else:
                pools_below_threshold += 1

        logger.info(f"  ✅ {len(filtered_pools)} V2 pools above ${self.min_liquidity_v2_usd:,.0f} threshold")
        logger.info(f"  ❌ {pools_below_threshold} V2 pools below threshold")
        logger.info(f"  💰 Discovered {len(token_prices) - initial_price_count} new token prices from V2 pools (total: {len(token_prices)})")

        return filtered_pools, token_prices

    async def filter_v3_pools(
        self,
        pools: Dict[str, Dict],
        token_symbols: Dict[str, str],
        token_decimals: Dict[str, int],
        token_prices: Dict[str, Decimal]
    ) -> Tuple[Dict[str, Dict], Dict[str, Decimal]]:
        """
        Filter Uniswap V3 pools by minimum liquidity.

        Uses degenbot's sqrt_price_math to calculate token amounts at current price.

        Args:
            pools: Dictionary of pool_address -> pool_data
            token_symbols: Dictionary of token_address -> symbol
            token_decimals: Dictionary of token_address -> decimals
            token_prices: Dictionary of token_address -> price (from V2 + Hyperliquid)

        Returns:
            Tuple of (filtered_pools, updated_token_prices)
        """
        if not pools:
            logger.warning("No V3 pools to filter")
            return {}, {}

        logger.info(f"🔍 Filtering {len(pools)} V3 pools by liquidity...")

        # Batch fetch pool state (sqrtPriceX96, liquidity, tick)
        pool_addresses = list(pools.keys())
        # Use smaller batch size to minimize impact of failed pools
        from src.batchers.base import BatchConfig
        batch_config = BatchConfig(batch_size=50)
        batcher = UniswapV3DataBatcher(self.web3, config=batch_config)

        try:
            pool_states = await batcher.fetch_pools_chunked(pool_addresses)
            logger.info(f"  Fetched state for {len(pool_states)} V3 pools")
        except Exception as e:
            logger.error(f"Failed to fetch V3 pool states: {e}")
            return {}, token_prices

        # Filter pools by liquidity
        filtered_pools = {}
        pools_below_threshold = 0
        pools_no_state = 0
        pools_no_liquidity = 0
        pools_no_decimals = 0
        pools_no_prices = 0
        pools_calc_error = 0
        updated_prices = dict(token_prices)

        logger.info(f"  📍 Starting with {len(updated_prices)} token prices from V2 + Hyperliquid")

        for pool_addr, pool_data in pools.items():
            # Get pool state
            state = pool_states.get(pool_addr.lower())
            if not state:
                pools_no_state += 1
                logger.debug(f"  No state data for {pool_addr[:10]}...")
                continue

            token0_addr = pool_data['token0']['address']
            token1_addr = pool_data['token1']['address']

            # Get pool parameters
            try:
                sqrt_price_x96 = int(state['sqrtPriceX96'])
                liquidity = int(state['liquidity'])
                tick = int(state['tick'])
            except (ValueError, TypeError, KeyError) as e:
                logger.debug(f"  Invalid pool state for {pool_addr[:10]}...: {e}")
                pools_calc_error += 1
                continue

            # Skip pools with no liquidity
            if liquidity == 0:
                pools_no_liquidity += 1
                continue

            # Get decimals - skip if we don't have this info
            decimals0 = token_decimals.get(token0_addr)
            decimals1 = token_decimals.get(token1_addr)

            if decimals0 is None or decimals1 is None:
                pools_no_decimals += 1
                logger.debug(f"  Missing decimals for pool (token0={decimals0}, token1={decimals1})")
                continue

            # Get prices
            price0 = updated_prices.get(token0_addr)
            price1 = updated_prices.get(token1_addr)

            # Skip if we have neither price
            if price0 is None and price1 is None:
                pools_no_prices += 1
                symbol0 = token_symbols.get(token0_addr, token0_addr[:8])
                symbol1 = token_symbols.get(token1_addr, token1_addr[:8])
                logger.debug(f"  No prices for {symbol0}/{symbol1} pool")
                continue

            # Calculate token amounts at current price
            # For V3, we calculate amounts in a range around current tick
            # Using tick +/- 100 (~1% price movement) as proxy for "active" liquidity
            try:
                tick_lower = tick - 100
                tick_upper = tick + 100

                sqrt_ratio_lower = get_sqrt_ratio_at_tick(tick_lower)
                sqrt_ratio_upper = get_sqrt_ratio_at_tick(tick_upper)

                # Calculate token amounts
                amount0 = get_amount0_delta(
                    sqrt_ratio_a_x96=sqrt_ratio_lower,
                    sqrt_ratio_b_x96=sqrt_ratio_upper,
                    liquidity=liquidity,
                    round_up=True
                )
                amount1 = get_amount1_delta(
                    sqrt_ratio_a_x96=sqrt_ratio_lower,
                    sqrt_ratio_b_x96=sqrt_ratio_upper,
                    liquidity=liquidity,
                    round_up=True
                )

            except Exception as e:
                logger.debug(f"  Failed to calculate amounts for {pool_addr[:10]}...: {e}")
                continue

            # Convert amounts to decimal format
            amount0_decimal = Decimal(amount0) / Decimal(10 ** decimals0)
            amount1_decimal = Decimal(amount1) / Decimal(10 ** decimals1)

            # Calculate or derive prices if one is missing
            if price0 is None and price1 is not None:
                # Calculate price0 from the ratio: price0 = (amount1 / amount0) * price1
                if amount0 > 0:
                    price0 = (amount1_decimal / amount0_decimal) * price1
                    updated_prices[token0_addr] = price0
                    logger.debug(f"  Calculated price for {token_symbols.get(token0_addr, token0_addr[:8])}: ${price0}")
                else:
                    continue

            elif price1 is None and price0 is not None:
                # Calculate price1 from the ratio: price1 = (amount0 / amount1) * price0
                if amount1 > 0:
                    price1 = (amount0_decimal / amount1_decimal) * price0
                    updated_prices[token1_addr] = price1
                    logger.debug(f"  Calculated price for {token_symbols.get(token1_addr, token1_addr[:8])}: ${price1}")
                else:
                    continue

            # Calculate liquidity in USD
            liquidity0_usd = amount0_decimal * price0
            liquidity1_usd = amount1_decimal * price1
            total_liquidity_usd = liquidity0_usd + liquidity1_usd

            # Filter by minimum liquidity
            if total_liquidity_usd >= self.min_liquidity_usd:
                filtered_pools[pool_addr] = pool_data
                logger.debug(f"  ✓ {pool_addr[:10]}... - ${total_liquidity_usd:,.2f} liquidity")
            else:
                pools_below_threshold += 1

        logger.info(f"  ✅ {len(filtered_pools)} V3 pools above ${self.min_liquidity_usd:,.0f} threshold")
        logger.info(f"  ❌ {pools_below_threshold} V3 pools below threshold")
        logger.info(f"  📊 V3 Filtering breakdown:")
        logger.info(f"     {pools_no_state} pools missing state data")
        logger.info(f"     {pools_no_liquidity} pools with zero liquidity")
        logger.info(f"     {pools_no_decimals} pools missing token decimals")
        logger.info(f"     {pools_no_prices} pools missing both token prices")
        logger.info(f"     {pools_calc_error} pools with calculation errors")
        logger.info(f"  💰 Discovered {len(updated_prices) - len(token_prices)} new token prices from V3 pools")

        return filtered_pools, updated_prices

    async def filter_v4_pools(
        self,
        pools: Dict[str, Dict],
        token_symbols: Dict[str, str],
        token_decimals: Dict[str, int],
        token_prices: Dict[str, Decimal]
    ) -> Tuple[Dict[str, Dict], Dict[str, Decimal]]:
        """
        Filter Uniswap V4 pools by minimum liquidity.

        V4 uses same concentrated liquidity math as V3.

        Args:
            pools: Dictionary of pool_id -> pool_data
            token_symbols: Dictionary of token_address -> symbol
            token_decimals: Dictionary of token_address -> decimals
            token_prices: Dictionary of token_address -> price (from V2 + Hyperliquid)

        Returns:
            Tuple of (filtered_pools, updated_token_prices)
        """
        if not pools:
            logger.warning("No V4 pools to filter")
            return {}, {}

        logger.info(f"🔍 Filtering {len(pools)} V4 pools by liquidity...")

        # Batch fetch pool state (sqrtPriceX96, liquidity, tick)
        pool_ids = list(pools.keys())
        from src.batchers.base import BatchConfig
        batch_config = BatchConfig(batch_size=50)
        batcher = UniswapV4DataBatcher(self.web3, config=batch_config)

        try:
            pool_states = await batcher.fetch_pools_chunked(pool_ids)
            logger.info(f"  Fetched state for {len(pool_states)} V4 pools")
        except Exception as e:
            logger.error(f"Failed to fetch V4 pool states: {e}")
            return {}, token_prices

        # Filter pools by liquidity
        filtered_pools = {}
        pools_below_threshold = 0
        pools_no_state = 0
        pools_no_decimals = 0
        pools_no_prices = 0
        pools_zero_liquidity = 0
        pools_calc_error = 0
        updated_prices = dict(token_prices)

        for pool_id, pool_data in pools.items():
            # Get pool state
            state = pool_states.get(pool_id.lower())
            if not state:
                pools_no_state += 1
                logger.debug(f"  No state data for pool {pool_id[:10]}...")
                continue

            token0_addr = pool_data['token0']['address']
            token1_addr = pool_data['token1']['address']

            # Normalize addresses for price/decimal lookup (zero address -> WETH)
            token0_lookup = normalize_token_for_pricing(token0_addr)
            token1_lookup = normalize_token_for_pricing(token1_addr)

            # Get pool parameters
            try:
                sqrt_price_x96 = int(state['sqrtPriceX96'])
                liquidity = int(state['liquidity'])
                tick = int(state['tick'])
            except (ValueError, TypeError, KeyError) as e:
                logger.debug(f"  Invalid pool state for {pool_id[:10]}...: {e}")
                continue

            # Skip pools with no liquidity
            if liquidity == 0:
                pools_zero_liquidity += 1
                continue

            # Get decimals - skip if we don't have this info
            # Use normalized addresses for lookup (treats native ETH as WETH)
            decimals0 = token_decimals.get(token0_lookup)
            decimals1 = token_decimals.get(token1_lookup)

            if decimals0 is None or decimals1 is None:
                pools_no_decimals += 1
                logger.debug(f"  Missing decimals for pool (token0={decimals0}, token1={decimals1})")
                continue

            # Get prices - use normalized addresses for lookup
            price0 = updated_prices.get(token0_lookup)
            price1 = updated_prices.get(token1_lookup)

            # Skip if we have neither price
            if price0 is None and price1 is None:
                pools_no_prices += 1
                continue

            # Calculate token amounts at current price
            # For V4 (like V3), we calculate amounts in a range around current tick
            # Using tick +/- 100 (~1% price movement) as proxy for "active" liquidity
            try:
                tick_lower = tick - 100
                tick_upper = tick + 100

                sqrt_ratio_a = get_sqrt_ratio_at_tick(tick_lower)
                sqrt_ratio_b = get_sqrt_ratio_at_tick(tick_upper)

                # Calculate token amounts
                amount0 = get_amount0_delta(sqrt_ratio_a, sqrt_ratio_b, liquidity, roundUp=False)
                amount1 = get_amount1_delta(sqrt_ratio_a, sqrt_ratio_b, liquidity, roundUp=False)

                # Convert to human-readable amounts
                amount0_decimal = Decimal(abs(amount0)) / Decimal(10 ** decimals0)
                amount1_decimal = Decimal(abs(amount1)) / Decimal(10 ** decimals1)

                # Calculate USD value
                value0_usd = amount0_decimal * price0 if price0 else Decimal(0)
                value1_usd = amount1_decimal * price1 if price1 else Decimal(0)
                total_liquidity_usd = value0_usd + value1_usd

                # If we only have one price, try to infer the other
                if price0 and not price1 and amount1_decimal > 0:
                    # Calculate implied price from pool ratio
                    price_ratio = Decimal(sqrt_price_x96 ** 2) / Decimal(2 ** 192)
                    implied_price1 = price0 / price_ratio if price_ratio > 0 else None
                    if implied_price1:
                        value1_usd = amount1_decimal * implied_price1
                        total_liquidity_usd = value0_usd + value1_usd
                        # Store with normalized address (zero address stored as WETH)
                        updated_prices[token1_lookup] = implied_price1

                elif price1 and not price0 and amount0_decimal > 0:
                    # Calculate implied price from pool ratio
                    price_ratio = Decimal(sqrt_price_x96 ** 2) / Decimal(2 ** 192)
                    implied_price0 = price1 * price_ratio
                    if implied_price0:
                        value0_usd = amount0_decimal * implied_price0
                        total_liquidity_usd = value0_usd + value1_usd
                        # Store with normalized address (zero address stored as WETH)
                        updated_prices[token0_lookup] = implied_price0

                # Filter by minimum liquidity
                if total_liquidity_usd >= self.min_liquidity_usd:
                    filtered_pools[pool_id] = pool_data
                else:
                    pools_below_threshold += 1

            except Exception as e:
                pools_calc_error += 1
                logger.debug(f"  Error calculating liquidity for {pool_id[:10]}...: {e}")
                continue

        # Log summary
        logger.info(f"  ✅ Filtered {len(filtered_pools)} V4 pools")
        logger.info(f"     {pools_below_threshold} pools below ${self.min_liquidity_usd} threshold")

        # Log reasons for filtering
        total_filtered = pools_no_state + pools_zero_liquidity + pools_no_decimals + pools_no_prices + pools_calc_error + pools_below_threshold
        if total_filtered > 0:
            logger.info(f"  📊 Filtering breakdown:")
            logger.info(f"     {pools_no_state} pools missing state data")
            logger.info(f"     {pools_zero_liquidity} pools with zero liquidity")
            logger.info(f"     {pools_no_decimals} pools missing token decimals")
            logger.info(f"     {pools_no_prices} pools missing both token prices")
            logger.info(f"     {pools_calc_error} pools with calculation errors")
            logger.info(f"     {pools_below_threshold} pools below liquidity threshold")

        return filtered_pools, updated_prices

    async def filter_pools_by_protocol(
        self,
        v2_pairs: Dict[str, Dict],
        v3_pairs: Dict[str, Dict],
        v4_pairs: Dict[str, Dict],
        token_symbols: Dict[str, str],
        token_decimals: Dict[str, int]
    ) -> Dict:
        """
        Filter all pools by protocol type.

        Args:
            v2_pairs: V2 pool dictionary
            v3_pairs: V3 pool dictionary
            v4_pairs: V4 pool dictionary
            token_symbols: Token symbols mapping
            token_decimals: Token decimals mapping

        Returns:
            Dictionary with filtered pools by protocol
        """
        logger.info("=" * 80)
        logger.info("🔍 Filtering Pools by Liquidity")
        logger.info("=" * 80)

        # Fetch Hyperliquid prices
        hyperliquid_prices = await self.fetch_hyperliquid_prices()

        # Filter V2 pools
        filtered_v2, discovered_prices = await self.filter_v2_pools(
            v2_pairs,
            token_symbols,
            token_decimals,
            hyperliquid_prices
        )

        # Filter V3 pools using prices discovered from V2 + Hyperliquid
        filtered_v3, updated_prices = await self.filter_v3_pools(
            v3_pairs,
            token_symbols,
            token_decimals,
            discovered_prices
        )

        # Filter V4 pools using prices discovered from V2 + V3 + Hyperliquid
        filtered_v4, final_prices = await self.filter_v4_pools(
            v4_pairs,
            token_symbols,
            token_decimals,
            updated_prices
        )

        return {
            'v2_pairs': filtered_v2,
            'v3_pairs': filtered_v3,
            'v4_pairs': filtered_v4,
            'discovered_token_prices': final_prices
        }

    async def filter_pools_with_price_discovery(
        self,
        storage,
        all_tokens: set,
        token_symbols: Dict[str, str],
        token_decimals: Dict[str, int],
        v2_factories: list,
        v3_factories: list,
        v4_factories: list,
        pools: Dict[str, Dict]
    ) -> Dict:
        """
        Filter pools by liquidity using comprehensive price discovery.

        This method:
        1. Fetches exchange prices (Hyperliquid + Binance)
        2. Discovers prices through V2/V3/V4 pools iteratively
        3. Filters pools by minimum liquidity threshold

        Args:
            storage: PostgresStorage instance
            all_tokens: Set of all token addresses to price
            token_symbols: Dict of token_address -> symbol
            token_decimals: Dict of token_address -> decimals
            v2_factories: List of V2 factory addresses
            v3_factories: List of V3 factory addresses
            v4_factories: List of V4 factory addresses
            pools: Dict of pool_address -> pool_data (all protocols combined)

        Returns:
            Dict with filtered pools and discovered prices
        """
        logger.info("=" * 80)
        logger.info("🔍 LIQUIDITY FILTERING WITH PRICE DISCOVERY")
        logger.info("=" * 80)

        # Step 1: Fetch exchange prices
        logger.info("\n📊 Step 1: Fetching exchange prices...")
        hyperliquid_prices = await self.fetch_hyperliquid_prices()
        binance_prices = await self.fetch_binance_prices()

        # Map exchange prices to token addresses
        initial_prices = {}
        for token_addr, symbol in token_symbols.items():
            symbol_upper = symbol.upper()
            exchange_symbol = "ETH" if symbol_upper == "WETH" else "BTC" if symbol_upper == "WBTC" else symbol_upper

            if exchange_symbol in binance_prices:
                initial_prices[token_addr] = binance_prices[exchange_symbol]
            elif symbol_upper in binance_prices:
                initial_prices[token_addr] = binance_prices[symbol_upper]
            elif exchange_symbol in hyperliquid_prices:
                initial_prices[token_addr] = hyperliquid_prices[exchange_symbol]
            elif symbol_upper in hyperliquid_prices:
                initial_prices[token_addr] = hyperliquid_prices[symbol_upper]

        logger.info(f"   ✅ Mapped {len(initial_prices)} exchange prices")

        # Step 2: V2 Price Discovery
        logger.info("\n🔍 Step 2: V2 Price Discovery...")
        prices_after_v2 = await self.discover_prices_from_v2_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=initial_prices,
            token_decimals=token_decimals,
            v2_factories=v2_factories,
            max_iterations=10
        )
        logger.info(f"   ✅ Total prices after V2: {len(prices_after_v2)}")

        # Step 3: V3 Price Discovery
        logger.info("\n🔍 Step 3: V3 Price Discovery...")
        prices_after_v3 = await self.discover_prices_from_v3_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=prices_after_v2,
            token_decimals=token_decimals,
            v3_factories=v3_factories,
            max_iterations=10
        )
        logger.info(f"   ✅ Total prices after V3: {len(prices_after_v3)}")

        # Step 4: V4 Price Discovery
        logger.info("\n🔍 Step 4: V4 Price Discovery...")
        final_prices = await self.discover_prices_from_v4_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=prices_after_v3,
            token_decimals=token_decimals,
            v4_factories=v4_factories,
            max_iterations=10
        )
        logger.info(f"   ✅ Total prices after V4: {len(final_prices)}")

        # Store prices in self for filtering
        self.prices = final_prices

        # Step 5: Filter pools by liquidity (batched by protocol)
        logger.info(f"\n💰 Step 5: Filtering {len(pools)} pools by liquidity...")

        # Separate pools by protocol
        v2_pools = {addr: data for addr, data in pools.items() if data.get('protocol') == 'v2'}
        v3_pools = {addr: data for addr, data in pools.items() if data.get('protocol') == 'v3'}
        v4_pools = {addr: data for addr, data in pools.items() if data.get('protocol') == 'v4'}

        logger.info(f"   V2: {len(v2_pools)}, V3: {len(v3_pools)}, V4: {len(v4_pools)} pools")

        filtered_pools = {}
        pools_below_threshold = 0

        # Batch fetch V2 reserves
        if v2_pools:
            logger.info(f"   Fetching reserves for {len(v2_pools)} V2 pools...")
            v2_batcher = UniswapV2ReservesBatcher(self.web3)
            v2_reserves = await v2_batcher.fetch_reserves_chunked(list(v2_pools.keys()))

            for pool_addr, pool_data in v2_pools.items():
                reserves = v2_reserves.get(pool_addr.lower())
                if not reserves:
                    pools_below_threshold += 1
                    continue

                liquidity_usd = self._calculate_v2_liquidity_from_reserves(
                    pool_data, reserves, final_prices, token_decimals
                )

                if liquidity_usd is not None and liquidity_usd >= self.min_liquidity_v2_usd:
                    pool_data['liquidity_usd'] = liquidity_usd
                    filtered_pools[pool_addr] = pool_data
                else:
                    pools_below_threshold += 1

        # Batch fetch V3 state
        if v3_pools:
            logger.info(f"   Fetching state for {len(v3_pools)} V3 pools...")
            from src.batchers.uniswap_v3_data import UniswapV3DataBatcher
            from src.batchers.base import BatchConfig
            v3_batcher = UniswapV3DataBatcher(self.web3, config=BatchConfig(batch_size=50))
            v3_states = await v3_batcher.fetch_pools_chunked(list(v3_pools.keys()))

            for pool_addr, pool_data in v3_pools.items():
                state = v3_states.get(pool_addr.lower())
                if not state or 'liquidity' not in state:
                    pools_below_threshold += 1
                    continue

                liquidity_usd = self._calculate_v3_v4_liquidity_from_state(
                    pool_data, state, final_prices, token_decimals
                )

                if liquidity_usd is not None and liquidity_usd >= self.min_liquidity_v3_usd:
                    pool_data['liquidity_usd'] = liquidity_usd
                    filtered_pools[pool_addr] = pool_data
                else:
                    pools_below_threshold += 1

        # Batch fetch V4 state
        if v4_pools:
            logger.info(f"   Fetching state for {len(v4_pools)} V4 pools...")
            from src.batchers.uniswap_v4_data import UniswapV4DataBatcher
            from src.batchers.base import BatchConfig
            v4_batcher = UniswapV4DataBatcher(self.web3, config=BatchConfig(batch_size=50))
            v4_states = await v4_batcher.fetch_pools_chunked(list(v4_pools.keys()))

            for pool_addr, pool_data in v4_pools.items():
                state = v4_states.get(pool_addr.lower())
                if not state or 'liquidity' not in state:
                    pools_below_threshold += 1
                    continue

                liquidity_usd = self._calculate_v3_v4_liquidity_from_state(
                    pool_data, state, final_prices, token_decimals
                )

                if liquidity_usd is not None and liquidity_usd >= self.min_liquidity_v4_usd:
                    pool_data['liquidity_usd'] = liquidity_usd
                    filtered_pools[pool_addr] = pool_data
                else:
                    pools_below_threshold += 1

        logger.info(f"   ✅ Filtered to {len(filtered_pools)} pools above threshold")
        logger.info(f"   ❌ Excluded {pools_below_threshold} pools below threshold")

        logger.info("=" * 80)

        return {
            'filtered_pools': filtered_pools,
            'discovered_prices': final_prices,
            'initial_prices': initial_prices,
            'exchange_prices': len(initial_prices),
            'total_prices': len(final_prices)
        }

    def _calculate_v2_liquidity_from_reserves(
        self,
        pool_data: Dict,
        reserves: Dict,
        prices: Dict[str, Decimal],
        token_decimals: Dict[str, int]
    ) -> Optional[Decimal]:
        """
        Calculate liquidity for a V2 pool using pre-fetched reserves.

        Args:
            pool_data: Pool metadata including token addresses
            reserves: Pre-fetched reserve data from batcher
            prices: Token prices in USD
            token_decimals: Token decimals mapping

        Returns:
            Liquidity in USD or None if cannot calculate
        """
        if not reserves:
            return None

        token0_addr = pool_data['token0']['address']
        token1_addr = pool_data['token1']['address']

        # Get prices
        price0 = prices.get(token0_addr)
        price1 = prices.get(token1_addr)

        if not price0 and not price1:
            return None

        # Parse reserves
        reserve0 = Decimal(str(int(reserves['reserve0'], 16)))
        reserve1 = Decimal(str(int(reserves['reserve1'], 16)))
        decimals0 = token_decimals.get(token0_addr, 18)
        decimals1 = token_decimals.get(token1_addr, 18)

        # Adjust for decimals
        adj_reserve0 = reserve0 / Decimal(10 ** decimals0)
        adj_reserve1 = reserve1 / Decimal(10 ** decimals1)

        # Calculate total liquidity
        liquidity_usd = Decimal(0)
        if price0:
            liquidity_usd += adj_reserve0 * price0
        if price1:
            liquidity_usd += adj_reserve1 * price1

        return liquidity_usd

    def _calculate_v3_v4_liquidity_from_state(
        self,
        pool_data: Dict,
        state: Dict,
        prices: Dict[str, Decimal],
        token_decimals: Dict[str, int]
    ) -> Optional[Decimal]:
        """
        Calculate liquidity for a V3/V4 pool using pre-fetched state.

        Args:
            pool_data: Pool metadata including token addresses
            state: Pre-fetched pool state from batcher
            prices: Token prices in USD
            token_decimals: Token decimals mapping

        Returns:
            Liquidity in USD or None if cannot calculate
        """
        if not state or 'liquidity' not in state:
            return None

        token0_addr = pool_data['token0']['address']
        token1_addr = pool_data['token1']['address']

        # Get prices
        price0 = prices.get(token0_addr)
        price1 = prices.get(token1_addr)

        if not price0 and not price1:
            return None

        # Parse state
        liquidity = int(state['liquidity'])
        sqrt_price_x96 = int(state['sqrtPriceX96'])

        if liquidity == 0:
            return None

        decimals0 = token_decimals.get(token0_addr, 18)
        decimals1 = token_decimals.get(token1_addr, 18)

        # Simplified TVL estimation using liquidity value
        # For V3/V4, liquidity represents sqrt(amount0 * amount1)
        # This is a simplified calculation - actual TVL requires integration over tick range
        liquidity_decimal = Decimal(liquidity)

        # Calculate rough USD value
        if price0 and price1:
            # Use geometric mean of both token values
            value0 = liquidity_decimal * price0 / Decimal(10 ** decimals0)
            value1 = liquidity_decimal * price1 / Decimal(10 ** decimals1)
            return (value0 + value1) / 2
        elif price0:
            return liquidity_decimal * price0 / Decimal(10 ** decimals0)
        elif price1:
            return liquidity_decimal * price1 / Decimal(10 ** decimals1)

        return None
