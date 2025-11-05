"""
Test the price discovery methods in PoolLiquidityFilter.

This demonstrates how to use the new V2, V3, and V4 price discovery methods.
"""

import asyncio
import logging
import sys
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from web3 import Web3

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.whitelist.liquidity_filter import PoolLiquidityFilter

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


async def main():
    """Test price discovery methods."""
    # Initialize
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
        # Get RPC and setup
        rpc_url = config.chains.get_rpc_url("ethereum")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        liquidity_filter = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_usd=1000.0,
            min_liquidity_v2_usd=1000.0,
            chain="ethereum",
        )

        # Build whitelist and collect token metadata (symbols + decimals)
        logger.info("📊 Building whitelist and collecting token metadata...")
        from src.whitelist.builder import TokenWhitelistBuilder

        builder = TokenWhitelistBuilder(storage=storage)
        whitelist_result = await builder.build_whitelist()
        whitelisted_tokens = set(whitelist_result["tokens"])

        # Extract symbols and decimals from builder's token_info
        token_symbols = {
            addr: info["symbol"]
            for addr, info in builder.token_info.items()
            if "symbol" in info
        }
        token_decimals = {
            addr: info["decimals"]
            for addr, info in builder.token_info.items()
            if "decimals" in info
        }

        logger.info(f"✅ Whitelist built with {len(whitelisted_tokens)} tokens")
        logger.info(
            f"✅ Collected {len(token_symbols)} symbols and {len(token_decimals)} decimals from builder\n"
        )

        trusted_tokens = config.chains.get_trusted_tokens_for_chain().get(
            "ethereum", {}
        )
        trusted_addresses = set(addr.lower() for addr in trusted_tokens.values())

        query = """
        SELECT DISTINCT
            LOWER(asset0) as token0,
            LOWER(asset1) as token1
        FROM network_1__dex_pools
        WHERE factory IN (
            SELECT LOWER(address) FROM (VALUES
                ('0x1F98431c8aD98523631AE4a59f267346ea31F984'),
                ('0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865')
            ) AS t(address)
        )
        AND (
            (LOWER(asset0) = ANY($1) AND LOWER(asset1) = ANY($2))
            OR
            (LOWER(asset0) = ANY($2) AND LOWER(asset1) = ANY($1))
        )
        """

        async with storage.pool.acquire() as conn:
            results = await conn.fetch(
                query,
                [addr.lower() for addr in whitelisted_tokens],
                [addr.lower() for addr in trusted_addresses],
            )

        # Collect all tokens from pools (both whitelisted and trusted)
        all_tokens_in_pools = set()
        for row in results:
            all_tokens_in_pools.add(row["token0"])
            all_tokens_in_pools.add(row["token1"])

        # For price discovery, we need to price ALL tokens (whitelisted + trusted)
        all_tokens = all_tokens_in_pools

        # Count only whitelisted tokens for coverage calculation
        whitelisted_in_pools = all_tokens_in_pools & whitelisted_tokens

        trusted_in_pools = trusted_addresses & all_tokens_in_pools
        logger.info(f"✅ Found {len(all_tokens)} total tokens in V3 pools")
        logger.info(
            f"   ({len(whitelisted_in_pools)} whitelisted + {len(trusted_in_pools)} trusted)\n"
        )

        # Fetch missing decimals on-chain for tokens not in builder's token_info
        missing_decimals = all_tokens - set(token_decimals.keys())
        if missing_decimals:
            logger.info(
                f"🔍 Fetching {len(missing_decimals)} missing decimals on-chain..."
            )
            token_decimals = await liquidity_filter.fetch_missing_decimals(
                all_tokens, token_decimals
            )
            logger.info(f"✅ Now have {len(token_decimals)} total decimals\n")
        else:
            logger.info("✅ All tokens have decimals!\n")

        # Step 1: Fetch exchange prices
        logger.info("=" * 80)
        logger.info("STEP 1: FETCH EXCHANGE PRICES")
        logger.info("=" * 80)

        hyperliquid_prices = await liquidity_filter.fetch_hyperliquid_prices()
        binance_prices = await liquidity_filter.fetch_binance_prices()

        # Map to addresses with WETH->ETH mapping
        initial_prices = {}
        for token_addr, symbol in token_symbols.items():
            symbol_upper = symbol.upper()
            exchange_symbol = (
                "ETH"
                if symbol_upper == "WETH"
                else "BTC"
                if symbol_upper == "WBTC"
                else symbol_upper
            )

            if exchange_symbol in binance_prices:
                initial_prices[token_addr] = binance_prices[exchange_symbol]
            elif symbol_upper in binance_prices:
                initial_prices[token_addr] = binance_prices[symbol_upper]
            elif exchange_symbol in hyperliquid_prices:
                initial_prices[token_addr] = hyperliquid_prices[exchange_symbol]
            elif symbol_upper in hyperliquid_prices:
                initial_prices[token_addr] = hyperliquid_prices[symbol_upper]

        logger.info(f"✅ Mapped {len(initial_prices)} exchange prices\n")

        # Step 2: V2 Price Discovery
        logger.info("=" * 80)
        logger.info("STEP 2: V2 PRICE DISCOVERY")
        logger.info("=" * 80)

        v2_factories = config.protocols.get_factory_addresses("uniswap_v2", "ethereum")

        prices_after_v2 = await liquidity_filter.discover_prices_from_v2_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=initial_prices,
            token_decimals=token_decimals,
            v2_factories=v2_factories,
            max_iterations=10,
        )

        logger.info(f"Total prices after V2: {len(prices_after_v2)}\n")

        # Step 3: V3 Price Discovery
        logger.info("=" * 80)
        logger.info("STEP 3: V3 PRICE DISCOVERY")
        logger.info("=" * 80)

        v3_factories = [
            "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # Uniswap V3
            "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",  # PancakeSwap V3
        ]

        prices_after_v3 = await liquidity_filter.discover_prices_from_v3_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=prices_after_v2,
            token_decimals=token_decimals,
            v3_factories=v3_factories,
            max_iterations=10,
        )

        logger.info(f"Total prices after V3: {len(prices_after_v3)}\n")

        # Step 4: V4 Price Discovery
        logger.info("=" * 80)
        logger.info("STEP 4: V4 PRICE DISCOVERY")
        logger.info("=" * 80)

        v4_factories = [
            "0x000000000004444c5dc75cb358380d2e3de08a90"  # V4 Pool Manager on Ethereum
        ]

        prices_after_v4 = await liquidity_filter.discover_prices_from_v4_pools(
            storage=storage,
            all_tokens=all_tokens,
            initial_prices=prices_after_v3,
            token_decimals=token_decimals,
            v4_factories=v4_factories,
            max_iterations=10,
        )

        logger.info(f"Total prices after V4: {len(prices_after_v4)}\n")

        # Summary - calculate coverage for whitelisted tokens only
        whitelisted_with_prices = set(prices_after_v4.keys()) & whitelisted_in_pools

        # Find extra tokens that got priced but weren't in original pool query
        all_priced = set(prices_after_v4.keys())
        extra_tokens_discovered = all_priced - all_tokens_in_pools

        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Tokens from V3 pools query: {len(all_tokens)}")
        logger.info(f"  - Whitelisted: {len(whitelisted_in_pools)}")
        logger.info(f"  - Trusted (WETH, USDC, etc.): {len(trusted_in_pools)}")
        logger.info(
            f"Additional tokens discovered via V2/V3/V4: {len(extra_tokens_discovered)}"
        )
        logger.info(f"Total unique token prices: {len(all_priced)}")
        logger.info(f"")
        logger.info(f"Exchange prices: {len(initial_prices)}")
        logger.info(
            f"After V2: {len(prices_after_v2)} (+{len(prices_after_v2) - len(initial_prices)})"
        )
        logger.info(
            f"After V3: {len(prices_after_v3)} (+{len(prices_after_v3) - len(prices_after_v2)})"
        )
        logger.info(
            f"After V4: {len(prices_after_v4)} (+{len(prices_after_v4) - len(prices_after_v3)})"
        )
        logger.info(f"")
        logger.info(
            f"Whitelisted tokens with prices: {len(whitelisted_with_prices)} / {len(whitelisted_in_pools)}"
        )
        logger.info(
            f"Coverage (whitelisted only): {100 * len(whitelisted_with_prices) / len(whitelisted_in_pools):.1f}%"
        )
        logger.info(
            f"Still missing: {len(whitelisted_in_pools) - len(whitelisted_with_prices)}"
        )
        logger.info("=" * 80)

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
