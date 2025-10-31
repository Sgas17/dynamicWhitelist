"""
Test V3 Pool Filtering - Triage Script

This script tests V3 pool filtering step-by-step to identify where pools are being dropped.
"""

import asyncio
import logging
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Test V3 pool filtering step by step."""
    logger.info("="*80)
    logger.info("V3 POOL FILTERING TRIAGE")
    logger.info("="*80)

    config = ConfigManager()

    db_config = {
        'host': config.database.POSTGRES_HOST,
        'port': config.database.POSTGRES_PORT,
        'user': config.database.POSTGRES_USER,
        'password': config.database.POSTGRES_PASSWORD,
        'database': config.database.POSTGRES_DB,
        'pool_size': 10,
        'pool_timeout': 10
    }

    storage = PostgresStorage(config=db_config)
    await storage.connect()

    try:
        # Step 1: Count total V3 pools in database
        logger.info("\n📊 Step 1: Total V3 pools in database")
        query_total = """
        SELECT COUNT(*) as total
        FROM network_1_dex_pools_cryo
        WHERE LOWER(factory) IN (
            '0x1f98431c8ad98523631ae4a59f267346ea31f984',  -- Uniswap V3
            '0xc35dadb65012ec5796536bd9864ed8773abc74c4'   -- Sushiswap V3
        )
        """
        async with storage.pool.acquire() as conn:
            result = await conn.fetchrow(query_total)
            total_v3 = result['total']
            logger.info(f"  Total V3 pools: {total_v3}")

        # Step 2: Get sample V3 pools
        logger.info("\n📊 Step 2: Sample V3 pools (first 5)")
        query_sample = """
        SELECT
            address,
            LOWER(asset0) as token0,
            LOWER(asset1) as token1,
            LOWER(factory) as factory,
            tick_spacing,
            fee
        FROM network_1_dex_pools_cryo
        WHERE LOWER(factory) IN (
            '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
        )
        LIMIT 5
        """
        async with storage.pool.acquire() as conn:
            results = await conn.fetch(query_sample)
            for row in results:
                logger.info(f"  Pool: {row['address']}")
                logger.info(f"    Token0: {row['token0']}")
                logger.info(f"    Token1: {row['token1']}")
                logger.info(f"    Factory: {row['factory']}")
                logger.info(f"    Tick spacing: {row['tick_spacing']}")
                logger.info(f"    Fee: {row['fee']}")

        # Step 3: Get trusted tokens from config
        logger.info("\n📊 Step 3: Trusted tokens")
        all_trusted_tokens = config.chains.get_trusted_tokens_for_chain()
        trusted_tokens = all_trusted_tokens.get("ethereum", {})
        trusted_token_addresses = set(trusted_tokens.keys())
        logger.info(f"  Trusted tokens: {list(trusted_tokens.keys())}")

        # Step 4: Query V3 pools with trusted tokens
        logger.info("\n📊 Step 4: V3 pools with at least one trusted token")
        query_with_trusted = """
        SELECT
            address,
            LOWER(asset0) as token0,
            LOWER(asset1) as token1,
            LOWER(factory) as factory,
            tick_spacing
        FROM network_1_dex_pools_cryo
        WHERE LOWER(factory) IN (
            '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
        )
        AND (
            LOWER(asset0) = ANY($1) OR LOWER(asset1) = ANY($1)
        )
        """
        async with storage.pool.acquire() as conn:
            results = await conn.fetch(query_with_trusted, list(trusted_token_addresses))
            logger.info(f"  V3 pools with trusted token: {len(results)}")

            # Show first 5
            logger.info(f"\n  First 5 pools:")
            for row in results[:5]:
                token0_trusted = "✓" if row['token0'] in trusted_token_addresses else "✗"
                token1_trusted = "✓" if row['token1'] in trusted_token_addresses else "✗"
                logger.info(f"    {row['address']}")
                logger.info(f"      Token0: {row['token0']} {token0_trusted}")
                logger.info(f"      Token1: {row['token1']} {token1_trusted}")
                logger.info(f"      Tick spacing: {row['tick_spacing']}")

        # Step 5: Check if we can load liquidity data for these pools
        logger.info("\n📊 Step 5: Testing liquidity data loading")
        if len(results) > 0:
            test_pool = results[0]
            logger.info(f"  Testing pool: {test_pool['address']}")
            logger.info(f"  Tick spacing: {test_pool['tick_spacing']}")

            # Try to load from Reth DB
            from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader
            import os

            reth_db_path = os.getenv('RETH_DB_PATH', '/var/lib/docker/volumes/eth-docker_reth-el-data/_data/db')
            logger.info(f"  Reth DB path: {reth_db_path}")

            try:
                loader = RethSnapshotLoader(reth_db_path)
                pool_address, tick_data, block_number = loader.load_v3_pool_snapshot(
                    pool_address=test_pool['address'],
                    tick_spacing=test_pool['tick_spacing']
                )
                logger.info(f"  ✅ Successfully loaded tick data")
                logger.info(f"    Block: {block_number}")
                logger.info(f"    Ticks: {len(tick_data)}")

                if len(tick_data) > 0:
                    # Show first tick
                    first_tick = list(tick_data.items())[0]
                    logger.info(f"    First tick: {first_tick[0]} -> {first_tick[1]}")

            except Exception as e:
                logger.error(f"  ❌ Failed to load tick data: {e}")

        # Step 6: Check liquidity calculation
        logger.info("\n📊 Step 6: Testing liquidity calculation for V3 pool")
        if len(results) > 0 and len(tick_data) > 0:
            from src.whitelist.v3_math import calculate_v3_liquidity_usd

            # We need token prices - let's use placeholder values for WETH/USDC
            token0 = test_pool['token0']
            token1 = test_pool['token1']

            # Check if tokens are WETH or USDC
            weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

            if token0.lower() == weth.lower() or token1.lower() == weth.lower():
                logger.info(f"  Pool contains WETH")
            if token0.lower() == usdc.lower() or token1.lower() == usdc.lower():
                logger.info(f"  Pool contains USDC")

        logger.info("\n" + "="*80)
        logger.info("TRIAGE COMPLETE")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"❌ Triage failed: {e}", exc_info=True)
        return 1

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
