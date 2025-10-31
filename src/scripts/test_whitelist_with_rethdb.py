"""
Test Whitelist Creation with Reth DB Integration

This script tests the complete whitelist creation pipeline with Reth DB direct access
for liquidity data, and measures the total time taken.

Usage:
    # With Reth DB (fast):
    RETH_DB_PATH=/path/to/reth/db uv run python src/scripts/test_whitelist_with_rethdb.py

    # Without Reth DB (RPC fallback):
    uv run python src/scripts/test_whitelist_with_rethdb.py
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.whitelist.orchestrator import WhitelistOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Test whitelist creation with timing."""
    logger.info("="*80)
    logger.info("WHITELIST CREATION TEST - RETH DB INTEGRATION")
    logger.info("="*80)

    # Initialize config and storage
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
        orchestrator = WhitelistOrchestrator(storage, config)

        # Measure total time
        logger.info("\n🚀 Starting whitelist creation pipeline...")
        overall_start = time.time()

        # Run pipeline - liquidity thresholds are in config
        results = await orchestrator.run_pipeline(
            chain="ethereum",
            top_transfers=100,
            protocols=["uniswap_v2", "uniswap_v3", "uniswap_v4"],
            save_snapshots_to_db=True  # Save snapshots for verification
        )

        overall_elapsed = time.time() - overall_start

        # Print timing summary
        logger.info("\n" + "="*80)
        logger.info("⏱️  TIMING SUMMARY")
        logger.info("="*80)
        logger.info(f"Total pipeline time: {overall_elapsed:.2f}s ({overall_elapsed/60:.2f} minutes)")
        logger.info(f"")
        logger.info(f"Results:")
        logger.info(f"  Tokens whitelisted: {results['whitelist']['total_tokens']}")
        logger.info(f"  Stage 1 pools: {results['stage1_pools']['count']}")

        # Count pools by protocol
        v2_count = sum(1 for p in results['stage1_pools']['pools'] if p['protocol'] == 'v2')
        v3_count = sum(1 for p in results['stage1_pools']['pools'] if p['protocol'] == 'v3')
        v4_count = sum(1 for p in results['stage1_pools']['pools'] if p['protocol'] == 'v4')

        logger.info(f"    V2 pools: {v2_count}")
        logger.info(f"    V3 pools: {v3_count}")
        logger.info(f"    V4 pools: {v4_count}")

        logger.info(f"  Stage 2 pools: {results['stage2_pools']['count']}")
        logger.info(f"  Token prices: {len(results['token_prices'])}")
        logger.info(f"")
        logger.info(f"Liquidity thresholds used:")
        logger.info(f"  V2: ${config.chains.MIN_LIQUIDITY_V2:,.0f}")
        logger.info(f"  V3: ${config.chains.MIN_LIQUIDITY_V3:,.0f}")
        logger.info(f"  V4: ${config.chains.MIN_LIQUIDITY_V4:,.0f}")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"❌ Whitelist creation failed: {e}", exc_info=True)
        return 1

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
