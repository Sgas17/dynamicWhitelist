"""Test script to verify RethDB returns actual tick data"""

import asyncio
import json
import logging
import os

from src.config.manager import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_rethdb_tick_data():
    # Initialize RethDB loader
    reth_db_path = os.getenv(
        "RETH_DB_PATH", "/var/lib/docker/volumes/eth-docker_reth-el-data/_data/db"
    )
    loader = RethSnapshotLoader(reth_db_path)

    # Initialize DB to get some pool addresses
    config = ConfigManager()
    db_config = {
        "host": config.database.POSTGRES_HOST,
        "port": config.database.POSTGRES_PORT,
        "user": config.database.POSTGRES_USER,
        "password": config.database.POSTGRES_PASSWORD,
        "database": config.database.POSTGRES_DB,
    }
    storage = PostgresStorage(config=db_config)
    await storage.connect()

    # Get V3 and V4 pools from our whitelist snapshots
    v3_pools = await storage.pool.fetch("""
        SELECT pool_address as address, tick_spacing
        FROM network_1_liquidity_snapshots
        WHERE protocol = 'v3' AND tick_spacing IS NOT NULL
        LIMIT 5
    """)

    v4_pools = await storage.pool.fetch("""
        SELECT pool_address as address, tick_spacing
        FROM network_1_liquidity_snapshots
        WHERE protocol = 'v4' AND tick_spacing IS NOT NULL
        LIMIT 5
    """)

    results = {}

    # Test V3 pools
    logger.info("=" * 80)
    logger.info("TESTING V3 POOLS")
    logger.info("=" * 80)

    for pool in v3_pools:
        pool_addr = pool["address"]
        tick_spacing = pool["tick_spacing"]

        logger.info(f"\nLoading V3 pool: {pool_addr}, tick_spacing={tick_spacing}")

        tick_data, block_number = loader.load_v3_pool_snapshot(
            pool_address=pool_addr, tick_spacing=tick_spacing
        )

        results[pool_addr] = {
            "protocol": "v3",
            "pool_address": pool_addr,
            "tick_spacing": tick_spacing,
            "block_number": block_number,
            "num_ticks": len(tick_data),
            "tick_data": tick_data,
            "sample_ticks": dict(list(tick_data.items())[:5]) if tick_data else {},
        }

        logger.info(f"  ✓ Loaded {len(tick_data)} ticks at block {block_number}")
        if tick_data:
            sample_tick = list(tick_data.items())[0]
            logger.info(f"  Sample tick: {sample_tick[0]} -> {sample_tick[1]}")

    # Test V4 pools
    logger.info("\n" + "=" * 80)
    logger.info("TESTING V4 POOLS")
    logger.info("=" * 80)

    for pool in v4_pools:
        pool_addr = pool["address"]  # This is the pool_id for V4
        tick_spacing = pool["tick_spacing"]

        logger.info(f"\nLoading V4 pool: {pool_addr}, tick_spacing={tick_spacing}")

        tick_data, block_number = loader.load_v4_pool_snapshot(
            pool_address="0x5302086A3a25d473aAbBd0356eFf8Dd811a4d89B",  # PoolManager
            pool_id=pool_addr,
            tick_spacing=tick_spacing,
        )

        results[pool_addr] = {
            "protocol": "v4",
            "pool_id": pool_addr,
            "tick_spacing": tick_spacing,
            "block_number": block_number,
            "num_ticks": len(tick_data),
            "tick_data": tick_data,
            "sample_ticks": dict(list(tick_data.items())[:5]) if tick_data else {},
        }

        logger.info(f"  ✓ Loaded {len(tick_data)} ticks at block {block_number}")
        if tick_data:
            sample_tick = list(tick_data.items())[0]
            logger.info(f"  Sample tick: {sample_tick[0]} -> {sample_tick[1]}")

    # Save to JSON
    output_file = "data/rethdb_tick_data_test.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"\n✅ Saved tick data to {output_file}")
    logger.info(f"   Total pools tested: {len(results)}")
    logger.info(
        f"   Total ticks collected: {sum(r['num_ticks'] for r in results.values())}"
    )


if __name__ == "__main__":
    asyncio.run(test_rethdb_tick_data())
