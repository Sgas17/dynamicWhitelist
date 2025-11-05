"""
Profile scraping performance for different pool protocols.

Measures how long it takes to scrape pools of different types to determine
optimal batch sizes for time-bounded (12 second) batching.
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.storage.postgres import PostgresStorage
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def profile_v2_scraping(
    reth_loader: RethSnapshotLoader, storage: PostgresStorage, sample_size: int = 100
) -> Dict:
    """
    Profile V2 pool scraping performance.

    Args:
        reth_loader: Reth snapshot loader
        storage: Database storage
        sample_size: Number of V2 pools to test

    Returns:
        Dict with performance metrics
    """
    logger.info(f"Profiling V2 pool scraping ({sample_size} pools)...")

    # Get sample V2 pools from database
    query = """
    SELECT DISTINCT address, LOWER(factory) as factory
    FROM network_1_dex_pools_cryo
    WHERE LOWER(factory) IN (
        '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',  -- Uniswap V2
        '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac'   -- Sushiswap
    )
    LIMIT $1
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, sample_size)

    pool_addresses = [row["address"] for row in results]
    logger.info(f"Got {len(pool_addresses)} V2 pools to profile")

    # Time the scraping
    start_time = time.time()

    for pool_addr in pool_addresses:
        try:
            reserves, block_number = reth_loader.load_v2_pool_snapshot(pool_addr)
        except Exception as e:
            logger.warning(f"Failed to load {pool_addr}: {e}")
            continue

    end_time = time.time()
    duration = end_time - start_time

    avg_time_per_pool = duration / len(pool_addresses)
    pools_per_second = len(pool_addresses) / duration
    pools_per_12_seconds = pools_per_second * 12

    metrics = {
        "protocol": "v2",
        "sample_size": len(pool_addresses),
        "total_duration_seconds": duration,
        "avg_time_per_pool_seconds": avg_time_per_pool,
        "pools_per_second": pools_per_second,
        "recommended_batch_size_12s": int(
            pools_per_12_seconds * 0.8
        ),  # 80% safety margin
    }

    logger.info(
        f"V2 Performance: {pools_per_second:.2f} pools/sec, "
        f"recommended batch size: {metrics['recommended_batch_size_12s']}"
    )

    return metrics


async def profile_v3_scraping(
    reth_loader: RethSnapshotLoader, storage: PostgresStorage, sample_size: int = 100
) -> Dict:
    """
    Profile V3 pool scraping performance (slot0 only for filtering).

    Args:
        reth_loader: Reth snapshot loader
        storage: Database storage
        sample_size: Number of V3 pools to test

    Returns:
        Dict with performance metrics
    """
    logger.info(f"Profiling V3 pool scraping ({sample_size} pools)...")

    # Get sample V3 pools from database
    query = """
    SELECT DISTINCT address, tick_spacing, LOWER(factory) as factory
    FROM network_1_dex_pools_cryo
    WHERE LOWER(factory) = '0x1f98431c8ad98523631ae4a59f267346ea31f984'  -- Uniswap V3
    AND tick_spacing IS NOT NULL
    LIMIT $1
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, sample_size)

    pool_configs = [
        {"address": row["address"], "tick_spacing": row["tick_spacing"]}
        for row in results
    ]
    logger.info(f"Got {len(pool_configs)} V3 pools to profile")

    # Time the batch scraping (slot0 only)
    start_time = time.time()

    try:
        states = reth_loader.batch_load_v3_states(pool_configs)
    except Exception as e:
        logger.error(f"Failed to batch load V3 states: {e}")
        return {}

    end_time = time.time()
    duration = end_time - start_time

    avg_time_per_pool = duration / len(pool_configs)
    pools_per_second = len(pool_configs) / duration
    pools_per_12_seconds = pools_per_second * 12

    metrics = {
        "protocol": "v3_slot0_only",
        "sample_size": len(pool_configs),
        "total_duration_seconds": duration,
        "avg_time_per_pool_seconds": avg_time_per_pool,
        "pools_per_second": pools_per_second,
        "recommended_batch_size_12s": int(
            pools_per_12_seconds * 0.8
        ),  # 80% safety margin
    }

    logger.info(
        f"V3 (slot0) Performance: {pools_per_second:.2f} pools/sec, "
        f"recommended batch size: {metrics['recommended_batch_size_12s']}"
    )

    return metrics


async def profile_v4_scraping(
    reth_loader: RethSnapshotLoader, storage: PostgresStorage, sample_size: int = 50
) -> Dict:
    """
    Profile V4 pool scraping performance (slot0 only for filtering).

    Args:
        reth_loader: Reth snapshot loader
        storage: Database storage
        sample_size: Number of V4 pools to test

    Returns:
        Dict with performance metrics
    """
    logger.info(f"Profiling V4 pool scraping ({sample_size} pools)...")

    # Get sample V4 pools from database
    query = """
    SELECT DISTINCT address as pool_id, tick_spacing, LOWER(factory) as pool_manager
    FROM network_1_dex_pools_cryo
    WHERE LOWER(factory) IN (
        '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865',  -- V4 PoolManager
        '0x000000000019e0c1e6345a21f33c11f6d75a74d9'   -- Alternative V4 address
    )
    AND tick_spacing IS NOT NULL
    LIMIT $1
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, sample_size)

    if not results:
        logger.warning("No V4 pools found in database")
        return {
            "protocol": "v4_slot0_only",
            "sample_size": 0,
            "total_duration_seconds": 0,
            "avg_time_per_pool_seconds": 0,
            "pools_per_second": 0,
            "recommended_batch_size_12s": 0,
        }

    pool_configs = [
        {
            "pool_id": row["pool_id"],
            "tick_spacing": row["tick_spacing"],
            "pool_manager": row["pool_manager"],
        }
        for row in results
    ]
    logger.info(f"Got {len(pool_configs)} V4 pools to profile")

    # Time the batch scraping (slot0 only)
    start_time = time.time()

    try:
        states = reth_loader.batch_load_v4_states(pool_configs)
    except Exception as e:
        logger.error(f"Failed to batch load V4 states: {e}")
        return {}

    end_time = time.time()
    duration = end_time - start_time

    avg_time_per_pool = duration / len(pool_configs)
    pools_per_second = len(pool_configs) / duration
    pools_per_12_seconds = pools_per_second * 12

    metrics = {
        "protocol": "v4_slot0_only",
        "sample_size": len(pool_configs),
        "total_duration_seconds": duration,
        "avg_time_per_pool_seconds": avg_time_per_pool,
        "pools_per_second": pools_per_second,
        "recommended_batch_size_12s": int(
            pools_per_12_seconds * 0.8
        ),  # 80% safety margin
    }

    logger.info(
        f"V4 (slot0) Performance: {pools_per_second:.2f} pools/sec, "
        f"recommended batch size: {metrics['recommended_batch_size_12s']}"
    )

    return metrics


async def profile_full_tick_scraping(
    reth_loader: RethSnapshotLoader,
    storage: PostgresStorage,
    sample_size_per_protocol: int = 20,
) -> Dict:
    """
    Profile full tick data scraping (after filtering) for V3/V4.

    This is the slower operation that happens after pools pass liquidity filters.

    Args:
        reth_loader: Reth snapshot loader
        storage: Database storage
        sample_size_per_protocol: Number of pools to test per protocol

    Returns:
        Dict with performance metrics for both V3 and V4
    """
    logger.info(
        f"Profiling full tick data scraping ({sample_size_per_protocol} pools per protocol)..."
    )

    results = {}

    # Profile V3 full tick loading
    query_v3 = """
    SELECT DISTINCT address, tick_spacing
    FROM network_1_dex_pools_cryo
    WHERE LOWER(factory) = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
    AND tick_spacing IS NOT NULL
    LIMIT $1
    """

    async with storage.pool.acquire() as conn:
        v3_results = await conn.fetch(query_v3, sample_size_per_protocol)

    if v3_results:
        start_time = time.time()

        for row in v3_results:
            try:
                tick_data, block_number = reth_loader.load_v3_pool_snapshot(
                    pool_address=row["address"], tick_spacing=row["tick_spacing"]
                )
            except Exception as e:
                logger.warning(f"Failed to load V3 pool {row['address']}: {e}")
                continue

        end_time = time.time()
        duration = end_time - start_time

        pools_per_second = len(v3_results) / duration
        pools_per_12_seconds = pools_per_second * 12

        results["v3_full_ticks"] = {
            "protocol": "v3_full_ticks",
            "sample_size": len(v3_results),
            "total_duration_seconds": duration,
            "avg_time_per_pool_seconds": duration / len(v3_results),
            "pools_per_second": pools_per_second,
            "recommended_batch_size_12s": int(pools_per_12_seconds * 0.8),
        }

        logger.info(
            f"V3 (full ticks) Performance: {pools_per_second:.2f} pools/sec, "
            f"recommended batch size: {results['v3_full_ticks']['recommended_batch_size_12s']}"
        )

    # Profile V4 full tick loading
    query_v4 = """
    SELECT DISTINCT address as pool_id, tick_spacing, LOWER(factory) as pool_manager
    FROM network_1_dex_pools_cryo
    WHERE LOWER(factory) IN (
        '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865',
        '0x000000000019e0c1e6345a21f33c11f6d75a74d9'
    )
    AND tick_spacing IS NOT NULL
    LIMIT $1
    """

    async with storage.pool.acquire() as conn:
        v4_results = await conn.fetch(query_v4, sample_size_per_protocol)

    if v4_results:
        start_time = time.time()

        for row in v4_results:
            try:
                tick_data, bitmap_data, block_number = (
                    reth_loader.load_v4_pool_snapshot(
                        pool_id=row["pool_id"],
                        tick_spacing=row["tick_spacing"],
                        pool_manager=row["pool_manager"],
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to load V4 pool {row['pool_id']}: {e}")
                continue

        end_time = time.time()
        duration = end_time - start_time

        pools_per_second = len(v4_results) / duration
        pools_per_12_seconds = pools_per_second * 12

        results["v4_full_ticks"] = {
            "protocol": "v4_full_ticks",
            "sample_size": len(v4_results),
            "total_duration_seconds": duration,
            "avg_time_per_pool_seconds": duration / len(v4_results),
            "pools_per_second": pools_per_second,
            "recommended_batch_size_12s": int(pools_per_12_seconds * 0.8),
        }

        logger.info(
            f"V4 (full ticks) Performance: {pools_per_second:.2f} pools/sec, "
            f"recommended batch size: {results['v4_full_ticks']['recommended_batch_size_12s']}"
        )

    return results


async def main():
    """Run performance profiling for all protocols."""
    logger.info("=" * 80)
    logger.info("SCRAPING PERFORMANCE PROFILING")
    logger.info("=" * 80)

    # Initialize components
    reth_db_path = os.getenv("RETH_DB_PATH")
    if not reth_db_path:
        logger.error("RETH_DB_PATH environment variable not set")
        return

    logger.info(f"Using reth DB: {reth_db_path}")

    reth_loader = RethSnapshotLoader(reth_db_path)
    storage = PostgresStorage()
    await storage.connect()

    try:
        # Profile each protocol
        v2_metrics = await profile_v2_scraping(reth_loader, storage, sample_size=100)
        v3_metrics = await profile_v3_scraping(reth_loader, storage, sample_size=100)
        v4_metrics = await profile_v4_scraping(reth_loader, storage, sample_size=50)

        # Profile full tick scraping (post-filter operation)
        full_tick_metrics = await profile_full_tick_scraping(
            reth_loader, storage, sample_size_per_protocol=20
        )

        # Summary
        logger.info("=" * 80)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("=" * 80)
        logger.info("\nFiltering Phase (slot0 only for V3/V4):")
        logger.info(
            f"  V2: {v2_metrics.get('pools_per_second', 0):.2f} pools/sec → "
            f"Batch size: {v2_metrics.get('recommended_batch_size_12s', 0)}"
        )
        logger.info(
            f"  V3: {v3_metrics.get('pools_per_second', 0):.2f} pools/sec → "
            f"Batch size: {v3_metrics.get('recommended_batch_size_12s', 0)}"
        )
        logger.info(
            f"  V4: {v4_metrics.get('pools_per_second', 0):.2f} pools/sec → "
            f"Batch size: {v4_metrics.get('recommended_batch_size_12s', 0)}"
        )

        logger.info("\nFull Tick Collection Phase (post-filter):")
        if "v3_full_ticks" in full_tick_metrics:
            v3_full = full_tick_metrics["v3_full_ticks"]
            logger.info(
                f"  V3: {v3_full.get('pools_per_second', 0):.2f} pools/sec → "
                f"Batch size: {v3_full.get('recommended_batch_size_12s', 0)}"
            )
        if "v4_full_ticks" in full_tick_metrics:
            v4_full = full_tick_metrics["v4_full_ticks"]
            logger.info(
                f"  V4: {v4_full.get('pools_per_second', 0):.2f} pools/sec → "
                f"Batch size: {v4_full.get('recommended_batch_size_12s', 0)}"
            )

        logger.info("\nRecommendations for 12-second block time:")
        logger.info(
            f"  - V2 filtering: {v2_metrics.get('recommended_batch_size_12s', 0)} pools per batch"
        )
        logger.info(
            f"  - V3 filtering: {v3_metrics.get('recommended_batch_size_12s', 0)} pools per batch"
        )
        logger.info(
            f"  - V4 filtering: {v4_metrics.get('recommended_batch_size_12s', 0)} pools per batch"
        )
        if "v3_full_ticks" in full_tick_metrics:
            logger.info(
                f"  - V3 full ticks: {full_tick_metrics['v3_full_ticks'].get('recommended_batch_size_12s', 0)} pools per batch"
            )
        if "v4_full_ticks" in full_tick_metrics:
            logger.info(
                f"  - V4 full ticks: {full_tick_metrics['v4_full_ticks'].get('recommended_batch_size_12s', 0)} pools per batch"
            )

        logger.info("=" * 80)

    finally:
        await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
