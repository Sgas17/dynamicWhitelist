"""
Liquidity Snapshot Generator Script

This script processes liquidity events (Mint/Burn) and generates snapshots
for all tracked pools. Designed to run on a schedule (daily/weekly).

Features:
- Process new events since last snapshot
- Store snapshots to PostgreSQL
- Store events to TimescaleDB
- Clean up old parquet files
- Detailed logging and statistics
"""

import asyncio
import logging
import sys
from datetime import datetime

from src.processors.pools.unified_liquidity_processor import UnifiedLiquidityProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Process liquidity events and generate snapshots."""
    logger.info("=" * 80)
    logger.info("LIQUIDITY SNAPSHOT GENERATOR")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now()}")
    logger.info("")

    try:
        # Initialize processor
        processor = UnifiedLiquidityProcessor(
            chain="ethereum", block_chunk_size=100000, enable_blacklist=True
        )

        # Validate configuration
        if not processor.validate_config():
            logger.error("Configuration validation failed")
            return 1

        # Get current statistics
        logger.info("Current snapshot statistics:")
        stats = processor.get_statistics()
        snapshot_stats = stats["snapshots"]
        logger.info(f"  Total snapshots: {snapshot_stats.get('total_snapshots', 0)}")
        logger.info(f"  Latest block: {snapshot_stats.get('latest_snapshot_block', 0)}")
        logger.info(f"  Protocols: {snapshot_stats.get('protocol_count', 0)}")
        logger.info("")

        # Process Uniswap V3 liquidity events
        logger.info("Processing Uniswap V3 liquidity events...")
        result_v3 = await processor.process_liquidity_snapshots(
            protocol="uniswap_v3",
            start_block=None,  # Auto-detect from last snapshot
            end_block=None,  # Process all available
            force_rebuild=False,
            cleanup_parquet=True,
        )

        if "error" in result_v3:
            logger.error(f"V3 processing failed: {result_v3['error']}")
        else:
            logger.info("✓ Uniswap V3 processing complete:")
            logger.info(f"  Start block: {result_v3['start_block']}")
            logger.info(f"  End block: {result_v3['end_block']}")
            logger.info(f"  Events processed: {result_v3['total_events_processed']}")
            logger.info(f"  Pools updated: {result_v3['pools_updated']}")
            logger.info(f"  Snapshots stored: {result_v3['snapshots_stored']}")
            logger.info(f"  Events stored: {result_v3['events_stored']}")
        logger.info("")

        # Process Uniswap V4 liquidity events (if available)
        logger.info("Processing Uniswap V4 liquidity events...")
        try:
            result_v4 = await processor.process_liquidity_snapshots(
                protocol="uniswap_v4",
                start_block=None,
                end_block=None,
                force_rebuild=False,
                cleanup_parquet=True,
            )

            if "error" in result_v4:
                logger.warning(f"V4 processing skipped: {result_v4['error']}")
            else:
                logger.info("✓ Uniswap V4 processing complete:")
                logger.info(f"  Start block: {result_v4['start_block']}")
                logger.info(f"  End block: {result_v4['end_block']}")
                logger.info(
                    f"  Events processed: {result_v4['total_events_processed']}"
                )
                logger.info(f"  Pools updated: {result_v4['pools_updated']}")
                logger.info(f"  Snapshots stored: {result_v4['snapshots_stored']}")
                logger.info(f"  Events stored: {result_v4['events_stored']}")
        except Exception as e:
            logger.warning(f"V4 processing failed (may not have events yet): {e}")
        logger.info("")

        # Get updated statistics
        logger.info("Updated snapshot statistics:")
        stats = processor.get_statistics()
        snapshot_stats = stats["snapshots"]
        logger.info(f"  Total snapshots: {snapshot_stats.get('total_snapshots', 0)}")
        logger.info(f"  Latest block: {snapshot_stats.get('latest_snapshot_block', 0)}")
        logger.info(
            f"  Avg ticks per pool: {snapshot_stats.get('avg_ticks_per_pool', 0):.2f}"
        )
        logger.info(
            f"  Hours since last update: {snapshot_stats.get('hours_since_last_update', 0):.2f}"
        )
        logger.info("")

        logger.info("=" * 80)
        logger.info("✓ SNAPSHOT GENERATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Finished at: {datetime.now()}")

        return 0

    except Exception as e:
        logger.error(f"Snapshot generation failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
