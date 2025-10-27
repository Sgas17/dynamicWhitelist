"""
Scheduled job for monitoring new pool creation events.

Run hourly via cron to detect and store new DEX pools across all Uniswap versions.
Uses database as source of truth for last processed block.
Leverages the unified pipeline for V2, V3, and V4.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.processors.pipeline.uniswap_pool_pipeline import UniswapPoolPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Process new pools across all Uniswap versions (V2, V3, V4)."""
    logger.info("=" * 80)
    logger.info("Starting Pool Creation Monitor")
    logger.info("=" * 80)

    try:
        # Initialize pipeline
        pipeline = UniswapPoolPipeline()

        # Process all Uniswap versions incrementally
        logger.info("\n🔄 Running incremental pool fetch for all versions...")

        result = await pipeline.run_all_versions(
            chain="ethereum",
            from_deployment=False,  # Incremental fetch from last processed block
            versions=["v2", "v3", "v4"]
        )

        # Display results
        logger.info("\n" + "=" * 80)
        if result["overall_success"]:
            logger.info("✅ Pool Creation Processing Complete")

            total_pools = 0
            for version, version_result in result.get("versions", {}).items():
                if version_result.get("success"):
                    count = version_result.get("processed_count", 0)
                    total_pools += count
                    metadata = version_result.get("metadata", {})

                    logger.info(f"\n  {version.upper()}:")
                    logger.info(f"    Processed: {count} pools")
                    logger.info(f"    Blocks: {metadata.get('start_block')} → {metadata.get('end_block')}")
                else:
                    logger.warning(f"\n  {version.upper()}: Failed - {version_result.get('error')}")

            logger.info(f"\n  Total new pools: {total_pools}")
        else:
            logger.error("❌ Some pool pipelines failed")
            for version, version_result in result.get("versions", {}).items():
                if not version_result.get("success"):
                    logger.error(f"  {version.upper()}: {version_result.get('error')}")
            return 1

        logger.info("\n" + "=" * 80)
        logger.info("Pool Creation Monitor Complete")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.exception(f"❌ Fatal error in pool creation monitor: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
