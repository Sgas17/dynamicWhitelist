#!/usr/bin/env python3
"""
Command-line interface for V4 pool pipeline.

Usage:
    uv run python -m src.processors.pipeline.cli --chain ethereum
    uv run python -m src.processors.pipeline.cli --chain ethereum --from-deployment
    uv run python -m src.processors.pipeline.cli --all-chains
"""

import argparse
import asyncio
import logging
import sys
from typing import Dict, Any

from src.processors.pipeline.v4_pool_pipeline import V4PoolPipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def format_pipeline_result(result: Dict[str, Any]) -> None:
    """Format and display pipeline results."""
    if result.get("success"):
        logger.info("✅ Pipeline completed successfully")

        if "processed_count" in result:
            logger.info(f"📊 Processed {result['processed_count']} pools")

        if "metadata" in result and result["metadata"]:
            metadata = result["metadata"]
            start = metadata.get("start_block")
            end = metadata.get("end_block")
            stored = metadata.get("stored_pools", 0)

            if start and end:
                logger.info(f"🔢 Block range: {start} → {end}")
            if stored:
                logger.info(f"💾 Stored {stored} pools to database")
    else:
        logger.error("❌ Pipeline failed")
        if "error" in result:
            logger.error(f"Error: {result['error']}")


def format_all_chains_result(results: Dict[str, Any]) -> None:
    """Format and display all chains results."""
    overall_success = results.get("overall_success", False)
    chain_results = results.get("chains", {})

    logger.info("=" * 60)
    logger.info("📊 ALL CHAINS SUMMARY")
    logger.info("=" * 60)

    success_count = 0
    total_pools = 0

    for chain, result in chain_results.items():
        if result.get("success"):
            success_count += 1
            pools = result.get("processed_count", 0)
            total_pools += pools
            logger.info(f"✅ {chain.upper()}: {pools} pools processed")
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"❌ {chain.upper()}: {error}")

    logger.info("=" * 60)
    logger.info(f"🎯 Results: {success_count}/{len(chain_results)} chains successful")
    logger.info(f"📊 Total pools processed: {total_pools}")

    if overall_success:
        logger.info("🎉 ALL CHAINS COMPLETED SUCCESSFULLY")
    else:
        logger.warning("⚠️  Some chains failed")


async def run_single_chain(args) -> bool:
    """Run pipeline for single chain."""
    logger.info(f"🚀 Starting V4 pool pipeline for {args.chain}")

    pipeline = V4PoolPipeline()
    result = await pipeline.run_full_pipeline(
        chain=args.chain,
        from_deployment=args.from_deployment,
        start_block=args.start_block,
        end_block=args.end_block,
    )

    format_pipeline_result(result)
    return result.get("success", False)


async def run_all_chains(args) -> bool:
    """Run pipeline for all chains."""
    logger.info("🌍 Starting V4 pool pipeline for all chains")

    pipeline = V4PoolPipeline()
    results = await pipeline.run_all_chains(from_deployment=args.from_deployment)

    format_all_chains_result(results)
    return results.get("overall_success", False)


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Uniswap V4 pool fetching and processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental update for Ethereum
  uv run python -m src.processors.pipeline.cli --chain ethereum

  # Full historical fetch for Ethereum
  uv run python -m src.processors.pipeline.cli --chain ethereum --from-deployment

  # Custom block range for Ethereum
  uv run python -m src.processors.pipeline.cli --chain ethereum --start-block 21690000 --end-block 21700000

  # Process all supported chains
  uv run python -m src.processors.pipeline.cli --all-chains

  # Full historical for all chains
  uv run python -m src.processors.pipeline.cli --all-chains --from-deployment
        """,
    )

    # Chain selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--chain",
        choices=["ethereum", "base", "arbitrum"],
        help="Process specific chain",
    )
    group.add_argument(
        "--all-chains", action="store_true", help="Process all supported chains"
    )

    # Processing options
    parser.add_argument(
        "--from-deployment",
        action="store_true",
        help="Fetch from deployment block (full historical data)",
    )

    # Block range override (only for single chain)
    parser.add_argument(
        "--start-block", type=int, help="Override start block (single chain only)"
    )

    parser.add_argument(
        "--end-block", type=int, help="Override end block (single chain only)"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.all_chains and (args.start_block or args.end_block):
        parser.error("--start-block and --end-block can only be used with --chain")

    try:
        # Run pipeline
        if args.all_chains:
            success = await run_all_chains(args)
        else:
            success = await run_single_chain(args)

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("⏹️  Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
