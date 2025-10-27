#!/usr/bin/env python3
"""
Command-line interface for unified Uniswap pool pipeline.

Usage:
    # Single version
    uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --version v3
    uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --version v4 --from-deployment

    # All versions on a chain
    uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --all-versions

    # All chains and versions
    uv run python -m src.processors.pipeline.uniswap_cli --all-chains --all-versions
"""

import argparse
import asyncio
import logging
import sys
from typing import Dict, Any

from src.processors.pipeline.uniswap_pool_pipeline import UniswapPoolPipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def format_version_result(version: str, result: Dict[str, Any]) -> None:
    """Format and display single version results."""
    version_upper = version.upper()

    if result.get("success"):
        logger.info(f"✅ {version_upper} pipeline completed successfully")

        if "processed_count" in result:
            logger.info(
                f"📊 {version_upper}: {result['processed_count']} pools processed"
            )

        if "metadata" in result and result["metadata"]:
            metadata = result["metadata"]
            start = metadata.get("start_block")
            end = metadata.get("end_block")
            stored = metadata.get("stored_pools", 0)

            if start and end:
                logger.info(f"🔢 {version_upper} blocks: {start} → {end}")
            if stored:
                logger.info(f"💾 {version_upper}: {stored} pools stored to database")
    else:
        logger.error(f"❌ {version_upper} pipeline failed")
        if "error" in result:
            logger.error(f"{version_upper} error: {result['error']}")


def format_chain_results(chain: str, results: Dict[str, Any]) -> None:
    """Format and display results for all versions on a chain."""
    logger.info("=" * 60)
    logger.info(f"📊 CHAIN SUMMARY: {chain.upper()}")
    logger.info("=" * 60)

    version_results = results.get("versions", {})
    success_count = 0
    total_pools = 0

    for version, result in version_results.items():
        version_upper = version.upper()
        if result.get("success"):
            success_count += 1
            pools = result.get("processed_count", 0)
            total_pools += pools
            logger.info(f"✅ {version_upper}: {pools} pools processed")
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"❌ {version_upper}: {error}")

    logger.info("=" * 60)
    logger.info(
        f"🎯 {chain.upper()}: {success_count}/{len(version_results)} versions successful"
    )
    logger.info(f"📊 {chain.upper()}: {total_pools} total pools processed")

    if results.get("overall_success"):
        logger.info(f"🎉 {chain.upper()} COMPLETED SUCCESSFULLY")
    else:
        logger.warning(f"⚠️  {chain.upper()} had some failures")


def format_global_results(results: Dict[str, Any]) -> None:
    """Format and display global results for all chains and versions."""
    logger.info("=" * 80)
    logger.info("🌍 GLOBAL SUMMARY - ALL CHAINS & VERSIONS")
    logger.info("=" * 80)

    chain_results = results.get("chains", {})
    success_chains = 0
    total_pools_global = 0

    for chain, chain_result in chain_results.items():
        if chain_result.get("overall_success"):
            success_chains += 1

        # Count pools for this chain
        version_results = chain_result.get("versions", {})
        chain_pools = sum(
            version_result.get("processed_count", 0)
            for version_result in version_results.values()
        )
        total_pools_global += chain_pools

        logger.info(f"📍 {chain.upper()}: {chain_pools} pools across all versions")

    logger.info("=" * 80)
    logger.info(f"🎯 Results: {success_chains}/{len(chain_results)} chains successful")
    logger.info(f"📊 Grand total: {total_pools_global} pools processed")

    if results.get("overall_success"):
        logger.info("🎉 ALL CHAINS & VERSIONS COMPLETED SUCCESSFULLY")
    else:
        logger.warning("⚠️  Some chains or versions failed")


async def run_single_version(args) -> bool:
    """Run pipeline for single version."""
    logger.info(f"🚀 Starting Uniswap {args.version.upper()} pipeline for {args.chain}")

    pipeline = UniswapPoolPipeline()
    result = await pipeline.run_version_pipeline(
        chain=args.chain,
        version=args.version,
        from_deployment=args.from_deployment,
        start_block=args.start_block,
        end_block=args.end_block,
    )

    format_version_result(args.version, result)
    return result.get("success", False)


async def run_all_versions_single_chain(args) -> bool:
    """Run pipeline for all versions on single chain."""
    logger.info(f"🚀 Starting all Uniswap versions for {args.chain}")

    pipeline = UniswapPoolPipeline()

    # Determine which versions to run
    versions = None
    if hasattr(args, "versions") and args.versions:
        versions = args.versions

    results = await pipeline.run_all_versions(
        chain=args.chain, from_deployment=args.from_deployment, versions=versions
    )

    format_chain_results(args.chain, results)
    return results.get("overall_success", False)


async def run_all_chains_all_versions(args) -> bool:
    """Run pipeline for all versions on all chains."""
    logger.info("🌍 Starting all Uniswap versions for all chains")

    pipeline = UniswapPoolPipeline()

    # Determine which chains and versions to run
    chains = None
    versions = None
    if hasattr(args, "chains") and args.chains:
        chains = args.chains
    if hasattr(args, "versions") and args.versions:
        versions = args.versions

    results = await pipeline.run_all_chains_all_versions(
        from_deployment=args.from_deployment, chains=chains, versions=versions
    )

    format_global_results(results)
    return results.get("overall_success", False)


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Unified Uniswap pool fetching and processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single version on Ethereum
  uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --version v3

  # V4 full historical on Ethereum
  uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --version v4 --from-deployment

  # All versions on Ethereum
  uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --all-versions

  # All versions on all chains
  uv run python -m src.processors.pipeline.uniswap_cli --all-chains --all-versions

  # Specific versions on all chains
  uv run python -m src.processors.pipeline.uniswap_cli --all-chains --versions v3 v4

  # Custom block range for V3 on Ethereum
  uv run python -m src.processors.pipeline.uniswap_cli --chain ethereum --version v3 --start-block 12369621 --end-block 12370000
        """,
    )

    # Chain selection
    chain_group = parser.add_mutually_exclusive_group(required=True)
    chain_group.add_argument(
        "--chain",
        choices=["ethereum", "base", "arbitrum"],
        help="Process specific chain",
    )
    chain_group.add_argument(
        "--all-chains", action="store_true", help="Process all supported chains"
    )

    # Version selection (for single chain mode)
    version_group = parser.add_mutually_exclusive_group()
    version_group.add_argument(
        "--version",
        choices=["v2", "v3", "v4"],
        help="Process specific Uniswap version (requires --chain)",
    )
    version_group.add_argument(
        "--all-versions", action="store_true", help="Process all Uniswap versions"
    )

    # Advanced options
    parser.add_argument(
        "--versions",
        nargs="+",
        choices=["v2", "v3", "v4"],
        help="Specify which versions to process (for --all-chains mode)",
    )

    parser.add_argument(
        "--chains",
        nargs="+",
        choices=["ethereum", "base", "arbitrum"],
        help="Specify which chains to process (for --all-chains mode)",
    )

    # Processing options
    parser.add_argument(
        "--from-deployment",
        action="store_true",
        help="Fetch from deployment block (full historical data)",
    )

    # Block range override (only for single version)
    parser.add_argument(
        "--start-block", type=int, help="Override start block (single version only)"
    )

    parser.add_argument(
        "--end-block", type=int, help="Override end block (single version only)"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.chain and not (args.version or args.all_versions):
        parser.error("--chain requires either --version or --all-versions")

    if args.version and not args.chain:
        parser.error("--version requires --chain")

    if args.all_chains and args.chain:
        parser.error("Cannot use both --all-chains and --chain")

    if (args.start_block or args.end_block) and not (args.chain and args.version):
        parser.error(
            "--start-block and --end-block can only be used with single version (--chain + --version)"
        )

    try:
        # Determine execution mode and run pipeline
        if args.all_chains:
            success = await run_all_chains_all_versions(args)
        elif args.chain and args.version:
            success = await run_single_version(args)
        elif args.chain and args.all_versions:
            success = await run_all_versions_single_chain(args)
        else:
            parser.error("Invalid argument combination")

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
