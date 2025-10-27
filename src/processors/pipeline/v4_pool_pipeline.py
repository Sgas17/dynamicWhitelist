"""
Uniswap V4 pool fetching and processing pipeline.

This module orchestrates the complete V4 pool pipeline:
1. Fetches V4 Initialize events using CryoFetcher
2. Processes events into pools using UniswapV4PoolProcessor
3. Supports incremental updates and database storage
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

from src.config.manager import ConfigManager
from src.fetchers.cryo_fetcher import CryoFetcher
from src.processors.pools.pool_processors import UniswapV4PoolProcessor


logger = logging.getLogger(__name__)


class V4PoolPipeline:
    """
    Complete V4 pool fetching and processing pipeline.

    Handles event fetching, processing, and database storage with proper
    configuration management and logging.
    """

    def __init__(self):
        """Initialize the V4 pool pipeline."""
        self.config = ConfigManager()
        logger.info("V4PoolPipeline initialized")

    async def fetch_events(
        self,
        chain: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> bool:
        """
        Fetch V4 Initialize events for a chain.

        Args:
            chain: Chain name (ethereum, base, arbitrum)
            start_block: Starting block (uses deployment if None)
            end_block: Ending block (uses latest finalized if None)

        Returns:
            Success status
        """
        logger.info(f"Fetching V4 Initialize events for {chain}")

        try:
            # Get configurations
            chain_config = self.config.chains.get_chain_config(chain)
            v4_config = self.config.protocols.get_protocol_config("uniswap_v4", chain)

            rpc_url = chain_config["rpc_url"]
            deployment_block = v4_config.get("deployment_block", 0)
            pool_manager = v4_config.get("pool_manager", "")

            # Set start block
            if start_block is None:
                start_block = deployment_block
                logger.info(f"Using deployment block as start: {start_block}")

            logger.info(f"Chain: {chain}, RPC: {rpc_url}, Pool manager: {pool_manager}")

            # Initialize fetcher
            fetcher = CryoFetcher(chain, rpc_url)

            # Get latest finalized block if needed
            if end_block is None:
                end_block = self._get_latest_finalized_block(rpc_url, start_block)

            logger.info(f"Fetching blocks {start_block} → {end_block}")

            # Setup output directory
            data_dir = Path(self.config.base.DATA_DIR)
            events_dir = data_dir / chain / "uniswap_v4_initialize_events"
            events_dir.mkdir(parents=True, exist_ok=True)

            # V4 Initialize event hash
            initialize_event_hash = (
                "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
            )

            # Fetch events
            result = await fetcher.fetch_logs(
                start_block=start_block,
                end_block=end_block,
                contracts=[pool_manager],
                events=[initialize_event_hash],
                output_dir=str(events_dir),
            )

            if result.success:
                parquet_files = list(events_dir.glob("*.parquet"))
                logger.info(
                    f"Successfully fetched events to {len(parquet_files)} parquet files"
                )
                return True
            else:
                logger.error(f"Failed to fetch events: {result.error}")
                return False

        except Exception as e:
            logger.exception(f"Error fetching V4 events for {chain}: {e}")
            return False

    def _get_latest_finalized_block(self, rpc_url: str, fallback_start: int) -> int:
        """Get latest finalized block using cast command."""
        try:
            result = subprocess.run(
                ["cast", "block", "-f", "number", "-r", rpc_url, "finalized"],
                capture_output=True,
                text=True,
                check=True,
            )
            block_number = int(result.stdout.strip())
            logger.info(f"Latest finalized block: {block_number}")
            return block_number
        except Exception as e:
            logger.warning(f"Could not get latest block: {e}")
            fallback_block = fallback_start + 1000
            logger.info(f"Using fallback end block: {fallback_block}")
            return fallback_block

    async def process_events(
        self, chain: str, start_block: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process V4 Initialize events into pools.

        Args:
            chain: Chain name
            start_block: Starting block (processor determines if None)

        Returns:
            Processing results dictionary
        """
        logger.info(f"Processing V4 events into pools for {chain}")

        try:
            processor = UniswapV4PoolProcessor(chain)
            result = await processor.process(start_block=start_block or 0)

            if result.success:
                logger.info(f"Successfully processed {result.processed_count} pools")
                if result.metadata:
                    start = result.metadata.get("start_block")
                    end = result.metadata.get("end_block")
                    stored = result.metadata.get("stored_pools", 0)
                    logger.info(f"Blocks: {start} → {end}, Stored: {stored}")

                return {
                    "success": True,
                    "processed_count": result.processed_count,
                    "metadata": result.metadata,
                }
            else:
                logger.error(f"Processing failed: {result.error}")
                return {"success": False, "error": result.error}

        except Exception as e:
            logger.exception(f"Error processing V4 events for {chain}: {e}")
            return {"success": False, "error": str(e)}

    async def run_full_pipeline(
        self,
        chain: str = "ethereum",
        from_deployment: bool = False,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Run complete V4 pool pipeline.

        Args:
            chain: Chain name
            from_deployment: Fetch from deployment block if True
            start_block: Override start block
            end_block: Override end block

        Returns:
            Pipeline results dictionary
        """
        logger.info(f"Starting V4 pool pipeline for {chain}")

        pipeline_result = {
            "chain": chain,
            "success": False,
            "fetch_success": False,
            "process_success": False,
            "error": None,
        }

        try:
            # Determine start block
            if start_block is None and from_deployment:
                v4_config = self.config.protocols.get_protocol_config(
                    "uniswap_v4", chain
                )
                start_block = v4_config.get("deployment_block", 0)
                logger.info(f"Full historical fetch from deployment: {start_block}")
            elif start_block is None:
                logger.info("Incremental fetch (processor determines start block)")
            else:
                logger.info(f"Custom start block: {start_block}")

            # Step 1: Fetch events
            fetch_success = await self.fetch_events(chain, start_block, end_block)
            pipeline_result["fetch_success"] = fetch_success

            if not fetch_success:
                pipeline_result["error"] = "Event fetching failed"
                return pipeline_result

            # Step 2: Process events
            process_result = await self.process_events(chain, start_block)
            pipeline_result["process_success"] = process_result["success"]
            pipeline_result["processed_count"] = process_result.get(
                "processed_count", 0
            )
            pipeline_result["metadata"] = process_result.get("metadata", {})

            if not process_result["success"]:
                pipeline_result["error"] = process_result.get(
                    "error", "Processing failed"
                )
                return pipeline_result

            # Success
            pipeline_result["success"] = True
            logger.info(f"V4 pool pipeline completed successfully for {chain}")

            return pipeline_result

        except Exception as e:
            error_msg = f"Pipeline failed for {chain}: {str(e)}"
            logger.exception(error_msg)
            pipeline_result["error"] = error_msg
            return pipeline_result

    async def run_all_chains(self, from_deployment: bool = False) -> Dict[str, Any]:
        """
        Run V4 pool pipeline for all supported chains.

        Args:
            from_deployment: Fetch from deployment if True

        Returns:
            Results for all chains
        """
        logger.info("Running V4 pool pipeline for all supported chains")

        chains = ["ethereum", "base", "arbitrum"]
        results = {"overall_success": True, "chains": {}}

        for chain in chains:
            try:
                chain_result = await self.run_full_pipeline(chain, from_deployment)
                results["chains"][chain] = chain_result

                if not chain_result["success"]:
                    results["overall_success"] = False
                    logger.error(f"Pipeline failed for {chain}")
                else:
                    logger.info(f"Pipeline succeeded for {chain}")

            except Exception as e:
                logger.exception(f"Pipeline error for {chain}: {e}")
                results["chains"][chain] = {"success": False, "error": str(e)}
                results["overall_success"] = False

        success_count = sum(1 for r in results["chains"].values() if r.get("success"))
        total_count = len(chains)
        logger.info(
            f"Pipeline completed: {success_count}/{total_count} chains successful"
        )

        return results


# Convenience functions for standalone usage
async def fetch_v4_pools(
    chain: str,
    from_deployment: bool = False,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Convenience function to run V4 pool pipeline for single chain.

    Args:
        chain: Chain name
        from_deployment: Fetch from deployment if True
        start_block: Override start block
        end_block: Override end block

    Returns:
        Pipeline results
    """
    pipeline = V4PoolPipeline()
    return await pipeline.run_full_pipeline(
        chain, from_deployment, start_block, end_block
    )


async def fetch_v4_pools_all() -> Dict[str, Any]:
    """
    Convenience function to run V4 pool pipeline for all chains.

    Returns:
        Results for all chains
    """
    pipeline = V4PoolPipeline()
    return await pipeline.run_all_chains()
