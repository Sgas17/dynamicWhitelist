"""
Unified Uniswap pool fetching and processing pipeline.

This module orchestrates pool pipelines for all Uniswap versions:
1. Uniswap V2 - PairCreated events
2. Uniswap V3 - PoolCreated events
3. Uniswap V4 - Initialize events

Supports incremental updates and database storage for all versions.
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.config.manager import ConfigManager
from src.fetchers.cryo_fetcher import CryoFetcher
from src.processors.pools.pool_processors import (
    UniswapV2PoolProcessor,
    UniswapV3PoolProcessor,
    UniswapV4PoolProcessor,
)


logger = logging.getLogger(__name__)


class UniswapPoolPipeline:
    """
    Unified pipeline for all Uniswap versions.

    Handles event fetching and processing for V2, V3, and V4 pools
    with proper configuration management and incremental processing.
    """

    def __init__(self):
        """Initialize the unified Uniswap pool pipeline."""
        self.config = ConfigManager()
        logger.info("UniswapPoolPipeline initialized")

        # Protocol configurations
        self.protocols = {
            "v2": {
                "processor_class": UniswapV2PoolProcessor,
                "event_hash": self.config.protocols.UNISWAP_V2_PAIR_CREATED_EVENT,
                "event_dir": "uniswap_v2_paircreated_events",
                "address_key": "factory_addresses",
            },
            "v3": {
                "processor_class": UniswapV3PoolProcessor,
                "event_hash": self.config.protocols.UNISWAP_V3_POOL_CREATED_EVENT,
                "event_dir": "uniswap_v3_poolcreated_events",
                "address_key": "factory_addresses",
            },
            "v4": {
                "processor_class": UniswapV4PoolProcessor,
                "event_hash": self.config.protocols.UNISWAP_V4_INITIALIZED_EVENT,
                "event_dir": "uniswap_v4_initialize_events",
                "address_key": "pool_manager",
            },
        }

    async def fetch_events(
        self,
        chain: str,
        version: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> bool:
        """
        Fetch events for specific Uniswap version.

        Args:
            chain: Chain name (ethereum, base, arbitrum)
            version: Uniswap version (v2, v3, v4)
            start_block: Starting block (uses deployment if None)
            end_block: Ending block (uses latest finalized if None)

        Returns:
            Success status
        """
        logger.info(f"Fetching Uniswap {version.upper()} events for {chain}")

        if version not in self.protocols:
            raise ValueError(f"Unsupported version: {version}")

        try:
            # Get configurations
            chain_config = self.config.chains.get_chain_config(chain)
            protocol_config = self.config.protocols.get_protocol_config(
                f"uniswap_{version}", chain
            )

            rpc_url = chain_config["rpc_url"]
            deployment_block = protocol_config.get("deployment_block", 0)

            # Get contract addresses
            address_key = self.protocols[version]["address_key"]
            addresses = protocol_config.get(address_key, [])
            if isinstance(addresses, str):
                addresses = [addresses]

            if not addresses:
                logger.warning(f"No {address_key} configured for {version} on {chain}")
                return False

            # Set start block
            if start_block is None:
                start_block = deployment_block
                logger.info(f"Using deployment block as start: {start_block}")

            logger.info(f"Chain: {chain}, RPC: {rpc_url}, Addresses: {len(addresses)}")

            # Initialize fetcher
            fetcher = CryoFetcher(chain, rpc_url)

            # Get latest finalized block if needed
            if end_block is None:
                end_block = self._get_latest_finalized_block(rpc_url, start_block)

            logger.info(f"Fetching blocks {start_block} → {end_block}")

            # Setup output directory
            data_dir = Path(self.config.base.DATA_DIR)
            events_dir = data_dir / chain / self.protocols[version]["event_dir"]
            events_dir.mkdir(parents=True, exist_ok=True)

            # Get event hash
            event_hash = self.protocols[version]["event_hash"]

            # Fetch events
            result = await fetcher.fetch_logs(
                start_block=start_block,
                end_block=end_block,
                contracts=addresses,
                events=[event_hash],
                output_dir=str(events_dir),
            )

            if result.success:
                parquet_files = list(events_dir.glob("*.parquet"))
                logger.info(
                    f"Successfully fetched {version.upper()} events to {len(parquet_files)} parquet files"
                )
                return True
            else:
                logger.error(
                    f"Failed to fetch {version.upper()} events: {result.error}"
                )
                return False

        except Exception as e:
            logger.exception(
                f"Error fetching {version.upper()} events for {chain}: {e}"
            )
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
        self, chain: str, version: str, start_block: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process events for specific Uniswap version.

        Args:
            chain: Chain name
            version: Uniswap version (v2, v3, v4)
            start_block: Starting block (processor determines if None)

        Returns:
            Processing results dictionary
        """
        logger.info(f"Processing Uniswap {version.upper()} events for {chain}")

        if version not in self.protocols:
            return {"success": False, "error": f"Unsupported version: {version}"}

        try:
            processor_class = self.protocols[version]["processor_class"]
            processor = processor_class(chain)
            result = await processor.process(start_block=start_block or 0)

            if result.success:
                logger.info(
                    f"Successfully processed {result.processed_count} {version.upper()} pools"
                )
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
                logger.error(f"{version.upper()} processing failed: {result.error}")
                return {"success": False, "error": result.error}

        except Exception as e:
            logger.exception(
                f"Error processing {version.upper()} events for {chain}: {e}"
            )
            return {"success": False, "error": str(e)}

    async def run_version_pipeline(
        self,
        chain: str,
        version: str,
        from_deployment: bool = False,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Run pipeline for specific Uniswap version.

        Args:
            chain: Chain name
            version: Uniswap version (v2, v3, v4)
            from_deployment: Fetch from deployment block if True
            start_block: Override start block
            end_block: Override end block

        Returns:
            Pipeline results dictionary
        """
        logger.info(f"Starting Uniswap {version.upper()} pipeline for {chain}")

        pipeline_result = {
            "chain": chain,
            "version": version,
            "success": False,
            "fetch_success": False,
            "process_success": False,
            "error": None,
        }

        try:
            # Determine start block
            if start_block is None and from_deployment:
                protocol_config = self.config.protocols.get_protocol_config(
                    f"uniswap_{version}", chain
                )
                start_block = protocol_config.get("deployment_block", 0)
                logger.info(f"Full historical fetch from deployment: {start_block}")
            elif start_block is None:
                logger.info("Incremental fetch (processor determines start block)")
            else:
                logger.info(f"Custom start block: {start_block}")

            # Step 1: Fetch events
            fetch_success = await self.fetch_events(
                chain, version, start_block, end_block
            )
            pipeline_result["fetch_success"] = fetch_success

            if not fetch_success:
                pipeline_result["error"] = f"{version.upper()} event fetching failed"
                return pipeline_result

            # Step 2: Process events
            process_result = await self.process_events(chain, version, start_block)
            pipeline_result["process_success"] = process_result["success"]
            pipeline_result["processed_count"] = process_result.get(
                "processed_count", 0
            )
            pipeline_result["metadata"] = process_result.get("metadata", {})

            if not process_result["success"]:
                pipeline_result["error"] = process_result.get(
                    "error", f"{version.upper()} processing failed"
                )
                return pipeline_result

            # Success
            pipeline_result["success"] = True
            logger.info(
                f"Uniswap {version.upper()} pipeline completed successfully for {chain}"
            )

            return pipeline_result

        except Exception as e:
            error_msg = f"{version.upper()} pipeline failed for {chain}: {str(e)}"
            logger.exception(error_msg)
            pipeline_result["error"] = error_msg
            return pipeline_result

    async def run_all_versions(
        self,
        chain: str,
        from_deployment: bool = False,
        versions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run pipeline for all Uniswap versions on a chain.

        Args:
            chain: Chain name
            from_deployment: Fetch from deployment if True
            versions: Specific versions to process (defaults to all)

        Returns:
            Results for all versions
        """
        if versions is None:
            versions = ["v2", "v3", "v4"]

        logger.info(f"Running Uniswap pipeline for {chain} - versions: {versions}")

        results = {"chain": chain, "overall_success": True, "versions": {}}

        for version in versions:
            try:
                version_result = await self.run_version_pipeline(
                    chain, version, from_deployment
                )
                results["versions"][version] = version_result

                if not version_result["success"]:
                    results["overall_success"] = False
                    logger.error(f"Pipeline failed for {version.upper()}")
                else:
                    logger.info(f"Pipeline succeeded for {version.upper()}")

            except Exception as e:
                logger.exception(f"Pipeline error for {version.upper()}: {e}")
                results["versions"][version] = {"success": False, "error": str(e)}
                results["overall_success"] = False

        success_count = sum(1 for r in results["versions"].values() if r.get("success"))
        total_count = len(versions)
        logger.info(
            f"Pipeline completed for {chain}: {success_count}/{total_count} versions successful"
        )

        return results

    async def run_all_chains_all_versions(
        self,
        from_deployment: bool = False,
        chains: Optional[List[str]] = None,
        versions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run pipeline for all versions on all chains.

        Args:
            from_deployment: Fetch from deployment if True
            chains: Specific chains to process (defaults to all)
            versions: Specific versions to process (defaults to all)

        Returns:
            Results for all chains and versions
        """
        if chains is None:
            chains = ["ethereum", "base", "arbitrum"]
        if versions is None:
            versions = ["v2", "v3", "v4"]

        logger.info("Running Uniswap pipeline for all chains and versions")

        results = {"overall_success": True, "chains": {}}

        for chain in chains:
            try:
                chain_result = await self.run_all_versions(
                    chain, from_deployment, versions
                )
                results["chains"][chain] = chain_result

                if not chain_result["overall_success"]:
                    results["overall_success"] = False

            except Exception as e:
                logger.exception(f"Chain pipeline error for {chain}: {e}")
                results["chains"][chain] = {"overall_success": False, "error": str(e)}
                results["overall_success"] = False

        # Summary statistics
        total_chains = len(chains)
        success_chains = sum(
            1 for r in results["chains"].values() if r.get("overall_success")
        )

        total_pools = 0
        for chain_result in results["chains"].values():
            if "versions" in chain_result:
                for version_result in chain_result["versions"].values():
                    total_pools += version_result.get("processed_count", 0)

        logger.info(
            f"Global pipeline completed: {success_chains}/{total_chains} chains successful"
        )
        logger.info(f"Total pools processed across all versions: {total_pools}")

        return results


# Convenience functions for standalone usage
async def fetch_uniswap_pools(
    chain: str,
    version: str,
    from_deployment: bool = False,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Convenience function to run Uniswap pipeline for single version.

    Args:
        chain: Chain name
        version: Uniswap version (v2, v3, v4)
        from_deployment: Fetch from deployment if True
        start_block: Override start block
        end_block: Override end block

    Returns:
        Pipeline results
    """
    pipeline = UniswapPoolPipeline()
    return await pipeline.run_version_pipeline(
        chain, version, from_deployment, start_block, end_block
    )


async def fetch_all_uniswap_pools(
    chain: str, from_deployment: bool = False
) -> Dict[str, Any]:
    """
    Convenience function to run pipeline for all Uniswap versions on a chain.

    Args:
        chain: Chain name
        from_deployment: Fetch from deployment if True

    Returns:
        Results for all versions
    """
    pipeline = UniswapPoolPipeline()
    return await pipeline.run_all_versions(chain, from_deployment)
