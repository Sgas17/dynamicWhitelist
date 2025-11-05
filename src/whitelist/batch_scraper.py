"""
Batch Scraper with Block-Synchronized Batching.

Scrapes pools in time-bounded batches synchronized to block times, ensuring each
batch has an accurate reference block for ExEx synchronization.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Dict, List, Optional, Tuple

from web3 import Web3

logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch scraping."""

    # Ethereum block time in seconds
    block_time_seconds: float = 12.0

    # Safety margin (0.8 = use 80% of block time, leaving 20% buffer)
    safety_margin: float = 0.8

    # Protocol-specific batch sizes (configurable, can be tuned via profiling)
    v2_batch_size: int = 200  # V2 is fastest (1 storage slot)
    v3_slot0_batch_size: int = 150  # V3 slot0 only (moderate)
    v4_slot0_batch_size: int = 100  # V4 slot0 only (moderate)
    v3_full_ticks_batch_size: int = 30  # V3 with full ticks (slower)
    v4_full_ticks_batch_size: int = 20  # V4 with full ticks (slowest)

    # Whether to wait for next block between batches
    wait_for_next_block: bool = True


@dataclass
class BatchResult:
    """Result from scraping a single batch."""

    batch_number: int
    protocol: str
    reference_block: int
    reference_timestamp: str
    pools_scraped: int
    duration_seconds: float
    success: bool
    error: Optional[str] = None


class BatchScraper:
    """
    Manages batch scraping with block synchronization.

    Key features:
    - Groups pools by protocol
    - Creates time-bounded batches
    - Captures reference block per batch
    - Waits for next block between batches
    - Publishes per-batch reference blocks to NATS
    """

    def __init__(self, web3: Web3, reth_loader, config: Optional[BatchConfig] = None):
        """
        Initialize batch scraper.

        Args:
            web3: Web3 instance for block tracking
            reth_loader: RethSnapshotLoader instance
            config: Batch configuration (uses defaults if not provided)
        """
        self.web3 = web3
        self.reth_loader = reth_loader
        self.config = config or BatchConfig()

        self.current_block = None
        self.batch_results: List[BatchResult] = []

    async def wait_for_next_block(self) -> int:
        """
        Wait for the next Ethereum block.

        Returns:
            New block number
        """
        if not self.current_block:
            self.current_block = self.web3.eth.block_number

        target_block = self.current_block + 1
        logger.info(f"⏳ Waiting for block {target_block}...")

        while True:
            current = self.web3.eth.block_number
            if current >= target_block:
                self.current_block = current
                logger.info(f"✓ Block {current} arrived")
                return current

            # Check every second
            await asyncio.sleep(1.0)

    def create_batches(
        self, pools: Dict[str, Dict], batch_type: str = "filtering"
    ) -> List[Tuple[str, List[Dict]]]:
        """
        Create protocol-specific batches.

        Args:
            pools: Dict of pool_address -> pool_data
            batch_type: "filtering" (slot0 only) or "full_ticks"

        Returns:
            List of (protocol, pool_list) tuples for each batch
        """
        # Group pools by protocol
        v2_pools = []
        v3_pools = []
        v4_pools = []

        for pool_addr, pool_data in pools.items():
            protocol = pool_data.get("protocol", "").lower()

            if protocol == "v2" or protocol == "uniswap_v2":
                v2_pools.append(pool_data)
            elif protocol == "v3" or protocol == "uniswap_v3":
                v3_pools.append(pool_data)
            elif protocol == "v4" or protocol == "uniswap_v4":
                v4_pools.append(pool_data)

        # Determine batch sizes based on type
        if batch_type == "filtering":
            v2_size = self.config.v2_batch_size
            v3_size = self.config.v3_slot0_batch_size
            v4_size = self.config.v4_slot0_batch_size
        else:  # full_ticks
            v2_size = self.config.v2_batch_size  # V2 doesn't have ticks
            v3_size = self.config.v3_full_ticks_batch_size
            v4_size = self.config.v4_full_ticks_batch_size

        # Create batches
        batches = []

        # V2 batches
        for i in range(0, len(v2_pools), v2_size):
            batch = v2_pools[i : i + v2_size]
            batches.append(("v2", batch))

        # V3 batches
        for i in range(0, len(v3_pools), v3_size):
            batch = v3_pools[i : i + v3_size]
            batches.append(("v3", batch))

        # V4 batches
        for i in range(0, len(v4_pools), v4_size):
            batch = v4_pools[i : i + v4_size]
            batches.append(("v4", batch))

        logger.info(
            f"Created {len(batches)} batches: "
            f"{len(v2_pools)} V2, {len(v3_pools)} V3, {len(v4_pools)} V4 pools"
        )

        return batches

    async def scrape_v2_batch(
        self,
        batch_number: int,
        pools: List[Dict],
        reference_block: int,
        reference_timestamp: str,
    ) -> BatchResult:
        """
        Scrape a batch of V2 pools.

        Args:
            batch_number: Batch index
            pools: List of V2 pool configs
            reference_block: Reference block for this batch
            reference_timestamp: ISO timestamp of reference block

        Returns:
            BatchResult with scraping metrics
        """
        import time

        start_time = time.time()

        logger.info(
            f"📦 Scraping V2 batch {batch_number}: {len(pools)} pools at block {reference_block}"
        )

        scraped_pools = []

        for pool_data in pools:
            pool_addr = pool_data.get("address") or pool_data.get("pool_address")

            try:
                reserves, block_number = self.reth_loader.load_v2_pool_snapshot(
                    pool_addr
                )

                scraped_pools.append(
                    {
                        "address": pool_addr,
                        "reserves": reserves,
                        "block_number": block_number,
                        "reference_block": reference_block,
                        "protocol": "v2",
                    }
                )

            except Exception as e:
                logger.warning(f"Failed to scrape V2 pool {pool_addr}: {e}")
                continue

        end_time = time.time()
        duration = end_time - start_time

        return BatchResult(
            batch_number=batch_number,
            protocol="v2",
            reference_block=reference_block,
            reference_timestamp=reference_timestamp,
            pools_scraped=len(scraped_pools),
            duration_seconds=duration,
            success=True,
        )

    async def scrape_v3_batch_slot0(
        self,
        batch_number: int,
        pools: List[Dict],
        reference_block: int,
        reference_timestamp: str,
    ) -> Tuple[BatchResult, List[Dict]]:
        """
        Scrape a batch of V3 pools (slot0 only for filtering).

        Args:
            batch_number: Batch index
            pools: List of V3 pool configs
            reference_block: Reference block for this batch
            reference_timestamp: ISO timestamp of reference block

        Returns:
            Tuple of (BatchResult, scraped_pool_states)
        """
        import time

        start_time = time.time()

        logger.info(
            f"📦 Scraping V3 batch {batch_number}: {len(pools)} pools (slot0) at block {reference_block}"
        )

        # Prepare configs for batch loading
        pool_configs = [
            {
                "address": pool.get("address") or pool.get("pool_address"),
                "tick_spacing": pool.get("tick_spacing"),
            }
            for pool in pools
        ]

        try:
            states = self.reth_loader.batch_load_v3_states(pool_configs)

            # Add reference block to each state
            for state in states:
                state["reference_block"] = reference_block
                state["protocol"] = "v3"

            end_time = time.time()
            duration = end_time - start_time

            return BatchResult(
                batch_number=batch_number,
                protocol="v3_slot0",
                reference_block=reference_block,
                reference_timestamp=reference_timestamp,
                pools_scraped=len(states),
                duration_seconds=duration,
                success=True,
            ), states

        except Exception as e:
            logger.error(f"Failed to scrape V3 batch {batch_number}: {e}")
            end_time = time.time()
            return BatchResult(
                batch_number=batch_number,
                protocol="v3_slot0",
                reference_block=reference_block,
                reference_timestamp=reference_timestamp,
                pools_scraped=0,
                duration_seconds=end_time - start_time,
                success=False,
                error=str(e),
            ), []

    async def scrape_v4_batch_slot0(
        self,
        batch_number: int,
        pools: List[Dict],
        reference_block: int,
        reference_timestamp: str,
    ) -> Tuple[BatchResult, List[Dict]]:
        """
        Scrape a batch of V4 pools (slot0 only for filtering).

        Args:
            batch_number: Batch index
            pools: List of V4 pool configs
            reference_block: Reference block for this batch
            reference_timestamp: ISO timestamp of reference block

        Returns:
            Tuple of (BatchResult, scraped_pool_states)
        """
        import time

        start_time = time.time()

        logger.info(
            f"📦 Scraping V4 batch {batch_number}: {len(pools)} pools (slot0) at block {reference_block}"
        )

        # Prepare configs for batch loading
        pool_configs = [
            {
                "pool_id": pool.get("pool_id") or pool.get("address"),
                "tick_spacing": pool.get("tick_spacing"),
                "pool_manager": pool.get("factory") or pool.get("pool_manager"),
            }
            for pool in pools
        ]

        try:
            states = self.reth_loader.batch_load_v4_states(pool_configs)

            # Add reference block to each state
            for state in states:
                state["reference_block"] = reference_block
                state["protocol"] = "v4"

            end_time = time.time()
            duration = end_time - start_time

            return BatchResult(
                batch_number=batch_number,
                protocol="v4_slot0",
                reference_block=reference_block,
                reference_timestamp=reference_timestamp,
                pools_scraped=len(states),
                duration_seconds=duration,
                success=True,
            ), states

        except Exception as e:
            logger.error(f"Failed to scrape V4 batch {batch_number}: {e}")
            end_time = time.time()
            return BatchResult(
                batch_number=batch_number,
                protocol="v4_slot0",
                reference_block=reference_block,
                reference_timestamp=reference_timestamp,
                pools_scraped=0,
                duration_seconds=end_time - start_time,
                success=False,
                error=str(e),
            ), []

    async def scrape_all_batches(
        self, pools: Dict[str, Dict], batch_type: str = "filtering", nats_publisher=None
    ) -> Dict[str, any]:
        """
        Scrape all pools in protocol-specific, time-bounded batches.

        This is the main entry point for batch scraping.

        Args:
            pools: Dict of pool_address -> pool_data
            batch_type: "filtering" (slot0 only) or "full_ticks"
            nats_publisher: Optional PoolWhitelistNatsPublisher for publishing reference blocks

        Returns:
            Dict with scraping results and metrics
        """
        logger.info("=" * 80)
        logger.info(f"BATCH SCRAPING ({batch_type.upper()})")
        logger.info("=" * 80)

        # Create batches
        batches = self.create_batches(pools, batch_type)

        if not batches:
            logger.warning("No batches to scrape")
            return {"batches": [], "total_pools": 0, "success": True}

        all_scraped_data = []
        batch_results = []

        # Process each batch
        for batch_idx, (protocol, pool_batch) in enumerate(batches):
            # Wait for next block if configured (except for first batch)
            if self.config.wait_for_next_block and batch_idx > 0:
                await self.wait_for_next_block()

            # Capture reference block for this batch
            reference_block = self.web3.eth.block_number
            reference_timestamp = datetime.now(UTC).isoformat()

            logger.info(
                f"📍 Batch {batch_idx + 1}/{len(batches)}: "
                f"{protocol.upper()} - Block {reference_block}"
            )

            # Scrape based on protocol
            if protocol == "v2":
                result = await self.scrape_v2_batch(
                    batch_number=batch_idx + 1,
                    pools=pool_batch,
                    reference_block=reference_block,
                    reference_timestamp=reference_timestamp,
                )
                batch_results.append(result)

            elif protocol == "v3":
                result, scraped_states = await self.scrape_v3_batch_slot0(
                    batch_number=batch_idx + 1,
                    pools=pool_batch,
                    reference_block=reference_block,
                    reference_timestamp=reference_timestamp,
                )
                batch_results.append(result)
                all_scraped_data.extend(scraped_states)

            elif protocol == "v4":
                result, scraped_states = await self.scrape_v4_batch_slot0(
                    batch_number=batch_idx + 1,
                    pools=pool_batch,
                    reference_block=reference_block,
                    reference_timestamp=reference_timestamp,
                )
                batch_results.append(result)
                all_scraped_data.extend(scraped_states)

            # Publish reference block to NATS for ExEx synchronization
            if nats_publisher and result.success:
                try:
                    await nats_publisher.publish_snapshot_reference_block(
                        chain="ethereum",  # TODO: Make configurable
                        reference_block=reference_block,
                        snapshot_timestamp=reference_timestamp,
                        metadata={
                            "batch_number": batch_idx + 1,
                            "total_batches": len(batches),
                            "protocol": protocol,
                            "pools_in_batch": result.pools_scraped,
                        },
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to publish reference block for batch {batch_idx + 1}: {e}"
                    )

            logger.info(
                f"✓ Batch {batch_idx + 1} complete: "
                f"{result.pools_scraped} pools in {result.duration_seconds:.2f}s"
            )

        # Summary
        total_pools = sum(r.pools_scraped for r in batch_results)
        total_duration = sum(r.duration_seconds for r in batch_results)
        successful_batches = sum(1 for r in batch_results if r.success)

        logger.info("=" * 80)
        logger.info(f"BATCH SCRAPING COMPLETE")
        logger.info(f"  Total batches: {len(batch_results)}")
        logger.info(f"  Successful: {successful_batches}/{len(batch_results)}")
        logger.info(f"  Total pools: {total_pools}")
        logger.info(f"  Total duration: {total_duration:.2f}s")
        logger.info(
            f"  Avg time per pool: {total_duration / total_pools if total_pools > 0 else 0:.3f}s"
        )
        logger.info("=" * 80)

        return {
            "batches": [
                {
                    "batch_number": r.batch_number,
                    "protocol": r.protocol,
                    "reference_block": r.reference_block,
                    "reference_timestamp": r.reference_timestamp,
                    "pools_scraped": r.pools_scraped,
                    "duration_seconds": r.duration_seconds,
                    "success": r.success,
                    "error": r.error,
                }
                for r in batch_results
            ],
            "total_pools": total_pools,
            "total_duration_seconds": total_duration,
            "success": successful_batches == len(batch_results),
            "scraped_data": all_scraped_data,
        }
