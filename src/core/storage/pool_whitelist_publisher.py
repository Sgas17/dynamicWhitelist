"""
Pool Whitelist NATS Publisher for ExEx and poolStateArena.

Publishes pool whitelist updates to two separate topics:
1. Minimal topic (whitelist.pools.{chain}.minimal) - For ExEx (addresses only)
2. Full topic (whitelist.pools.{chain}.full) - For poolStateArena (with metadata)

Integration with WhitelistPublisher:
    Import and use in src/core/storage/whitelist_publisher.py _publish_to_nats() method.
"""

import logging
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
import json

import nats

logger = logging.getLogger(__name__)


class PoolWhitelistNatsPublisher:
    """
    Publisher for pool whitelist updates to NATS.

    Publishes to two topics optimized for different consumers:
    - Minimal: Just pool addresses (for ExEx event filtering)
    - Full: Complete metadata (for poolStateArena price calculations)
    """

    def __init__(self, nats_url: str = "nats://localhost:4222"):
        """
        Initialize the pool whitelist publisher.

        Args:
            nats_url: NATS server URL (default: localhost:4222)
        """
        self.nats_url = nats_url
        self.nc: Optional[nats.Client] = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def connect(self):
        """Connect to NATS server."""
        try:
            self.nc = await nats.connect(self.nats_url)
            logger.info(f"✅ Connected to NATS at {self.nats_url}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to NATS: {e}")
            raise

    async def close(self):
        """Close NATS connection."""
        if self.nc:
            await self.nc.close()
            logger.info("Disconnected from NATS")

    async def publish_pool_whitelist(
        self,
        chain: str,
        pools: List[Dict[str, Any]],
        publish_minimal: bool = True,
        publish_full: bool = True
    ) -> Dict[str, bool]:
        """
        Publish pool whitelist to NATS topics.

        Args:
            chain: Chain identifier (ethereum, base, etc.)
            pools: List of pool dicts with structure:
                {
                    'address': str,              # Pool address (required)
                    'token0': {                  # Required for full topic
                        'address': str,
                        'decimals': int,
                        'symbol': str,
                        'name': str (optional)
                    },
                    'token1': {...},             # Same as token0
                    'protocol': str,             # "UniswapV2", "UniswapV3", "UniswapV4"
                    'factory': str,
                    'fee': int (optional),       # For V3/V4
                    'tick_spacing': int (optional),  # For V3/V4
                    'stable': bool (optional)    # For V2 stable pools
                }
            publish_minimal: Whether to publish to minimal topic
            publish_full: Whether to publish to full topic

        Returns:
            Dict mapping topic type to success status
        """
        if not self.nc:
            logger.error("❌ Not connected to NATS")
            return {"minimal": False, "full": False}

        if not pools:
            logger.warning(f"⚠️  No pools to publish for {chain}")
            return {"minimal": False, "full": False}

        results = {}
        timestamp = datetime.now(UTC).isoformat()

        # Publish minimal message (for ExEx)
        if publish_minimal:
            try:
                minimal_msg = {
                    "pools": [pool["address"] for pool in pools],
                    "chain": chain,
                    "timestamp": timestamp
                }
                minimal_subject = f"whitelist.pools.{chain}.minimal"

                payload = json.dumps(minimal_msg).encode()

                await self.nc.publish(minimal_subject, payload)

                results["minimal"] = True
                logger.info(
                    f"📤 Published {len(pools)} pools to {minimal_subject} "
                    f"({len(payload)} bytes)"
                )
            except Exception as e:
                logger.error(f"❌ Failed to publish minimal message: {e}")
                results["minimal"] = False

        # Publish full message (for poolStateArena)
        if publish_full:
            try:
                full_msg = {
                    "pools": pools,
                    "chain": chain,
                    "timestamp": timestamp
                }
                full_subject = f"whitelist.pools.{chain}.full"

                payload = json.dumps(full_msg).encode()

                await self.nc.publish(full_subject, payload)

                results["full"] = True
                logger.info(
                    f"📤 Published {len(pools)} pools to {full_subject} "
                    f"({len(payload)} bytes)"
                )
            except Exception as e:
                logger.error(f"❌ Failed to publish full message: {e}")
                results["full"] = False

        return results

    async def publish_snapshot_reference_block(
        self,
        chain: str,
        reference_block: int,
        snapshot_timestamp: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish snapshot reference block for ExEx synchronization.

        This publishes a reference block number captured BEFORE pool scraping begins.
        The ExEx service uses this to know from which block to start applying updates.

        Key Architecture:
        - Reference block is captured BEFORE scraping starts
        - Scraping may take multiple blocks to complete
        - Individual pools may have data from blocks >= reference_block
        - ExEx should apply updates from (reference_block + 1) onwards
        - This ensures no gaps in state reconstruction

        Args:
            chain: Chain identifier (ethereum, base, etc.)
            reference_block: Block number captured before scraping started
            snapshot_timestamp: ISO timestamp when reference block was captured
            metadata: Additional metadata (e.g., {"pool_count": 1000, "protocols": ["v2", "v3", "v4"]})

        Returns:
            True if publishing succeeded, False otherwise

        Example:
            >>> publisher = PoolWhitelistNatsPublisher()
            >>> await publisher.connect()
            >>> await publisher.publish_snapshot_reference_block(
            ...     chain="ethereum",
            ...     reference_block=12345678,
            ...     metadata={"pool_count": 1000, "duration_seconds": 45.2}
            ... )
        """
        if not self.nc:
            logger.error("❌ Not connected to NATS")
            return False

        subject = f"whitelist.snapshots.{chain}.reference_block"

        message = {
            "chain": chain,
            "reference_block": reference_block,
            "snapshot_timestamp": snapshot_timestamp or datetime.now(UTC).isoformat(),
            "metadata": metadata or {}
        }

        try:
            payload = json.dumps(message).encode()
            await self.nc.publish(subject, payload)

            logger.info(
                f"📤 Published snapshot reference block to {subject}: "
                f"block={reference_block}"
            )
            return True
        except Exception as e:
            logger.error(f"❌ Failed to publish reference block to {subject}: {e}")
            return False


# Standalone function for easy integration
async def publish_pool_whitelist(
    chain: str,
    pools: List[Dict[str, Any]],
    nats_url: str = "nats://localhost:4222"
) -> Dict[str, bool]:
    """
    Standalone function to publish pool whitelist.

    Args:
        chain: Chain identifier (ethereum, base, etc.)
        pools: List of pool info dicts
        nats_url: NATS server URL

    Returns:
        Dict mapping topic type to success status

    Example:
        >>> pools = [
        ...     {
        ...         'address': '0x88e6...',
        ...         'token0': {'address': '0xA0b8...', 'decimals': 6, 'symbol': 'USDC'},
        ...         'token1': {'address': '0xC02a...', 'decimals': 18, 'symbol': 'WETH'},
        ...         'protocol': 'UniswapV3',
        ...         'factory': '0x1F98...',
        ...         'fee': 500,
        ...         'tick_spacing': 10
        ...     }
        ... ]
        >>> results = await publish_pool_whitelist('ethereum', pools)
        >>> print(results)  # {'minimal': True, 'full': True}
    """
    async with PoolWhitelistNatsPublisher(nats_url) as publisher:
        return await publisher.publish_pool_whitelist(chain, pools)


async def publish_snapshot_reference_block(
    chain: str,
    reference_block: int,
    snapshot_timestamp: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    nats_url: str = "nats://localhost:4222"
) -> bool:
    """
    Standalone function to publish snapshot reference block.

    Args:
        chain: Chain identifier
        reference_block: Block number captured before scraping
        snapshot_timestamp: ISO timestamp when reference block was captured
        metadata: Additional metadata about the snapshot
        nats_url: NATS server URL

    Returns:
        True if publishing succeeded

    Example:
        >>> await publish_snapshot_reference_block(
        ...     chain="ethereum",
        ...     reference_block=12345678,
        ...     metadata={"pool_count": 1000}
        ... )
    """
    async with PoolWhitelistNatsPublisher(nats_url) as publisher:
        return await publisher.publish_snapshot_reference_block(
            chain=chain,
            reference_block=reference_block,
            snapshot_timestamp=snapshot_timestamp,
            metadata=metadata
        )
