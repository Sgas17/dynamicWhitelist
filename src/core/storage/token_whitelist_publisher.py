"""
Token Whitelist NATS Publisher for dynamic token whitelist management.

Publishes token whitelist updates to multiple topics:
1. Full topic (whitelist.tokens.{chain}.full) - Complete token whitelist
2. Add delta topic (whitelist.tokens.{chain}.add) - Newly added tokens
3. Remove delta topic (whitelist.tokens.{chain}.remove) - Removed tokens

Integration with WhitelistPublisher:
    Import and use in src/core/storage/whitelist_publisher.py _publish_to_nats() method.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set

import nats

logger = logging.getLogger(__name__)


class TokenWhitelistNatsPublisher:
    """
    Publisher for token whitelist updates to NATS.

    Publishes to multiple topics for different use cases:
    - Full: Complete token whitelist (periodic updates)
    - Add: Newly added tokens (delta updates)
    - Remove: Removed tokens (delta updates)
    """

    def __init__(self, nats_url: str = "nats://localhost:4222"):
        """
        Initialize the token whitelist publisher.

        Args:
            nats_url: NATS server URL (default: localhost:4222)
        """
        self.nats_url = nats_url
        self.nc: Optional[nats.Client] = None
        self._previous_tokens: Dict[str, Set[str]] = {}  # Track tokens per chain

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

    async def publish_token_whitelist(
        self,
        chain: str,
        tokens: Dict[str, Dict[str, Any]],
        publish_full: bool = True,
        publish_deltas: bool = True,
    ) -> Dict[str, bool]:
        """
        Publish token whitelist to NATS topics.

        Args:
            chain: Chain identifier (ethereum, base, etc.)
            tokens: Dict mapping token address to metadata:
                {
                    '0x...': {
                        'symbol': str,           # Token symbol (required)
                        'decimals': int,         # Token decimals (required)
                        'name': str,             # Token name (optional)
                        'filters': List[str]     # Filter types (required)
                    }
                }
            publish_full: Whether to publish complete whitelist
            publish_deltas: Whether to publish add/remove deltas

        Returns:
            Dict mapping topic type to success status
        """
        if not self.nc:
            logger.error("❌ Not connected to NATS")
            return {"full": False, "add": False, "remove": False}

        if not tokens:
            logger.warning(f"⚠️  No tokens to publish for {chain}")
            return {"full": False, "add": False, "remove": False}

        results = {}
        timestamp = datetime.now(UTC).isoformat()

        # Calculate filter counts
        filter_counts = self._calculate_filter_counts(tokens)

        # Publish full whitelist
        if publish_full:
            results["full"] = await self._publish_full_whitelist(
                chain, tokens, timestamp, filter_counts
            )

        # Publish deltas (add/remove)
        if publish_deltas:
            current_addresses = set(tokens.keys())
            previous_addresses = self._previous_tokens.get(chain, set())

            # Added tokens
            added_addresses = current_addresses - previous_addresses
            if added_addresses:
                added_tokens = {addr: tokens[addr] for addr in added_addresses}
                results["add"] = await self._publish_add_delta(
                    chain, added_tokens, timestamp
                )
            else:
                results["add"] = True  # No additions is success

            # Removed tokens
            removed_addresses = previous_addresses - current_addresses
            if removed_addresses:
                results["remove"] = await self._publish_remove_delta(
                    chain, list(removed_addresses), timestamp
                )
            else:
                results["remove"] = True  # No removals is success

            # Update tracking
            self._previous_tokens[chain] = current_addresses

        return results

    async def _publish_full_whitelist(
        self,
        chain: str,
        tokens: Dict[str, Dict[str, Any]],
        timestamp: str,
        filter_counts: Dict[str, int],
    ) -> bool:
        """Publish complete token whitelist."""
        try:
            full_msg = {
                "chain": chain,
                "timestamp": timestamp,
                "tokens": tokens,
                "metadata": {
                    "total_count": len(tokens),
                    "filter_counts": filter_counts,
                },
            }
            full_subject = f"whitelist.tokens.{chain}.full"

            payload = json.dumps(full_msg).encode()
            await self.nc.publish(full_subject, payload)

            logger.info(
                f"📤 Published {len(tokens)} tokens to {full_subject} "
                f"({len(payload)} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish full whitelist: {e}")
            return False

    async def _publish_add_delta(
        self, chain: str, added_tokens: Dict[str, Dict[str, Any]], timestamp: str
    ) -> bool:
        """Publish newly added tokens."""
        try:
            add_msg = {
                "chain": chain,
                "timestamp": timestamp,
                "action": "add",
                "tokens": added_tokens,
            }
            add_subject = f"whitelist.tokens.{chain}.add"

            payload = json.dumps(add_msg).encode()
            await self.nc.publish(add_subject, payload)

            logger.info(
                f"📤 Published {len(added_tokens)} added tokens to {add_subject} "
                f"({len(payload)} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish add delta: {e}")
            return False

    async def _publish_remove_delta(
        self, chain: str, removed_addresses: List[str], timestamp: str
    ) -> bool:
        """Publish removed token addresses."""
        try:
            remove_msg = {
                "chain": chain,
                "timestamp": timestamp,
                "action": "remove",
                "token_addresses": removed_addresses,
            }
            remove_subject = f"whitelist.tokens.{chain}.remove"

            payload = json.dumps(remove_msg).encode()
            await self.nc.publish(remove_subject, payload)

            logger.info(
                f"📤 Published {len(removed_addresses)} removed tokens to {remove_subject} "
                f"({len(payload)} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish remove delta: {e}")
            return False

    def _calculate_filter_counts(
        self, tokens: Dict[str, Dict[str, Any]]
    ) -> Dict[str, int]:
        """Calculate counts per filter type."""
        filter_counts: Dict[str, int] = {}

        for token_data in tokens.values():
            filters = token_data.get("filters", [])
            for filter_type in filters:
                filter_counts[filter_type] = filter_counts.get(filter_type, 0) + 1

        return filter_counts


# Standalone function for easy integration
async def publish_token_whitelist(
    chain: str,
    tokens: Dict[str, Dict[str, Any]],
    nats_url: str = "nats://localhost:4222",
) -> Dict[str, bool]:
    """
    Standalone function to publish token whitelist.

    Args:
        chain: Chain identifier (ethereum, base, etc.)
        tokens: Dict mapping token address to metadata
        nats_url: NATS server URL

    Returns:
        Dict mapping topic type to success status

    Example:
        >>> tokens = {
        ...     '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48': {
        ...         'symbol': 'USDC',
        ...         'decimals': 6,
        ...         'name': 'USD Coin',
        ...         'filters': ['cross_chain', 'top_transferred']
        ...     },
        ...     '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': {
        ...         'symbol': 'WETH',
        ...         'decimals': 18,
        ...         'name': 'Wrapped Ether',
        ...         'filters': ['cross_chain', 'hyperliquid']
        ...     }
        ... }
        >>> results = await publish_token_whitelist('ethereum', tokens)
        >>> print(results)  # {'full': True, 'add': True, 'remove': True}
    """
    async with TokenWhitelistNatsPublisher(nats_url) as publisher:
        return await publisher.publish_token_whitelist(chain, tokens)
