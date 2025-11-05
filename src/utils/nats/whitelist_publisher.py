"""
NATS publisher for dynamicWhitelist data.

This module provides functionality to publish whitelist updates
to NATS for consumption by other services.
"""

import asyncio
import logging
from typing import Any, Dict

from .client import NatsClientJS

logger = logging.getLogger(__name__)


class WhitelistPublisher:
    """
    Publisher for whitelist data to NATS/JetStream.

    This class handles publishing token whitelist updates to NATS
    for consumption by other trading and monitoring services.
    """

    def __init__(self, env: str = "local"):
        """
        Initialize the whitelist publisher.

        Args:
            env: Environment to connect to (local, dev, production)
        """
        self.nats_client = NatsClientJS(env)
        self.stream_name = "WHITELIST_UPDATES"
        self.subjects = [
            "whitelist.tokens.hyperliquid",
            "whitelist.tokens.base",
            "whitelist.tokens.ethereum",
            "whitelist.pools.uniswap_v3",
            "whitelist.pools.uniswap_v4",
        ]

    async def aconnect(self):
        """Connect to NATS and setup JetStream"""
        await self.nats_client.aconnect()
        await self.nats_client.aregister_new_stream(self.stream_name, self.subjects)
        logger.info("WhitelistPublisher connected and stream registered")

    def connect(self):
        """Connect to NATS and setup JetStream (synchronous wrapper)"""
        asyncio.run(self.aconnect())

    async def aclose(self):
        """Close NATS connection"""
        await self.nats_client.aclose()
        logger.info("WhitelistPublisher connection closed")

    def close(self):
        """Close NATS connection (synchronous wrapper)"""
        asyncio.run(self.aclose())

    async def apublish_hyperliquid_tokens(self, tokens: Dict[str, float]):
        """
        Publish Hyperliquid token updates.

        Args:
            tokens: Dictionary mapping token symbols to prices
        """
        message = {
            "type": "hyperliquid_tokens_update",
            "data": tokens,
            "timestamp": asyncio.get_event_loop().time(),
        }

        await self.nats_client.apublish("whitelist.tokens.hyperliquid", message)
        logger.info(f"Published {len(tokens)} Hyperliquid tokens to NATS")

    def publish_hyperliquid_tokens(self, tokens: Dict[str, float]):
        """Publish Hyperliquid token updates (synchronous wrapper)"""
        asyncio.run(self.apublish_hyperliquid_tokens(tokens))

    async def apublish_token_update(self, chain: str, tokens: Dict[str, Any]):
        """
        Publish general token updates for any chain.

        Args:
            chain: Blockchain name (ethereum, base, etc.)
            tokens: Token data to publish
        """
        message = {
            "type": f"{chain}_tokens_update",
            "chain": chain,
            "data": tokens,
            "timestamp": asyncio.get_event_loop().time(),
        }

        subject = f"whitelist.tokens.{chain}"
        await self.nats_client.apublish(subject, message)
        logger.info(f"Published {len(tokens)} {chain} tokens to NATS")

    def publish_token_update(self, chain: str, tokens: Dict[str, Any]):
        """Publish general token updates (synchronous wrapper)"""
        asyncio.run(self.apublish_token_update(chain, tokens))

    async def apublish_pool_update(self, protocol: str, pools: Dict[str, Any]):
        """
        Publish pool updates for DeFi protocols.

        Args:
            protocol: Protocol name (uniswap_v3, uniswap_v4, etc.)
            pools: Pool data to publish
        """
        message = {
            "type": f"{protocol}_pools_update",
            "protocol": protocol,
            "data": pools,
            "timestamp": asyncio.get_event_loop().time(),
        }

        subject = f"whitelist.pools.{protocol}"
        await self.nats_client.apublish(subject, message)
        logger.info(f"Published {len(pools)} {protocol} pools to NATS")

    def publish_pool_update(self, protocol: str, pools: Dict[str, Any]):
        """Publish pool updates (synchronous wrapper)"""
        asyncio.run(self.apublish_pool_update(protocol, pools))

    async def apublish_whitelist_status(self, status: Dict[str, Any]):
        """
        Publish general whitelist status updates.

        Args:
            status: Status information about the whitelist
        """
        message = {
            "type": "whitelist_status",
            "data": status,
            "timestamp": asyncio.get_event_loop().time(),
        }

        await self.nats_client.apublish("whitelist.status", message)
        logger.info("Published whitelist status to NATS")

    def publish_whitelist_status(self, status: Dict[str, Any]):
        """Publish whitelist status (synchronous wrapper)"""
        asyncio.run(self.apublish_whitelist_status(status))
