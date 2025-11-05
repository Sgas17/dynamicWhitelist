"""
NATS configuration for dynamicWhitelist.
"""

from dataclasses import dataclass
from typing import Dict, List

from .base import BaseConfig


@dataclass
class NatsConfig(BaseConfig):
    """NATS messaging configuration."""

    # NATS Connection Settings
    NATS_ENABLED: bool = BaseConfig.get_env_bool("NATS_ENABLED", True)
    NATS_URL_LOCAL: str = BaseConfig.get_env("NATS_URL_LOCAL", "nats://localhost:4222")
    NATS_URL_DEV: str = BaseConfig.get_env("NATS_URL_DEV", "nats://nats:4222")
    NATS_URL_PRODUCTION: str = BaseConfig.get_env(
        "NATS_URL_PRODUCTION", "nats://nats-server:4222"
    )

    # Connection Parameters
    NATS_TIMEOUT: int = BaseConfig.get_env_int("NATS_TIMEOUT", 30)
    NATS_MAX_RECONNECT_ATTEMPTS: int = BaseConfig.get_env_int(
        "NATS_MAX_RECONNECT_ATTEMPTS", 60
    )
    NATS_RECONNECT_TIME_WAIT: int = BaseConfig.get_env_int(
        "NATS_RECONNECT_TIME_WAIT", 2
    )

    # JetStream Configuration
    JETSTREAM_ENABLED: bool = BaseConfig.get_env_bool("JETSTREAM_ENABLED", True)
    STREAM_NAME: str = BaseConfig.get_env("STREAM_NAME", "WHITELIST_UPDATES")

    # Message Publishing Settings
    PUBLISH_BATCH_SIZE: int = BaseConfig.get_env_int("PUBLISH_BATCH_SIZE", 100)
    MESSAGE_TIMEOUT: int = BaseConfig.get_env_int("MESSAGE_TIMEOUT", 10)

    @property
    def nats_urls(self) -> Dict[str, str]:
        """Get NATS URLs for different environments."""
        return {
            "local": self.NATS_URL_LOCAL,
            "dev": self.NATS_URL_DEV,
            "staging": self.NATS_URL_DEV,  # Use dev for staging
            "production": self.NATS_URL_PRODUCTION,
        }

    def get_nats_url(self, environment: str = None) -> str:
        """Get NATS URL for the current or specified environment."""
        env = environment or self.ENVIRONMENT
        return self.nats_urls.get(env, self.NATS_URL_LOCAL)

    @property
    def whitelist_subjects(self) -> List[str]:
        """Get all whitelist-related NATS subjects."""
        return [
            "whitelist.tokens.hyperliquid",
            "whitelist.tokens.ethereum",
            "whitelist.tokens.base",
            "whitelist.tokens.arbitrum",
            "whitelist.pools.uniswap_v2",
            "whitelist.pools.uniswap_v3",
            "whitelist.pools.uniswap_v4",
            "whitelist.pools.sushiswap_v2",
            "whitelist.pools.sushiswap_v3",
            "whitelist.status",
            "whitelist.validation",
            "whitelist.metrics",
        ]

    @property
    def token_subjects(self) -> List[str]:
        """Get token-related NATS subjects."""
        return [subject for subject in self.whitelist_subjects if "tokens" in subject]

    @property
    def pool_subjects(self) -> List[str]:
        """Get pool-related NATS subjects."""
        return [subject for subject in self.whitelist_subjects if "pools" in subject]

    def get_token_subject(self, chain: str) -> str:
        """Get the token subject for a specific chain."""
        return f"whitelist.tokens.{chain.lower()}"

    def get_pool_subject(self, protocol: str) -> str:
        """Get the pool subject for a specific protocol."""
        return f"whitelist.pools.{protocol.lower()}"

    def get_chain_from_subject(self, subject: str) -> str:
        """Extract chain name from a subject."""
        if "tokens" in subject:
            return subject.split(".")[-1]
        return None

    def get_protocol_from_subject(self, subject: str) -> str:
        """Extract protocol name from a subject."""
        if "pools" in subject:
            return subject.split(".")[-1]
        return None

    @property
    def connection_params(self) -> Dict:
        """Get NATS connection parameters."""
        return {
            "servers": [self.get_nats_url()],
            "max_reconnect_attempts": self.NATS_MAX_RECONNECT_ATTEMPTS,
            "reconnect_time_wait": self.NATS_RECONNECT_TIME_WAIT,
            "allow_reconnect": True,
            "ping_interval": 120,
            "max_outstanding_pings": 2,
        }

    @property
    def jetstream_config(self) -> Dict:
        """Get JetStream configuration."""
        return {
            "stream_name": self.STREAM_NAME,
            "subjects": self.whitelist_subjects,
            "storage": "file",  # or "memory" for testing
            "retention": "limits",  # or "interest" or "workqueue"
            "max_age": 24 * 60 * 60,  # 24 hours in seconds
            "max_msgs": 1000000,
            "max_bytes": 1024 * 1024 * 1024,  # 1GB
            "replicas": 1,
            "no_ack": False,
        }
