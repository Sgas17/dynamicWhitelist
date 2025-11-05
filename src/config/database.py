"""
Database configuration for dynamicWhitelist.
"""

from dataclasses import dataclass
from typing import Optional

from .base import BaseConfig


@dataclass
class DatabaseConfig(BaseConfig):
    """Database connection and settings configuration."""

    # PostgreSQL Configuration
    POSTGRES_HOST: str = BaseConfig.get_env("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = BaseConfig.get_env_int("POSTGRES_PORT", 5432)
    POSTGRES_USER: str = BaseConfig.get_env("POSTGRES_USER", required=True)
    POSTGRES_PASSWORD: str = BaseConfig.get_env("POSTGRES_PASSWORD", required=True)
    POSTGRES_DB: str = BaseConfig.get_env("POSTGRES_DB", "whitelist")

    # Redis Configuration
    REDIS_HOST: str = BaseConfig.get_env("REDIS_HOST", "localhost")
    REDIS_PORT: int = BaseConfig.get_env_int("REDIS_PORT", 6379)
    REDIS_PASSWORD: Optional[str] = BaseConfig.get_env("REDIS_PASSWORD") or None
    REDIS_DB: int = BaseConfig.get_env_int("REDIS_DB", 0)

    # Database Settings
    CHAIN_ID: str = BaseConfig.get_env("CHAIN_ID", "1")  # Ethereum mainnet by default
    MAX_CONNECTIONS: int = BaseConfig.get_env_int("MAX_CONNECTIONS", 20)
    CONNECTION_TIMEOUT: int = BaseConfig.get_env_int("CONNECTION_TIMEOUT", 30)

    # Table Naming
    POOLS_TABLE_PREFIX: str = "network"
    POOLS_TABLE_SUFFIX: str = "dex_pools_cryo"

    @property
    def postgres_url(self) -> str:
        """Build PostgreSQL connection URL."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def pools_table_name(self) -> str:
        """Get the pools table name for the current chain."""
        return f"{self.POOLS_TABLE_PREFIX}_{self.CHAIN_ID}_{self.POOLS_TABLE_SUFFIX}"

    def get_redis_connection_kwargs(self) -> dict:
        """Get Redis connection parameters."""
        kwargs = {
            "host": self.REDIS_HOST,
            "port": self.REDIS_PORT,
            "db": self.REDIS_DB,
            "decode_responses": True,
            "socket_timeout": self.CONNECTION_TIMEOUT,
            "socket_connect_timeout": self.CONNECTION_TIMEOUT,
        }

        # Only add password if it's actually set and not empty/whitespace
        if self.REDIS_PASSWORD and self.REDIS_PASSWORD.strip():
            kwargs["password"] = self.REDIS_PASSWORD.strip()

        return kwargs
