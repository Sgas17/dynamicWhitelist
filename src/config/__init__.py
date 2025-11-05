"""
Configuration management for dynamicWhitelist project.

This module provides centralized configuration management for the entire
dynamicWhitelist system. Use get_config() to access all configuration settings.

Example:
    from src.config import get_config

    config = get_config()

    # Access database settings
    postgres_url = config.database.postgres_url

    # Access chain settings
    ethereum_rpc = config.chains.get_rpc_url("ethereum")

    # Access protocol settings
    uniswap_factories = config.protocols.get_factory_addresses("uniswap_v3", "ethereum")

    # Access NATS settings
    nats_url = config.nats.get_nats_url()
"""

from .base import BaseConfig, ConfigError
from .chains import ChainConfig
from .database import DatabaseConfig
from .manager import ConfigManager, get_config, reload_config
from .nats_config import NatsConfig
from .protocols import ProtocolConfig

__all__ = [
    "BaseConfig",
    "ConfigError",
    "ChainConfig",
    "ProtocolConfig",
    "DatabaseConfig",
    "NatsConfig",
    "ConfigManager",
    "get_config",
    "reload_config",
]
