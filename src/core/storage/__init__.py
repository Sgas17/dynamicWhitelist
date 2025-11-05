"""
Storage abstraction layer for the dynamic whitelist system.

This module provides storage backends for the whitelist pipeline:
- PostgreSQL for persistent structured data
- Redis for cache and real-time data (used by WhitelistPublisher)
- JSON for configuration and backup (used by WhitelistPublisher)

Usage:
    from src.core.storage import PostgresStorage

    storage = PostgresStorage(config)
    await storage.connect()

    # PostgreSQL operations
    await storage.store_tokens(tokens)
    tokens = await storage.get_whitelisted_tokens('ethereum')
"""

from .base import ConnectionError, DataError, StorageBase, StorageError
from .json_storage import JsonStorage
from .postgres import PostgresStorage
from .redis import RedisStorage

__all__ = [
    "StorageBase",
    "StorageError",
    "ConnectionError",
    "DataError",
    "PostgresStorage",
    "RedisStorage",
    "JsonStorage",
]
