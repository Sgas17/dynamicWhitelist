"""
Storage abstraction layer for the dynamic whitelist system.

This module provides a unified interface for different storage backends:
- PostgreSQL for persistent structured data
- Redis for cache and real-time data
- JSON for configuration and backup

Usage:
    from src.core.storage import StorageManager
    
    storage = StorageManager()
    
    # PostgreSQL operations
    await storage.postgres.store_tokens(tokens)
    tokens = await storage.postgres.get_whitelisted_tokens('ethereum')
    
    # Redis operations
    await storage.redis.set_whitelist('ethereum', whitelist)
    whitelist = await storage.redis.get_whitelist('ethereum')
    
    # JSON operations
    storage.json.save_backup('whitelist_backup.json', data)
    data = storage.json.load_backup('whitelist_backup.json')
"""

from .base import StorageBase, StorageError, ConnectionError, DataError
from .postgres import PostgresStorage
from .redis import RedisStorage
from .json_storage import JsonStorage
from .manager import StorageManager

__all__ = [
    'StorageBase',
    'StorageError',
    'ConnectionError', 
    'DataError',
    'PostgresStorage',
    'RedisStorage',
    'JsonStorage',
    'StorageManager'
]