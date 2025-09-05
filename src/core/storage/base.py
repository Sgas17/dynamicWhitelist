"""
Base classes and interfaces for storage implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Base exception for storage-related errors."""
    pass


class ConnectionError(StorageError):
    """Raised when connection to storage backend fails."""
    pass


class DataError(StorageError):
    """Raised when data operations fail."""
    pass


class StorageBase(ABC):
    """
    Abstract base class for storage implementations.
    All storage backends must implement these methods.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize storage backend with configuration.
        
        Args:
            config: Configuration dictionary for the storage backend
        """
        self.config = config
        self.is_connected = False
        
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the storage backend."""
        pass
        
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the storage backend."""
        pass
        
    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the storage backend is healthy and accessible.
        
        Returns:
            bool: True if healthy, False otherwise
        """
        pass
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


class TokenStorageInterface(ABC):
    """Interface for token-related storage operations."""
    
    @abstractmethod
    async def store_token(self, token: Dict[str, Any], chain: str) -> bool:
        """Store a single token."""
        pass
        
    @abstractmethod
    async def store_tokens_batch(self, tokens: List[Dict[str, Any]], chain: str) -> int:
        """Store multiple tokens in batch."""
        pass
        
    @abstractmethod
    async def get_token(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single token by address."""
        pass
        
    @abstractmethod
    async def get_whitelisted_tokens(self, chain: str) -> List[Dict[str, Any]]:
        """Get all whitelisted tokens for a chain."""
        pass
        
    @abstractmethod
    async def update_token_status(self, address: str, chain: str, status: str) -> bool:
        """Update token whitelist status."""
        pass


class PoolStorageInterface(ABC):
    """Interface for pool-related storage operations."""
    
    @abstractmethod
    async def store_pool(self, pool: Dict[str, Any], chain: str, protocol: str) -> bool:
        """Store a single pool."""
        pass
        
    @abstractmethod
    async def store_pools_batch(self, pools: List[Dict[str, Any]], chain: str, protocol: str) -> int:
        """Store multiple pools in batch."""
        pass
        
    @abstractmethod
    async def get_pool(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single pool by address."""
        pass
        
    @abstractmethod
    async def get_active_pools(self, chain: str, protocol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all active pools for a chain and optionally a specific protocol."""
        pass
        
    @abstractmethod
    async def update_pool_liquidity(self, address: str, chain: str, liquidity: float) -> bool:
        """Update pool liquidity."""
        pass


class CacheInterface(ABC):
    """Interface for caching operations."""
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a cache value with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            bool: True if successful
        """
        pass
        
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a cached value.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        pass
        
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Delete a cached value.
        
        Args:
            key: Cache key
            
        Returns:
            bool: True if key existed and was deleted
        """
        pass
        
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            bool: True if key exists
        """
        pass
        
    @abstractmethod
    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for a key.
        
        Args:
            key: Cache key
            ttl: Time-to-live in seconds
            
        Returns:
            bool: True if expiration was set
        """
        pass


class TransactionManager(ABC):
    """Interface for transaction management."""
    
    @abstractmethod
    async def begin_transaction(self) -> Any:
        """Begin a new transaction."""
        pass
        
    @abstractmethod
    async def commit_transaction(self, transaction: Any) -> bool:
        """Commit a transaction."""
        pass
        
    @abstractmethod
    async def rollback_transaction(self, transaction: Any) -> bool:
        """Rollback a transaction."""
        pass