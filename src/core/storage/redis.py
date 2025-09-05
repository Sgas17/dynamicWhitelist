"""
Redis storage implementation for caching and real-time data.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import redis.asyncio as redis
from redis.asyncio import Redis

from .base import (
    StorageBase,
    CacheInterface,
    ConnectionError,
    DataError
)

logger = logging.getLogger(__name__)


class RedisStorage(StorageBase, CacheInterface):
    """
    Redis storage implementation for caching and real-time data.
    
    Features:
    - Key-value caching with TTL support
    - Pub/sub for real-time updates
    - Whitelist caching
    - JSON serialization for complex objects
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Redis storage.
        
        Args:
            config: Configuration with keys:
                - host: Redis host
                - port: Redis port
                - password: Redis password (optional)
                - db: Redis database number (default: 0)
                - decode_responses: Whether to decode responses (default: True)
                - socket_timeout: Socket timeout in seconds (default: 5)
                - connection_pool_kwargs: Additional connection pool arguments
        """
        super().__init__(config)
        self.client: Optional[Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        
    async def connect(self) -> None:
        """Establish connection to Redis."""
        try:
            # Build connection pool kwargs
            pool_kwargs = {
                'host': self.config.get('host', 'localhost'),
                'port': self.config.get('port', 6379),
                'db': self.config.get('db', 0),
                'decode_responses': self.config.get('decode_responses', True),
                'socket_timeout': self.config.get('socket_timeout', 5),
                **self.config.get('connection_pool_kwargs', {})
            }
            
            # Only add password if it's actually set
            password = self.config.get('password')
            if password is not None:
                pool_kwargs['password'] = password
            
            pool = redis.ConnectionPool(**pool_kwargs)
            
            self.client = redis.Redis(connection_pool=pool)
            
            # Test connection
            await self.client.ping()
            
            # Setup pubsub
            self.pubsub = self.client.pubsub()
            
            self.is_connected = True
            logger.info("Redis connection established")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectionError(f"Redis connection failed: {e}")
            
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.pubsub:
            await self.pubsub.close()
            
        if self.client:
            await self.client.close()
            
        self.is_connected = False
        logger.info("Redis connection closed")
        
    async def health_check(self) -> bool:
        """Check Redis connection health."""
        if not self.client:
            return False
            
        try:
            response = await self.client.ping()
            return response is True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
            
    # Cache Interface Implementation
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a cache value with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized if not string)
            ttl: Time-to-live in seconds
            
        Returns:
            bool: True if successful
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            # Serialize non-string values to JSON
            if not isinstance(value, str):
                value = json.dumps(value, default=str)
                
            if ttl:
                result = await self.client.setex(key, ttl, value)
            else:
                result = await self.client.set(key, value)
                
            return result is True
            
        except Exception as e:
            logger.error(f"Failed to set cache key {key}: {e}")
            raise DataError(f"Cache set failed: {e}")
            
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a cached value.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            value = await self.client.get(key)
            
            if value is None:
                return None
                
            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Failed to get cache key {key}: {e}")
            raise DataError(f"Cache get failed: {e}")
            
    async def delete(self, key: str) -> bool:
        """
        Delete a cached value.
        
        Args:
            key: Cache key
            
        Returns:
            bool: True if key existed and was deleted
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            result = await self.client.delete(key)
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to delete cache key {key}: {e}")
            raise DataError(f"Cache delete failed: {e}")
            
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.
        
        Args:
            key: Cache key
            
        Returns:
            bool: True if key exists
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            result = await self.client.exists(key)
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to check key existence {key}: {e}")
            raise DataError(f"Cache exists check failed: {e}")
            
    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for a key.
        
        Args:
            key: Cache key
            ttl: Time-to-live in seconds
            
        Returns:
            bool: True if expiration was set
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            result = await self.client.expire(key, ttl)
            return result is True
            
        except Exception as e:
            logger.error(f"Failed to set expiration for key {key}: {e}")
            raise DataError(f"Cache expire failed: {e}")
            
    # Whitelist-specific methods
    
    async def set_whitelist(self, chain: str, whitelist: List[Dict[str, Any]], ttl: int = 3600) -> bool:
        """
        Cache a whitelist for a specific chain.
        
        Args:
            chain: Chain identifier (ethereum, base, arbitrum)
            whitelist: List of whitelisted tokens
            ttl: Time-to-live in seconds (default: 1 hour)
            
        Returns:
            bool: True if successful
        """
        key = f"whitelist:{chain}"
        return await self.set(key, whitelist, ttl)
        
    async def get_whitelist(self, chain: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached whitelist for a specific chain.
        
        Args:
            chain: Chain identifier
            
        Returns:
            List of whitelisted tokens or None if not cached
        """
        key = f"whitelist:{chain}"
        return await self.get(key)
        
    async def invalidate_whitelist(self, chain: str) -> bool:
        """
        Invalidate cached whitelist for a specific chain.
        
        Args:
            chain: Chain identifier
            
        Returns:
            bool: True if whitelist was cached and deleted
        """
        key = f"whitelist:{chain}"
        return await self.delete(key)
        
    # Pool-specific methods
    
    async def set_pool_data(self, chain: str, protocol: str, pools: List[Dict[str, Any]], ttl: int = 1800) -> bool:
        """
        Cache pool data for a specific chain and protocol.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier (uniswap_v2, uniswap_v3, etc.)
            pools: List of pool data
            ttl: Time-to-live in seconds (default: 30 minutes)
            
        Returns:
            bool: True if successful
        """
        key = f"pools:{chain}:{protocol}"
        return await self.set(key, pools, ttl)
        
    async def get_pool_data(self, chain: str, protocol: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached pool data for a specific chain and protocol.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier
            
        Returns:
            List of pools or None if not cached
        """
        key = f"pools:{chain}:{protocol}"
        return await self.get(key)
        
    # Pub/Sub methods
    
    async def publish(self, channel: str, message: Union[str, Dict[str, Any]]) -> int:
        """
        Publish a message to a Redis channel.
        
        Args:
            channel: Channel name
            message: Message to publish (will be JSON serialized if dict)
            
        Returns:
            int: Number of subscribers that received the message
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            if isinstance(message, dict):
                message = json.dumps(message, default=str)
                
            result = await self.client.publish(channel, message)
            return result
            
        except Exception as e:
            logger.error(f"Failed to publish to channel {channel}: {e}")
            raise DataError(f"Publish failed: {e}")
            
    async def subscribe(self, *channels: str) -> None:
        """
        Subscribe to Redis channels.
        
        Args:
            *channels: Channel names to subscribe to
        """
        if not self.pubsub:
            raise ConnectionError("PubSub not initialized")
            
        try:
            await self.pubsub.subscribe(*channels)
            logger.info(f"Subscribed to channels: {channels}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe to channels: {e}")
            raise DataError(f"Subscribe failed: {e}")
            
    async def get_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Get a message from subscribed channels.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Message dict or None if no message
        """
        if not self.pubsub:
            raise ConnectionError("PubSub not initialized")
            
        try:
            message = await self.pubsub.get_message(timeout=timeout)
            
            if message and message['type'] == 'message':
                # Try to parse JSON data
                data = message['data']
                if isinstance(data, str):
                    try:
                        message['data'] = json.loads(data)
                    except json.JSONDecodeError:
                        pass
                        
            return message
            
        except Exception as e:
            logger.error(f"Failed to get message: {e}")
            raise DataError(f"Get message failed: {e}")
            
    # Batch operations
    
    async def mset(self, mapping: Dict[str, Any]) -> bool:
        """
        Set multiple key-value pairs at once.
        
        Args:
            mapping: Dictionary of key-value pairs
            
        Returns:
            bool: True if successful
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            # Serialize non-string values
            processed = {}
            for key, value in mapping.items():
                if not isinstance(value, str):
                    value = json.dumps(value, default=str)
                processed[key] = value
                
            result = await self.client.mset(processed)
            return result is True
            
        except Exception as e:
            logger.error(f"Failed to mset: {e}")
            raise DataError(f"Batch set failed: {e}")
            
    async def mget(self, keys: List[str]) -> List[Optional[Any]]:
        """
        Get multiple values at once.
        
        Args:
            keys: List of keys to get
            
        Returns:
            List of values (None for missing keys)
        """
        if not self.client:
            raise ConnectionError("Not connected to Redis")
            
        try:
            values = await self.client.mget(keys)
            
            # Try to deserialize JSON values
            result = []
            for value in values:
                if value is None:
                    result.append(None)
                else:
                    try:
                        result.append(json.loads(value))
                    except (json.JSONDecodeError, TypeError):
                        result.append(value)
                        
            return result
            
        except Exception as e:
            logger.error(f"Failed to mget: {e}")
            raise DataError(f"Batch get failed: {e}")