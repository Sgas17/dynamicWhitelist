"""
Storage manager that provides unified access to all storage backends.
"""

import logging
from typing import Any, Dict, Optional
import asyncio

from src.config import ConfigManager
from .postgres import PostgresStorage
from .redis import RedisStorage
from .json_storage import JsonStorage
from .whitelist_publisher import WhitelistPublisher
from .base import StorageError

logger = logging.getLogger(__name__)


class StorageManager:
    """
    Unified storage manager for the dynamic whitelist system.
    
    Provides access to PostgreSQL, Redis, and JSON storage backends
    through a single interface, managing connections and lifecycle.
    
    Usage:
        async with StorageManager() as storage:
            # PostgreSQL operations
            tokens = await storage.postgres.get_whitelisted_tokens('ethereum')
            
            # Redis caching
            await storage.redis.set_whitelist('ethereum', tokens)
            
            # JSON backup
            storage.json.save_whitelist('ethereum', tokens)
    """
    
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Initialize storage manager.
        
        Args:
            config: Configuration manager instance (creates default if None)
        """
        self.config = config or ConfigManager()
        
        # Initialize storage backends
        self.postgres = PostgresStorage(self._get_postgres_config())
        self.redis = RedisStorage(self._get_redis_config())
        self.json = JsonStorage(self._get_json_config())
        
        self.is_initialized = False
        
    def _get_postgres_config(self) -> Dict[str, Any]:
        """Get PostgreSQL configuration from config manager."""
        return {
            'host': self.config.database.POSTGRES_HOST,
            'port': self.config.database.POSTGRES_PORT, 
            'user': self.config.database.POSTGRES_USER,
            'password': self.config.database.POSTGRES_PASSWORD,
            'database': self.config.database.POSTGRES_DB,
            'pool_size': getattr(self.config.database, 'MAX_CONNECTIONS', 10),
            'pool_timeout': getattr(self.config.database, 'CONNECTION_TIMEOUT', 30)
        }
        
    def _get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration from config manager."""
        config = self.config.database.get_redis_connection_kwargs()
        # Override socket_timeout for consistency
        config['socket_timeout'] = 5
        return config
        
    def _get_json_config(self) -> Dict[str, Any]:
        """Get JSON storage configuration from config manager."""
        return {
            'base_path': self.config.base.DATA_DIR / 'json',
            'compress': False,
            'pretty': True,
            'backup_count': 5
        }
        
    async def initialize(self) -> None:
        """
        Initialize all storage backends.
        
        Establishes connections to PostgreSQL and Redis,
        and ensures JSON storage directory exists.
        Individual backend failures are logged but don't prevent initialization.
        """
        if self.is_initialized:
            logger.warning("Storage manager already initialized")
            return
            
        initialization_results = {}
        
        # Initialize PostgreSQL
        try:
            await self.postgres.connect()
            initialization_results['postgres'] = True
            logger.info("PostgreSQL storage initialized successfully")
        except Exception as e:
            logger.warning(f"PostgreSQL initialization failed: {e}")
            initialization_results['postgres'] = False
            
        # Initialize Redis  
        try:
            await self.redis.connect()
            initialization_results['redis'] = True
            logger.info("Redis storage initialized successfully")
        except Exception as e:
            logger.warning(f"Redis initialization failed: {e}")
            initialization_results['redis'] = False
            
        # Initialize JSON storage (should always work)
        try:
            await self.json.connect()
            initialization_results['json'] = True
            logger.info("JSON storage initialized successfully")
        except Exception as e:
            logger.error(f"JSON storage initialization failed: {e}")
            initialization_results['json'] = False
        
        # Consider initialized if at least one backend works
        if any(initialization_results.values()):
            self.is_initialized = True
            working_backends = [k for k, v in initialization_results.items() if v]
            logger.info(f"Storage manager initialized with backends: {working_backends}")
        else:
            raise StorageError("All storage backends failed to initialize")
            
    async def shutdown(self) -> None:
        """
        Shutdown all storage backends.
        
        Closes connections gracefully.
        """
        if not self.is_initialized:
            return
            
        try:
            # Shutdown backends in parallel
            await asyncio.gather(
                self.postgres.disconnect(),
                self.redis.disconnect(),
                self.json.disconnect(),
                return_exceptions=True
            )
            
            self.is_initialized = False
            logger.info("Storage manager shutdown successfully")
            
        except Exception as e:
            logger.error(f"Error during storage manager shutdown: {e}")
            
    async def health_check(self) -> Dict[str, bool]:
        """
        Check health of all storage backends.
        
        Returns:
            Dictionary mapping backend name to health status
        """
        results = {}
        
        # Check each backend
        results['postgres'] = await self.postgres.health_check()
        results['redis'] = await self.redis.health_check()
        results['json'] = await self.json.health_check()
        
        # Overall health
        results['healthy'] = all(results.values())
        
        return results
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()
        
    # High-level operations combining multiple backends
    
    async def get_cached_whitelist(self, chain: str) -> Optional[list]:
        """
        Get whitelist from cache, falling back to database if needed.
        
        Args:
            chain: Chain identifier
            
        Returns:
            List of whitelisted tokens or None
        """
        # Try Redis cache first
        whitelist = await self.redis.get_whitelist(chain)
        
        if whitelist is not None:
            logger.debug(f"Whitelist for {chain} found in cache")
            return whitelist
            
        # Fall back to PostgreSQL
        logger.debug(f"Whitelist for {chain} not in cache, fetching from database")
        whitelist = await self.postgres.get_whitelisted_tokens(chain)
        
        if whitelist:
            # Update cache for next time
            await self.redis.set_whitelist(chain, whitelist)
            
        return whitelist
        
    async def publish_whitelist(
        self, 
        chain: str, 
        whitelist: list, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, bool]:
        """
        Publish final whitelist to consumption endpoints.
        
        IMPORTANT: Does NOT store back to PostgreSQL source tables.
        Only publishes to Redis cache, JSON backup, and NATS messaging.
        
        Args:
            chain: Chain identifier
            whitelist: Final filtered whitelist
            metadata: Additional metadata about generation
            
        Returns:
            Dictionary mapping endpoint to success status
        """
        async with WhitelistPublisher(self.config) as publisher:
            return await publisher.publish_whitelist(chain, whitelist, metadata)
    
    async def save_whitelist_everywhere(self, chain: str, whitelist: list) -> Dict[str, bool]:
        """
        DEPRECATED: Use publish_whitelist() instead.
        
        This method incorrectly stored whitelists back to source PostgreSQL tables.
        """
        logger.warning(
            "save_whitelist_everywhere() is deprecated and creates circular data flow. "
            "Use publish_whitelist() instead."
        )
        return await self.publish_whitelist(chain, whitelist)
        
    async def get_cached_pools(self, chain: str, protocol: str) -> Optional[list]:
        """
        Get pools from cache, falling back to database if needed.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier
            
        Returns:
            List of pools or None
        """
        # Try Redis cache first
        pools = await self.redis.get_pool_data(chain, protocol)
        
        if pools is not None:
            logger.debug(f"Pools for {chain}/{protocol} found in cache")
            return pools
            
        # Fall back to PostgreSQL
        logger.debug(f"Pools for {chain}/{protocol} not in cache, fetching from database")
        pools = await self.postgres.get_active_pools(chain, protocol)
        
        if pools:
            # Update cache for next time
            await self.redis.set_pool_data(chain, protocol, pools)
            
        return pools
        
    async def save_pools_everywhere(self, chain: str, protocol: str, pools: list) -> Dict[str, bool]:
        """
        Save pools to all storage backends.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier
            pools: List of pools
            
        Returns:
            Dictionary mapping backend name to success status
        """
        results = {}
        
        # Save to PostgreSQL
        try:
            count = await self.postgres.store_pools_batch(pools, chain, protocol)
            results['postgres'] = count > 0
        except Exception as e:
            logger.error(f"Failed to save pools to PostgreSQL: {e}")
            results['postgres'] = False
            
        # Save to Redis cache
        try:
            results['redis'] = await self.redis.set_pool_data(chain, protocol, pools)
        except Exception as e:
            logger.error(f"Failed to save pools to Redis: {e}")
            results['redis'] = False
            
        # Save to JSON backup
        try:
            results['json'] = self.json.save_pools(chain, protocol, pools)
        except Exception as e:
            logger.error(f"Failed to save pools to JSON: {e}")
            results['json'] = False
            
        return results
        
    async def export_all_data(self, export_file: str = 'export.json') -> bool:
        """
        Export all data from the system.
        
        Args:
            export_file: Export filename
            
        Returns:
            bool: True if successful
        """
        try:
            # Gather all data from PostgreSQL
            chains = ['ethereum', 'base', 'arbitrum']
            protocols = ['uniswap_v2', 'uniswap_v3', 'sushiswap']
            
            export_data = {
                'whitelists': {},
                'pools': {}
            }
            
            for chain in chains:
                # Get whitelisted tokens
                tokens = await self.postgres.get_whitelisted_tokens(chain)
                if tokens:
                    export_data['whitelists'][chain] = tokens
                    
                # Get pools for each protocol
                for protocol in protocols:
                    pools = await self.postgres.get_active_pools(chain, protocol)
                    if pools:
                        key = f"{chain}_{protocol}"
                        export_data['pools'][key] = pools
                        
            # Save to JSON file
            return self.json.save(export_file, export_data)
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            return False
            
    async def clear_all_caches(self) -> bool:
        """
        Clear all Redis caches.
        
        Returns:
            bool: True if successful
        """
        try:
            chains = ['ethereum', 'base', 'arbitrum']
            protocols = ['uniswap_v2', 'uniswap_v3', 'sushiswap']
            
            for chain in chains:
                # Clear whitelist cache
                await self.redis.invalidate_whitelist(chain)
                
                # Clear pool caches
                for protocol in protocols:
                    key = f"pools:{chain}:{protocol}"
                    await self.redis.delete(key)
                    
            logger.info("All caches cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear caches: {e}")
            return False