"""
Whitelist publisher that publishes final whitelists without storing back to source data.

KISS: Simple publisher that sends whitelists to consumption endpoints.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, UTC
import json

from src.config import ConfigManager
from .redis import RedisStorage
from .json_storage import JsonStorage

logger = logging.getLogger(__name__)


class WhitelistPublisher:
    """
    Publishes final whitelists to consumption endpoints.
    
    Does NOT store back to source PostgreSQL tables.
    Publishing targets:
    - Redis cache (for fast API access)
    - JSON files (for backup/audit)
    - NATS messaging (for real-time consumers)
    """
    
    def __init__(self, config: Optional[ConfigManager] = None):
        """Initialize publisher."""
        self.config = config or ConfigManager()
        self.redis: Optional[RedisStorage] = None
        self.json_storage: Optional[JsonStorage] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        # Initialize Redis cache
        self.redis = RedisStorage(self.config)
        await self.redis.connect()
        
        # Initialize JSON storage
        self.json_storage = JsonStorage(self.config)
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.redis:
            await self.redis.disconnect()
    
    async def publish_whitelist(
        self, 
        chain: str, 
        whitelist: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, bool]:
        """
        Publish whitelist to all consumption endpoints.
        
        Args:
            chain: Chain identifier
            whitelist: Final filtered whitelist
            metadata: Additional metadata about generation
            
        Returns:
            Dictionary mapping endpoint to success status
        """
        if not whitelist:
            logger.warning(f"Empty whitelist provided for {chain}")
            return {}
        
        # Add publishing metadata
        publish_metadata = {
            "chain": chain,
            "token_count": len(whitelist),
            "published_at": datetime.now(UTC).isoformat(),
            "publisher": "dynamicWhitelist",
            **(metadata or {})
        }
        
        results = {}
        
        # Publish to Redis cache (fast API access)
        results['redis'] = await self._publish_to_redis(chain, whitelist, publish_metadata)
        
        # Publish to JSON backup (audit/recovery)
        results['json'] = await self._publish_to_json(chain, whitelist, publish_metadata)
        
        # Publish to NATS messaging (real-time consumers)
        results['nats'] = await self._publish_to_nats(chain, whitelist, publish_metadata)
        
        # Log summary
        successful_endpoints = [k for k, v in results.items() if v]
        failed_endpoints = [k for k, v in results.items() if not v]
        
        logger.info(
            f"Published {len(whitelist)} tokens for {chain} - "
            f"Success: {successful_endpoints}, Failed: {failed_endpoints}"
        )
        
        return results
    
    async def _publish_to_redis(
        self, 
        chain: str, 
        whitelist: List[Dict[str, Any]], 
        metadata: Dict[str, Any]
    ) -> bool:
        """Publish whitelist to Redis cache."""
        try:
            if not self.redis:
                logger.error("Redis not initialized")
                return False
                
            # Store whitelist with metadata
            success = await self.redis.set_whitelist(chain, whitelist)
            
            if success:
                # Store metadata separately
                metadata_key = f"whitelist:{chain}:metadata"
                await self.redis.redis.set(
                    metadata_key, 
                    json.dumps(metadata),
                    ex=7 * 24 * 3600  # 7 days TTL
                )
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")
            return False
    
    async def _publish_to_json(
        self, 
        chain: str, 
        whitelist: List[Dict[str, Any]], 
        metadata: Dict[str, Any]
    ) -> bool:
        """Publish whitelist to JSON backup."""
        try:
            if not self.json_storage:
                logger.error("JSON storage not initialized")
                return False
            
            # Create timestamped backup
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            filename = f"whitelist_{chain}_{timestamp}.json"
            
            whitelist_data = {
                "metadata": metadata,
                "whitelist": whitelist
            }
            
            success = self.json_storage.save(filename, whitelist_data)
            
            # Also save as "latest" for easy access
            latest_filename = f"whitelist_{chain}_latest.json"
            self.json_storage.save(latest_filename, whitelist_data)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to publish to JSON: {e}")
            return False
    
    async def _publish_to_nats(
        self, 
        chain: str, 
        whitelist: List[Dict[str, Any]], 
        metadata: Dict[str, Any]
    ) -> bool:
        """Publish whitelist to NATS messaging."""
        try:
            # Import NATS here to avoid circular imports
            from ...utils.nats.whitelist_publisher import WhitelistNatsPublisher
            
            async with WhitelistNatsPublisher() as nats_publisher:
                success = await nats_publisher.publish_whitelist(chain, whitelist, metadata)
                return success
                
        except ImportError:
            logger.warning("NATS publisher not available, skipping")
            return True  # Don't fail if NATS not configured
        except Exception as e:
            logger.error(f"Failed to publish to NATS: {e}")
            return False
    
    async def get_published_whitelist(self, chain: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve published whitelist from cache.
        
        Args:
            chain: Chain identifier
            
        Returns:
            Cached whitelist or None if not found
        """
        try:
            if not self.redis:
                logger.error("Redis not initialized")
                return None
                
            return await self.redis.get_whitelist(chain)
            
        except Exception as e:
            logger.error(f"Failed to retrieve whitelist from cache: {e}")
            return None
    
    async def get_publication_metadata(self, chain: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata about last whitelist publication.
        
        Args:
            chain: Chain identifier
            
        Returns:
            Publication metadata or None if not found
        """
        try:
            if not self.redis:
                logger.error("Redis not initialized")
                return None
            
            metadata_key = f"whitelist:{chain}:metadata"
            metadata_json = await self.redis.redis.get(metadata_key)
            
            if metadata_json:
                return json.loads(metadata_json)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve publication metadata: {e}")
            return None