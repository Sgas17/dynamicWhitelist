"""Tests for whitelist publisher functionality."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, UTC

from src.core.storage.whitelist_publisher import WhitelistPublisher
from src.config import ConfigManager


class TestWhitelistPublisher:
    """Test cases for WhitelistPublisher."""
    
    @pytest.fixture
    def sample_whitelist(self):
        """Sample whitelist data."""
        return [
            {
                "address": "0xa0b86a33e6c6c7c8e1f2e3f4a5b6c7d8e9f0a1b2",
                "symbol": "USDC",
                "name": "USD Coin",
                "decimals": 6
            },
            {
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "symbol": "WETH",
                "name": "Wrapped Ether",
                "decimals": 18
            }
        ]
    
    @pytest.fixture
    def sample_metadata(self):
        """Sample metadata."""
        return {
            "generation_method": "hyperliquid_filter",
            "source_token_count": 5000,
            "filter_criteria": ["market_cap > 1M", "liquidity > 100k"]
        }
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_publisher_context_manager(self, mock_json_storage, mock_redis_storage):
        """Test publisher context manager initialization."""
        mock_redis = AsyncMock()
        mock_redis_storage.return_value = mock_redis
        
        async with WhitelistPublisher() as publisher:
            assert publisher.redis == mock_redis
            assert publisher.json_storage is not None
            mock_redis.connect.assert_called_once()
            
        # Should disconnect on exit
        mock_redis.disconnect.assert_called_once()
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_publish_whitelist_success(
        self, 
        mock_json_storage, 
        mock_redis_storage,
        sample_whitelist,
        sample_metadata
    ):
        """Test successful whitelist publishing."""
        # Mock Redis
        mock_redis = AsyncMock()
        mock_redis.set_whitelist.return_value = True
        mock_redis.redis.set = AsyncMock()
        mock_redis_storage.return_value = mock_redis
        
        # Mock JSON storage
        mock_json = MagicMock()
        mock_json.save.return_value = True
        mock_json_storage.return_value = mock_json
        
        async with WhitelistPublisher() as publisher:
            # Mock NATS publisher to avoid import issues
            with patch.object(publisher, '_publish_to_nats', return_value=True):
                results = await publisher.publish_whitelist(
                    "ethereum",
                    sample_whitelist,
                    sample_metadata
                )
        
        # All endpoints should succeed
        assert results['redis'] is True
        assert results['json'] is True
        assert results['nats'] is True
        
        # Verify Redis was called
        mock_redis.set_whitelist.assert_called_once()
        call_args = mock_redis.set_whitelist.call_args
        assert call_args[0][0] == "ethereum"  # chain
        assert call_args[0][1] == sample_whitelist  # whitelist
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_publish_empty_whitelist(self, mock_json_storage, mock_redis_storage):
        """Test publishing empty whitelist."""
        mock_redis = AsyncMock()
        mock_redis_storage.return_value = mock_redis
        
        async with WhitelistPublisher() as publisher:
            results = await publisher.publish_whitelist("ethereum", [])
        
        # Should return empty results for empty whitelist
        assert results == {}
        mock_redis.set_whitelist.assert_not_called()
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_publish_redis_failure(
        self,
        mock_json_storage,
        mock_redis_storage,
        sample_whitelist
    ):
        """Test Redis publishing failure."""
        # Mock Redis failure
        mock_redis = AsyncMock()
        mock_redis.set_whitelist.side_effect = Exception("Redis connection failed")
        mock_redis_storage.return_value = mock_redis
        
        # Mock JSON success
        mock_json = MagicMock()
        mock_json.save.return_value = True
        mock_json_storage.return_value = mock_json
        
        async with WhitelistPublisher() as publisher:
            with patch.object(publisher, '_publish_to_nats', return_value=True):
                results = await publisher.publish_whitelist("ethereum", sample_whitelist)
        
        # Redis should fail, others succeed
        assert results['redis'] is False
        assert results['json'] is True
        assert results['nats'] is True
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_get_published_whitelist(
        self,
        mock_json_storage,
        mock_redis_storage,
        sample_whitelist
    ):
        """Test retrieving published whitelist."""
        mock_redis = AsyncMock()
        mock_redis.get_whitelist.return_value = sample_whitelist
        mock_redis_storage.return_value = mock_redis
        
        async with WhitelistPublisher() as publisher:
            result = await publisher.get_published_whitelist("ethereum")
        
        assert result == sample_whitelist
        mock_redis.get_whitelist.assert_called_once_with("ethereum")
    
    @patch('src.core.storage.whitelist_publisher.RedisStorage')
    @patch('src.core.storage.whitelist_publisher.JsonStorage')
    @pytest.mark.asyncio
    async def test_get_publication_metadata(
        self,
        mock_json_storage,
        mock_redis_storage,
        sample_metadata
    ):
        """Test retrieving publication metadata."""
        import json
        
        mock_redis = AsyncMock()
        mock_redis.redis.get.return_value = json.dumps(sample_metadata)
        mock_redis_storage.return_value = mock_redis
        
        async with WhitelistPublisher() as publisher:
            result = await publisher.get_publication_metadata("ethereum")
        
        assert result == sample_metadata
        mock_redis.redis.get.assert_called_once_with("whitelist:ethereum:metadata")
    
    def test_publisher_does_not_import_postgres(self):
        """Test that publisher doesn't import PostgreSQL storage."""
        # This is important - the publisher should NOT have access to source data
        from src.core.storage.whitelist_publisher import WhitelistPublisher
        
        publisher = WhitelistPublisher()
        
        # Should not have postgres attribute
        assert not hasattr(publisher, 'postgres')
        assert not hasattr(publisher, 'postgresql')
        
        # Only publishing-related storage should be available
        assert hasattr(publisher, 'redis')
        assert hasattr(publisher, 'json_storage')