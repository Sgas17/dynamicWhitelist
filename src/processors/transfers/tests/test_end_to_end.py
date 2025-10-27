"""
End-to-end tests for unified transfer processor.

These tests verify the complete functionality without external dependencies.
"""

import pytest
import polars as pl
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock

from ..unified_transfer_processor import UnifiedTransferProcessor


class TestEndToEndTransferProcessing:
    """End-to-end tests for complete transfer processing functionality."""

    @pytest.fixture
    def processor(self):
        """Create processor with mocked dependencies."""
        with patch('src.config.ConfigManager'), \
             patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine'), \
             patch('src.processors.transfers.unified_transfer_processor.setup_timescale_tables'):
            return UnifiedTransferProcessor("ethereum")

    @pytest.mark.asyncio
    async def test_complete_workflow_mock(self, processor):
        """Test complete workflow with all components mocked."""
        
        # Mock external dependencies
        mock_files = [Mock()]
        mock_df = pl.DataFrame([
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transaction_hash": "0x1234567890abcdef",
                "from_address": "0x742d35cc6e3c014c41ba3ab0b41f2e2e5e9e5678",
                "to_address": "0x8ba1f109551bd432803012645heff07b94090123",
                "value": 1000000000000000000,
                "timestamp": datetime.now()
            },
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234", 
                "transaction_hash": "0x2345678901bcdef1",
                "from_address": "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",  # MEV
                "to_address": "0x9ba1f109551bd432803012645heff07b94090456",
                "value": 500000000000000000,
                "timestamp": datetime.now()
            }
        ])
        
        expected_results = [
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transfer_count": 2,
                "unique_senders": 2,
                "unique_receivers": 2,
                "avg_transfers": 2.0
            }
        ]
        
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.glob', return_value=mock_files), \
             patch('polars.read_parquet', return_value=mock_df), \
             patch('src.processors.transfers.unified_transfer_processor.store_raw_transfers'), \
             patch('src.processors.transfers.unified_transfer_processor.aggregate_raw_to_hourly'), \
             patch('src.processors.transfers.unified_transfer_processor.get_top_tokens_by_average', return_value=expected_results), \
             patch.object(processor, '_update_redis_cache', new_callable=AsyncMock):
            
            # Run complete processing
            result = await processor.process(
                hours_back=24,
                min_transfers=1,
                store_raw=True,
                update_cache=True
            )
            
            # Verify results
            assert result.success is True
            assert len(result.data) == 1
            assert result.data[0]["token_address"] == "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234"
            assert result.processed_count == 1  # 1 unique token stored
            
            # Verify metadata
            assert result.metadata["hours_back"] == 24
            assert result.metadata["min_transfers"] == 1
            assert result.metadata["raw_stored"] is True
            assert result.metadata["cache_updated"] is True

    def test_mev_address_configuration(self, processor):
        """Test MEV address configuration and loading."""
        mev_addresses = processor._load_mev_addresses()
        
        # Verify essential MEV addresses
        expected_addresses = {
            "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",  # jared
            "0x00000000000000adc04c56bf30ac9d3c0aaf14dc",  # searcher
            "0x000000000000084e91743124a982076c59f10084",  # mev bot
        }
        
        for addr in expected_addresses:
            assert addr in mev_addresses, f"MEV address {addr} not found"
        
        # Verify all addresses are properly formatted
        for addr in mev_addresses:
            assert addr.startswith("0x"), f"Address {addr} doesn't start with 0x"
            assert len(addr) in [42, 66], f"Address {addr} is not a valid Ethereum address length"
            assert addr == addr.lower(), f"Address {addr} is not lowercase"

    @pytest.mark.asyncio 
    async def test_error_scenarios(self, processor):
        """Test various error scenarios."""
        
        # Test missing data directory
        result = await processor.process(data_dir="/nonexistent")
        assert result.success is False
        assert "Data directory not found" in result.error
        
        # Test empty data
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.glob', return_value=[]):
            result = await processor.process()
            assert result.success is True
            assert result.processed_count == 0
            assert len(result.data) == 0

    @pytest.mark.asyncio
    async def test_data_aggregation_logic(self, processor):
        """Test data aggregation and filtering logic."""
        
        # Create test data with different transfer counts
        transfer_data = [
            {"token_address": "0xToken1", "transaction_hash": "0xTx1", "from_address": "0xFrom1", "to_address": "0xTo1"},
            {"token_address": "0xToken1", "transaction_hash": "0xTx2", "from_address": "0xFrom2", "to_address": "0xTo2"},
            {"token_address": "0xToken1", "transaction_hash": "0xTx3", "from_address": "0xFrom3", "to_address": "0xTo3"},
            {"token_address": "0xToken2", "transaction_hash": "0xTx4", "from_address": "0xFrom4", "to_address": "0xTo4"},
        ]
        
        # Mock aggregation to return tokens with different counts
        mock_tokens = [
            {"token_address": "0xToken1", "transfer_count": 150, "unique_senders": 3, "unique_receivers": 3, "avg_transfers": 150.0},
            {"token_address": "0xToken2", "transfer_count": 50, "unique_senders": 1, "unique_receivers": 1, "avg_transfers": 50.0}
        ]
        
        with patch('src.processors.transfers.unified_transfer_processor.aggregate_raw_to_hourly'), \
             patch('src.processors.transfers.unified_transfer_processor.get_top_tokens_by_average', return_value=mock_tokens):
            
            # Test with min_transfers = 100 (should return only Token1)
            result = await processor._aggregate_and_get_top_tokens(transfer_data, min_transfers=100)
            
            assert len(result) == 1
            assert result[0]["token_address"] == "0xToken1"
            assert result[0]["transfer_count"] == 150

    def test_processor_initialization(self, processor):
        """Test processor initialization and configuration."""
        assert processor.chain == "ethereum"
        assert processor.protocol == "transfers"
        assert hasattr(processor, 'config')
        assert hasattr(processor, 'logger')

    @pytest.mark.asyncio
    async def test_cache_operations(self, processor):
        """Test Redis cache operations."""
        
        # Mock Redis client
        mock_redis = AsyncMock()
        test_tokens = [
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transfer_count": 100,
                "unique_senders": 50,
                "unique_receivers": 75,
                "avg_transfers": 85.5
            }
        ]
        
        with patch.object(processor, '_get_redis_client', return_value=mock_redis):
            
            # Test cache update
            await processor._update_redis_cache(test_tokens)
            
            # Verify Redis operations
            assert mock_redis.set.called
            assert mock_redis.hset.called  
            assert mock_redis.expire.called
            assert mock_redis.close.called
            
            # Test individual token stats
            mock_redis.hgetall.return_value = {
                "transfer_count": "100",
                "unique_senders": "50", 
                "unique_receivers": "75",
                "avg_transfers_24h": "85.5",
                "last_updated": "2024-01-01 12:00:00"
            }
            
            stats = await processor.get_token_stats("0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234")
            
            assert stats is not None
            assert stats["transfer_count"] == 100
            assert stats["avg_transfers_24h"] == 85.5

    def test_single_event_processing(self, processor):
        """Test single event processing (should return None for batch processor)."""
        result = processor._process_single_event({"test": "data"})
        assert result is None

    @pytest.mark.asyncio
    async def test_validation_and_config(self, processor):
        """Test configuration validation."""
        
        # Mock successful validation
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', return_value=mock_engine):
            assert processor.validate_config() is True
        
        # Mock failed validation
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', side_effect=Exception("Connection failed")):
            assert processor.validate_config() is False