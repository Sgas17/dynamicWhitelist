"""
Tests for Unified Transfer Processor
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timedelta
import polars as pl

from ..unified_transfer_processor import UnifiedTransferProcessor
from ...base import ProcessorResult


class TestUnifiedTransferProcessor:
    """Test cases for Unified Transfer Processor."""
    
    @pytest.fixture
    def unified_processor(self):
        """Create unified processor for testing."""
        with patch('src.config.ConfigManager'), \
             patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine'), \
             patch('src.processors.transfers.unified_transfer_processor.setup_timescale_tables'):
            processor = UnifiedTransferProcessor("ethereum")
            return processor
    
    def test_init(self, unified_processor):
        """Test processor initialization."""
        assert unified_processor.chain == "ethereum"
        assert unified_processor.protocol == "transfers"
    
    def test_validate_config_valid(self, unified_processor):
        """Test configuration validation with valid config."""
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.return_value.connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.return_value.connect.return_value.__exit__ = Mock(return_value=None)
            
            assert unified_processor.validate_config() is True
    
    def test_validate_config_invalid(self, unified_processor):
        """Test configuration validation with invalid config."""
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', side_effect=Exception("Connection failed")):
            assert unified_processor.validate_config() is False
    
    @pytest.mark.asyncio
    async def test_process_no_data_dir(self, unified_processor):
        """Test processing with missing data directory."""
        with patch('pathlib.Path.exists', return_value=False):
            result = await unified_processor.process(data_dir="/nonexistent")
            
            assert result.success is False
            assert "Data directory not found" in result.error
    
    @pytest.mark.asyncio
    async def test_process_no_transfer_files(self, unified_processor):
        """Test processing with no transfer files."""
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.glob', return_value=[]):
            result = await unified_processor.process()
            
            assert result.success is True
            assert result.processed_count == 0
            assert result.data == []
    
    @pytest.mark.asyncio
    async def test_load_transfer_data(self, unified_processor):
        """Test loading transfer data from parquet files."""
        # Mock parquet files
        mock_file1 = Mock()
        mock_file1.name = "transfers_1.parquet"
        mock_file2 = Mock()
        mock_file2.name = "transfers_2.parquet"
        
        # Mock transfer data
        mock_df1 = pl.DataFrame({
            "token_address": ["0xToken1", "0xToken2"],
            "transaction_hash": ["0xTx1", "0xTx2"],
            "from_address": ["0xFrom1", "0xFrom2"],
            "to_address": ["0xTo1", "0xTo2"],
            "value": [100, 200],
            "timestamp": [datetime.now(), datetime.now()]
        })
        
        mock_df2 = pl.DataFrame({
            "token_address": ["0xToken3"],
            "transaction_hash": ["0xTx3"],
            "from_address": ["0xFrom3"],
            "to_address": ["0xTo3"],
            "value": [300],
            "timestamp": [datetime.now()]
        })
        
        mock_data_dir = Mock()
        mock_data_dir.glob.return_value = [mock_file1, mock_file2]
        
        with patch('polars.read_parquet', side_effect=[mock_df1, mock_df2]):
            
            result = await unified_processor._load_transfer_data(
                mock_data_dir, hours_back=24
            )
            
            assert len(result) == 3
            assert all("token_address" in record for record in result)
    
    @pytest.mark.asyncio
    async def test_store_raw_data(self, unified_processor):
        """Test storing raw transfer data."""
        transfer_data = [
            {
                "token_address": "0xToken1",
                "transaction_hash": "0xTx1",
                "from_address": "0xFrom1",
                "to_address": "0xTo1",
                "value": 100
            },
            {
                "token_address": "0xToken1",
                "transaction_hash": "0xTx2",
                "from_address": "0xFrom2", 
                "to_address": "0xTo2",
                "value": 200
            }
        ]
        
        with patch('src.processors.transfers.unified_transfer_processor.store_raw_transfers') as mock_store:
            result = await unified_processor._store_raw_data(transfer_data)
            
            assert result == 1  # 1 unique token
            mock_store.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_aggregate_and_get_top_tokens(self, unified_processor):
        """Test aggregation and getting top tokens."""
        transfer_data = [
            {
                "token_address": "0xToken1",
                "transaction_hash": "0xTx1",
                "from_address": "0xFrom1",
                "to_address": "0xTo1"
            }
        ]
        
        mock_top_tokens = [
            {
                "token_address": "0xToken1",
                "transfer_count": 150,
                "unique_senders": 10,
                "unique_receivers": 15,
                "avg_transfers": 125.5
            }
        ]
        
        with patch('src.processors.transfers.unified_transfer_processor.aggregate_raw_to_hourly'), \
             patch('src.processors.transfers.unified_transfer_processor.get_top_tokens_by_average', return_value=mock_top_tokens):
            
            result = await unified_processor._aggregate_and_get_top_tokens(
                transfer_data, min_transfers=100
            )
            
            assert len(result) == 1
            assert result[0]["token_address"] == "0xToken1"
            assert result[0]["transfer_count"] == 150
    
    @pytest.mark.asyncio
    async def test_update_redis_cache(self, unified_processor):
        """Test updating Redis cache."""
        tokens = [
            {
                "token_address": "0xToken1",
                "transfer_count": 150,
                "unique_senders": 10,
                "unique_receivers": 15,
                "avg_transfers": 125.5
            }
        ]
        
        mock_redis = AsyncMock()
        with patch.object(unified_processor, '_get_redis_client', return_value=mock_redis):
            await unified_processor._update_redis_cache(tokens)
            
            # Verify Redis operations were called
            mock_redis.set.assert_called_once()
            mock_redis.hset.assert_called_once()
            mock_redis.expire.assert_called_once()
            mock_redis.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cached_top_tokens(self, unified_processor):
        """Test getting cached top tokens."""
        # Use a simple JSON string instead of Polars write_json
        cached_data = '[{"token_address":"0xToken1","transfer_count":150}]'
        
        mock_redis = AsyncMock()
        mock_redis.get.return_value = cached_data
        
        with patch('polars.read_json') as mock_read_json:
            mock_df = pl.DataFrame([{"token_address": "0xToken1", "transfer_count": 150}])
            mock_read_json.return_value = mock_df
            
            with patch.object(unified_processor, '_get_redis_client', return_value=mock_redis):
                result = await unified_processor.get_cached_top_tokens()
                
                assert len(result) == 1
                assert result[0]["token_address"] == "0xToken1"
                mock_redis.get.assert_called_once()
                mock_redis.close.assert_called_once()
                mock_read_json.assert_called_once_with(cached_data)
    
    @pytest.mark.asyncio
    async def test_get_token_stats(self, unified_processor):
        """Test getting token statistics."""
        mock_stats = {
            "transfer_count": "150",
            "unique_senders": "10",
            "unique_receivers": "15",
            "avg_transfers_24h": "125.5",
            "last_updated": "2024-01-01 12:00:00"
        }
        
        mock_redis = AsyncMock()
        mock_redis.hgetall.return_value = mock_stats
        
        with patch.object(unified_processor, '_get_redis_client', return_value=mock_redis):
            result = await unified_processor.get_token_stats("0xToken1")
            
            assert result is not None
            assert result["token_address"] == "0xToken1"
            assert result["transfer_count"] == 150
            assert result["avg_transfers_24h"] == 125.5
            mock_redis.hgetall.assert_called_once()
            mock_redis.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_full_process_workflow(self, unified_processor):
        """Test complete processing workflow."""
        # Mock all external dependencies
        mock_files = [Mock()]
        mock_df = pl.DataFrame({
            "token_address": ["0xToken1"],
            "transaction_hash": ["0xTx1"],
            "from_address": ["0xFrom1"],
            "to_address": ["0xTo1"],
            "value": [100],
            "timestamp": [datetime.now()]
        })
        
        mock_top_tokens = [
            {
                "token_address": "0xToken1",
                "transfer_count": 150,
                "unique_senders": 10,
                "unique_receivers": 15,
                "avg_transfers": 125.5
            }
        ]
        
        with patch('pathlib.Path.exists', return_value=True), \
             patch('pathlib.Path.glob', return_value=mock_files), \
             patch('polars.read_parquet', return_value=mock_df), \
             patch('src.processors.transfers.unified_transfer_processor.store_raw_transfers'), \
             patch('src.processors.transfers.unified_transfer_processor.aggregate_raw_to_hourly'), \
             patch('src.processors.transfers.unified_transfer_processor.get_top_tokens_by_average', return_value=mock_top_tokens), \
             patch.object(unified_processor, '_update_redis_cache', new_callable=AsyncMock):
            
            result = await unified_processor.process(min_transfers=100)
            
            assert result.success is True
            assert len(result.data) == 1
            assert result.data[0]["token_address"] == "0xToken1"
            assert result.processed_count == 1  # 1 raw record stored
    
    def test_process_single_event_not_used(self, unified_processor):
        """Test that _process_single_event returns None (not used in this processor)."""
        result = unified_processor._process_single_event({"test": "data"})
        assert result is None