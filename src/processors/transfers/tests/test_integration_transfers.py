"""
Comprehensive integration tests for transfer processing.

Tests the full pipeline: Parquet → TimescaleDB → Redis → API
"""

import pytest
import tempfile
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
import os

from ..unified_transfer_processor import UnifiedTransferProcessor


class TestTransferProcessingIntegration:
    """Integration tests for complete transfer processing pipeline."""

    @pytest.fixture
    def sample_transfer_data(self):
        """Create sample transfer data for testing."""
        return [
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transaction_hash": "0x1234567890abcdef1234567890abcdef12345678",
                "from_address": "0x742d35cc6e3c014c41ba3ab0b41f2e2e5e9e5678",
                "to_address": "0x8ba1f109551bd432803012645heff07b94090123",
                "value": 1000000000000000000,  # 1 ETH in wei
                "timestamp": datetime.now() - timedelta(hours=1),
                "block_number": 18500000
            },
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transaction_hash": "0x2345678901bcdef12345678901cdef1234567890",
                "from_address": "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",  # MEV address
                "to_address": "0x9ba1f109551bd432803012645heff07b94090456",
                "value": 500000000000000000,  # 0.5 ETH in wei
                "timestamp": datetime.now() - timedelta(minutes=30),
                "block_number": 18500001
            },
            {
                "token_address": "0xb0c86a33e6441e53a0f1e87f58f8fc87f7Ff5678",
                "transaction_hash": "0x3456789012cdef123456789012def12345678901",
                "from_address": "0x852d35cc6e3c014c41ba3ab0b41f2e2e5e9e9012",
                "to_address": "0xaba1f109551bd432803012645heff07b94090789",
                "value": 2000000000000000000,  # 2 ETH in wei
                "timestamp": datetime.now() - timedelta(minutes=15),
                "block_number": 18500002
            }
        ]

    @pytest.fixture
    def temp_parquet_file(self, sample_transfer_data):
        """Create temporary parquet file with transfer data."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df = pl.DataFrame(sample_transfer_data)
            df.write_parquet(f.name)
            yield Path(f.name)
            # Cleanup
            try:
                os.unlink(f.name)
            except FileNotFoundError:
                pass

    @pytest.fixture
    def mock_unified_processor(self):
        """Create mock unified processor for testing."""
        with patch('src.config.ConfigManager'), \
             patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine'), \
             patch('src.processors.transfers.unified_transfer_processor.setup_timescale_tables'):
            processor = UnifiedTransferProcessor("ethereum")
            return processor

    @pytest.mark.asyncio
    async def test_full_processing_pipeline(self, mock_unified_processor, temp_parquet_file):
        """Test complete processing pipeline: Parquet → Processing → Results."""
        
        # Mock the data directory to return our test parquet file
        temp_dir = temp_parquet_file.parent
        
        with patch.object(mock_unified_processor, '_store_raw_data', return_value=2) as mock_store, \
             patch.object(mock_unified_processor, '_aggregate_and_get_top_tokens') as mock_aggregate, \
             patch.object(mock_unified_processor, '_update_redis_cache') as mock_cache, \
             patch('pathlib.Path.glob', return_value=[temp_parquet_file]):
            
            # Setup expected aggregation results
            expected_tokens = [
                {
                    "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                    "transfer_count": 2,
                    "unique_senders": 2,
                    "unique_receivers": 2,
                    "avg_transfers": 2.0,
                    "chain": "ethereum"
                },
                {
                    "token_address": "0xb0c86a33e6441e53a0f1e87f58f8fc87f7Ff5678",
                    "transfer_count": 1,
                    "unique_senders": 1,
                    "unique_receivers": 1,
                    "avg_transfers": 1.0,
                    "chain": "ethereum"
                }
            ]
            mock_aggregate.return_value = expected_tokens
            
            # Run processing
            result = await mock_unified_processor.process(
                data_dir=str(temp_dir),
                hours_back=24,
                min_transfers=1,
                store_raw=True,
                update_cache=True
            )
            
            # Verify results
            assert result.success is True
            assert result.processed_count == 2  # 2 raw records stored
            assert len(result.data) == 2
            assert result.data[0]["token_address"] == "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234"
            
            # Verify pipeline steps were called
            mock_store.assert_called_once()
            mock_aggregate.assert_called_once()
            mock_cache.assert_called_once_with(expected_tokens)

    @pytest.mark.asyncio
    async def test_mev_analysis_integration(self, mock_unified_processor):
        """Test MEV analysis functionality."""
        
        # Mock database connection and results
        mock_engine = Mock()
        mock_conn = Mock()
        mock_result = Mock()
        
        # Setup MEV query results
        mock_row = Mock()
        mock_row.token_address = "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234"
        mock_row.mev_transfer_count = 5
        mock_row.unique_mev_senders = 2
        mock_row.unique_receivers = 3
        mock_row.total_mev_volume = 1500000000000000000  # 1.5 ETH
        mock_row.first_seen = datetime.now() - timedelta(hours=2)
        mock_row.last_seen = datetime.now() - timedelta(minutes=10)
        
        mock_result.__iter__ = Mock(return_value=iter([mock_row]))
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', return_value=mock_engine):
            
            mev_tokens = await mock_unified_processor.get_mev_active_tokens(
                hours_back=24,
                limit=10
            )
            
            assert len(mev_tokens) == 1
            token = mev_tokens[0]
            assert token["token_address"] == "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234"
            assert token["mev_transfer_count"] == 5
            assert token["total_mev_volume"] == 1.5e18
            assert token["chain"] == "ethereum"
            
            # Verify MEV addresses are loaded correctly
            mev_addresses = mock_unified_processor._load_mev_addresses()
            assert "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387" in mev_addresses

    @pytest.mark.asyncio
    async def test_database_stats_integration(self, mock_unified_processor):
        """Test database statistics functionality."""
        
        # Mock database stats results
        mock_engine = Mock()
        mock_conn = Mock()
        
        # Mock table sizes
        size_row1 = Mock()
        size_row1.schemaname = "public"
        size_row1.tablename = "token_raw_transfers"
        size_row1.size = "150 MB"
        size_row1.size_bytes = 157286400
        
        size_row2 = Mock()
        size_row2.schemaname = "public"
        size_row2.tablename = "token_hourly_transfers"
        size_row2.size = "50 MB"
        size_row2.size_bytes = 52428800
        
        # Mock data age
        raw_age_row = Mock()
        raw_age_row.oldest = datetime.now() - timedelta(days=6)
        raw_age_row.newest = datetime.now()
        raw_age_row.total_records = 50000
        
        hourly_age_row = Mock()
        hourly_age_row.oldest = datetime.now() - timedelta(days=60)
        hourly_age_row.newest = datetime.now()
        hourly_age_row.total_records = 2000
        
        # Mock compression stats
        compression_row = Mock()
        compression_row.hypertable_name = "token_hourly_transfers"
        compression_row.compression_enabled = True
        compression_row.compressed_heap_size = 10000000
        compression_row.uncompressed_heap_size = 50000000
        
        # Setup mock responses
        mock_conn.execute.side_effect = [
            Mock(fetchall=Mock(return_value=[size_row1, size_row2])),  # table sizes
            Mock(fetchone=Mock(return_value=raw_age_row)),             # raw age
            Mock(fetchone=Mock(return_value=hourly_age_row)),          # hourly age
            Mock(fetchall=Mock(return_value=[compression_row]))        # compression
        ]
        
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', return_value=mock_engine):
            
            stats = await mock_unified_processor.get_database_stats()
            
            assert "error" not in stats
            assert len(stats["table_sizes"]) == 2
            assert stats["table_sizes"][0]["table"] == "token_raw_transfers"
            assert stats["table_sizes"][0]["size_pretty"] == "150 MB"
            
            assert stats["raw_data"]["total_records"] == 50000
            assert stats["raw_data"]["retention_days"] == 7
            
            assert stats["hourly_data"]["total_records"] == 2000
            assert stats["hourly_data"]["retention_days"] == 90
            
            assert len(stats["compression"]) == 1
            assert stats["compression"][0]["compression_ratio"] == 5.0  # 50M/10M

    @pytest.mark.asyncio
    async def test_redis_caching_integration(self, mock_unified_processor):
        """Test Redis caching functionality."""
        
        # Mock Redis client
        mock_redis = AsyncMock()
        
        # Test data
        test_tokens = [
            {
                "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234",
                "transfer_count": 100,
                "unique_senders": 50,
                "unique_receivers": 75,
                "avg_transfers": 85.5
            }
        ]
        
        with patch.object(mock_unified_processor, '_get_redis_client', return_value=mock_redis):
            
            # Test cache update
            await mock_unified_processor._update_redis_cache(test_tokens)
            
            # Verify Redis operations
            mock_redis.set.assert_called_once()
            mock_redis.hset.assert_called_once()
            mock_redis.expire.assert_called_once()
            mock_redis.close.assert_called_once()
            
            # Test cache retrieval
            mock_redis.get.return_value = '[{"token_address":"0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234","transfer_count":100}]'
            
            with patch('polars.read_json') as mock_read_json:
                mock_df = pl.DataFrame([{"token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234", "transfer_count": 100}])
                mock_read_json.return_value = mock_df
                
                cached_tokens = await mock_unified_processor.get_cached_top_tokens()
                
                assert len(cached_tokens) == 1
                assert cached_tokens[0]["token_address"] == "0xa0b86a33e6441e53a0f1e87f58f8fc87f7Ff1234"

    @pytest.mark.asyncio
    async def test_error_handling_integration(self, mock_unified_processor):
        """Test error handling in integration scenarios."""
        
        # Test with non-existent data directory
        result = await mock_unified_processor.process(data_dir="/nonexistent/path")
        assert result.success is False
        assert "Data directory not found" in result.error
        
        # Test MEV analysis with database error
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', side_effect=Exception("DB connection failed")):
            mev_tokens = await mock_unified_processor.get_mev_active_tokens()
            assert mev_tokens == []
        
        # Test database stats with connection error
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', side_effect=Exception("Connection error")):
            stats = await mock_unified_processor.get_database_stats()
            assert "error" in stats
            assert "Connection error" in stats["error"]

    @pytest.mark.asyncio
    async def test_data_validation_integration(self, mock_unified_processor, sample_transfer_data):
        """Test data validation throughout the pipeline."""
        
        # Mock the store_raw_transfers function to avoid actual DB calls
        with patch('src.processors.transfers.unified_transfer_processor.store_raw_transfers') as mock_store:
            
            # Test with invalid transfer data (missing required fields)
            invalid_data = [
                {
                    "token_address": "0xinvalid",
                    "transaction_hash": "0xinvalid",
                    "from_address": "0xinvalid",
                    "to_address": "0xinvalid",
                    "value": 0
                    # Minimal data for DataFrame creation
                }
            ]
            
            # Should handle gracefully without crashing
            result = await mock_unified_processor._store_raw_data(invalid_data)
            assert isinstance(result, int)  # Should return count (possibly 0)
            
            # Test with valid data
            valid_result = await mock_unified_processor._store_raw_data(sample_transfer_data)
            assert valid_result >= 0
            
            # Verify store_raw_transfers was called
            assert mock_store.call_count >= 1

    def test_mev_addresses_loading(self, mock_unified_processor):
        """Test MEV addresses are loaded correctly."""
        mev_addresses = mock_unified_processor._load_mev_addresses()
        
        assert isinstance(mev_addresses, set)
        assert len(mev_addresses) > 0
        
        # Check Jared's address is included
        jared_addr = "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387"
        assert jared_addr in mev_addresses
        
        # All addresses should be lowercase
        for addr in mev_addresses:
            assert addr == addr.lower()
            assert addr.startswith("0x")

    @pytest.mark.asyncio
    async def test_configuration_validation(self, mock_unified_processor):
        """Test configuration validation."""
        
        # Test with valid config (mocked TimescaleDB connection)
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', return_value=mock_engine):
            assert mock_unified_processor.validate_config() is True
        
        # Test with invalid config (connection fails)
        with patch('src.processors.transfers.unified_transfer_processor.get_timescale_engine', side_effect=Exception("Connection failed")):
            assert mock_unified_processor.validate_config() is False