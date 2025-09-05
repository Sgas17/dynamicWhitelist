"""
Tests for transfer processor functionality.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path
import polars as pl

from ..transfer_processors import LatestTransfersProcessor
from ..base import ProcessorResult


class TestLatestTransfersProcessor:
    """Test latest transfers processor functionality."""
    
    def test_initialization(self):
        """Test processor initialization."""
        processor = LatestTransfersProcessor("ethereum")
        
        assert processor.chain == "ethereum"
        assert processor.protocol == "transfers"
        assert processor.get_identifier() == "ethereum_transfers_processor"
    
    def test_validate_config(self):
        """Test config validation (always passes)."""
        processor = LatestTransfersProcessor("ethereum")
        
        result = processor.validate_config()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_process_no_files(self):
        """Test processing with no transfer files."""
        processor = LatestTransfersProcessor("ethereum")
        
        with patch('pathlib.Path.glob') as mock_glob:
            mock_glob.return_value = []  # No files found
            
            result = await processor.process(
                data_dir="/tmp/test_data",
                hours_back=24,
                min_transfers=100
            )
        
        assert result.success is True
        assert result.processed_count == 0
        assert len(result.data) == 0
        assert "No qualifying tokens found" in result.metadata["message"]
    
    @pytest.mark.asyncio
    async def test_get_top_tokens_by_transfers(self):
        """Test getting top tokens from transfer data."""
        processor = LatestTransfersProcessor("ethereum")
        data_dir = Path("/tmp/test")
        
        # Mock transfer data
        mock_df = pl.DataFrame({
            "token_address": ["0x123", "0x123", "0x456", "0x456", "0x456", "0x789"],
            "transaction_hash": ["tx1", "tx2", "tx3", "tx4", "tx5", "tx6"],
            "from_address": ["0xaaa", "0xbbb", "0xccc", "0xddd", "0xeee", "0xfff"],
            "to_address": ["0x111", "0x222", "0x333", "0x444", "0x555", "0x666"]
        })
        
        with patch('pathlib.Path.glob') as mock_glob:
            with patch('polars.read_parquet') as mock_read:
                mock_glob.return_value = [Path("/tmp/transfers_1.parquet")]
                mock_read.return_value = mock_df
                
                tokens = await processor._get_top_tokens_by_transfers(
                    data_dir, hours_back=24, min_transfers=2
                )
        
        assert len(tokens) == 2  # Only tokens with >= 2 transfers
        
        # Check token with most transfers is first
        assert tokens[0]["token_address"] == "0x456"
        assert tokens[0]["transfer_count"] == 3
        assert tokens[0]["unique_senders"] == 3
        assert tokens[0]["unique_receivers"] == 3
        
        assert tokens[1]["token_address"] == "0x123"
        assert tokens[1]["transfer_count"] == 2
    
    @pytest.mark.asyncio 
    async def test_update_redis_cache(self):
        """Test Redis cache update functionality."""
        processor = LatestTransfersProcessor("ethereum")
        
        tokens = [
            {
                "token_address": "0x123",
                "transfer_count": 100,
                "unique_senders": 50,
                "unique_receivers": 75
            },
            {
                "token_address": "0x456", 
                "transfer_count": 200,
                "unique_senders": 80,
                "unique_receivers": 120
            }
        ]
        
        # Mock Redis client
        mock_redis = AsyncMock()
        mock_pipeline = AsyncMock()
        mock_redis.pipeline.return_value = mock_pipeline
        
        with patch.object(processor, '_get_redis_client', return_value=mock_redis):
            await processor._update_redis_cache(tokens)
        
        # Verify Redis operations
        assert mock_redis.hset.call_count == 2  # One for each token
        assert mock_redis.expire.call_count == 2  # One for each token  
        assert mock_redis.close.called
    
    @pytest.mark.asyncio
    async def test_process_full_pipeline(self):
        """Test complete processing pipeline."""
        processor = LatestTransfersProcessor("ethereum")
        
        # Mock successful token processing
        mock_tokens = [
            {
                "token_address": "0x123",
                "transfer_count": 150,
                "unique_senders": 75,
                "unique_receivers": 80,
                "chain": "ethereum"
            }
        ]
        
        with patch.object(processor, '_get_top_tokens_by_transfers', return_value=mock_tokens):
            with patch.object(processor, '_update_redis_cache') as mock_cache_update:
                result = await processor.process(
                    data_dir="/tmp/test",
                    hours_back=24,
                    min_transfers=100
                )
        
        assert result.success is True
        assert result.processed_count == 1
        assert len(result.data) == 1
        assert result.data[0]["token_address"] == "0x123"
        assert result.metadata["hours_back"] == 24
        assert result.metadata["min_transfers"] == 100
        assert mock_cache_update.called
    
    @pytest.mark.asyncio
    async def test_process_error_handling(self):
        """Test error handling during processing."""
        processor = LatestTransfersProcessor("ethereum")
        
        # Mock method that raises exception
        with patch.object(processor, '_get_top_tokens_by_transfers', side_effect=Exception("Test error")):
            result = await processor.process()
        
        assert result.success is False
        assert "Transfer processing failed: Test error" in result.error