"""Tests for base fetcher functionality."""
import pytest
from unittest.mock import AsyncMock
from src.fetchers.base import BaseFetcher, FetchResult, FetchError


class MockFetcher(BaseFetcher):
    """Mock fetcher for testing base functionality."""
    
    def __init__(self):
        super().__init__("test", "https://test-rpc.com")
    
    async def fetch_logs(self, from_block, to_block, contracts=None, **kwargs):
        """Mock fetch_logs implementation."""
        if from_block > to_block:
            raise FetchError("Invalid block range")
        
        return FetchResult(
            success=True,
            data_path="/tmp/test",
            fetched_blocks=to_block - from_block + 1,
            start_block=from_block,
            end_block=to_block,
            metadata={
                "from_block": from_block,
                "to_block": to_block,
                "chain": self.chain
            }
        )
    
    async def get_latest_block(self) -> int:
        """Mock get_latest_block implementation."""
        return 18500000
    
    def validate_config(self) -> bool:
        """Mock validate_config implementation."""
        return True


class TestBaseFetcher:
    """Test cases for BaseFetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a mock fetcher instance."""
        return MockFetcher()
    
    def test_fetcher_initialization(self, fetcher):
        """Test fetcher initializes correctly."""
        assert fetcher.chain == "test"
        assert fetcher.rpc_url == "https://test-rpc.com"
        assert fetcher.logger is not None
    
    @pytest.mark.asyncio
    async def test_fetch_logs_success(self, fetcher):
        """Test successful log fetching."""
        result = await fetcher.fetch_logs(18000000, 18000100)
        
        assert result.success is True
        assert result.data_path == "/tmp/test"
        assert result.fetched_blocks == 101
        assert result.start_block == 18000000
        assert result.end_block == 18000100
        assert result.metadata["from_block"] == 18000000
        assert result.metadata["to_block"] == 18000100
        assert result.metadata["chain"] == "test"
    
    @pytest.mark.asyncio
    async def test_fetch_logs_invalid_range(self, fetcher):
        """Test fetch_logs with invalid block range."""
        with pytest.raises(FetchError, match="Invalid block range"):
            await fetcher.fetch_logs(18000100, 18000000)
    
    def test_fetch_result_dataclass(self):
        """Test FetchResult dataclass functionality."""
        result = FetchResult(
            success=True,
            data_path="/tmp/test",
            fetched_blocks=100,
            start_block=18000000,
            end_block=18000100,
            error="No error",
            metadata={"key": "value"}
        )
        
        assert result.success is True
        assert result.data_path == "/tmp/test"
        assert result.fetched_blocks == 100
        assert result.start_block == 18000000
        assert result.end_block == 18000100
        assert result.metadata == {"key": "value"}
        assert result.error == "No error"
        assert result.failed is False
    
    def test_fetch_error_exception(self):
        """Test FetchError exception."""
        error = FetchError("Test error message")
        assert str(error) == "Test error message"
        assert isinstance(error, Exception)