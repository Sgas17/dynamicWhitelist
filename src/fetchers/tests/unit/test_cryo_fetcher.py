"""Tests for cryo fetcher functionality."""
import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from pathlib import Path
from ....fetchers.cryo_fetcher import CryoFetcher
from ....fetchers.base import FetchError, FetchResult


class TestCryoFetcher:
    """Test cases for CryoFetcher."""
    
    @pytest.fixture
    def cryo_fetcher(self):
        """Create a CryoFetcher instance."""
        return CryoFetcher("ethereum", "https://eth.llamarpc.com")
    
    def test_cryo_fetcher_initialization(self, cryo_fetcher):
        """Test cryo fetcher initializes correctly."""
        assert cryo_fetcher.chain == "ethereum"
        assert cryo_fetcher.rpc_url == "https://eth.llamarpc.com"
        assert cryo_fetcher.config is not None
        assert cryo_fetcher.blocks_per_request == 10000
        assert isinstance(cryo_fetcher.cryo_options, list)
    
    @patch('subprocess.run')
    def test_validate_config_success(self, mock_run, cryo_fetcher):
        """Test successful cryo config validation."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "cryo 0.1.0"
        mock_run.return_value = mock_result
        
        result = cryo_fetcher.validate_config()
        
        assert result is True
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[0] == "cryo"
        assert "--version" in call_args
    
    @patch('subprocess.run')
    def test_validate_config_failure(self, mock_run, cryo_fetcher):
        """Test failed cryo config validation."""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_run.return_value = mock_result
        
        result = cryo_fetcher.validate_config()
        
        assert result is False
    
    @patch('subprocess.run')
    def test_validate_config_command_not_found(self, mock_run, cryo_fetcher):
        """Test cryo command not found."""
        mock_run.side_effect = FileNotFoundError("cryo command not found")
        
        result = cryo_fetcher.validate_config()
        
        assert result is False
    
    @patch('asyncio.create_subprocess_exec')
    @pytest.mark.asyncio
    async def test_get_latest_block_success(self, mock_subprocess, cryo_fetcher):
        """Test successful latest block retrieval."""
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (b'18500000\n', b'')
        mock_subprocess.return_value = mock_process
        
        result = await cryo_fetcher.get_latest_block()
        
        assert result == 18500000
        mock_subprocess.assert_called_once()
    
    @patch('asyncio.create_subprocess_exec')
    @pytest.mark.asyncio
    async def test_get_latest_block_failure(self, mock_subprocess, cryo_fetcher):
        """Test failed latest block retrieval."""
        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_process.communicate.return_value = (b'', b'RPC error\n')
        mock_subprocess.return_value = mock_process
        
        with pytest.raises(FetchError, match="Failed to get latest block"):
            await cryo_fetcher.get_latest_block()
    
    @patch('asyncio.create_subprocess_exec')
    @patch('subprocess.run')
    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.glob')
    @pytest.mark.asyncio
    async def test_fetch_logs_success(self, mock_glob, mock_mkdir, mock_run, mock_subprocess, cryo_fetcher):
        """Test successful log fetching."""
        # Mock validation
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "cryo 0.1.0"
        mock_run.return_value = mock_result
        
        # Mock cryo execution
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (b'Success', b'')
        mock_subprocess.return_value = mock_process
        
        # Mock file system
        mock_glob.return_value = [Path("/tmp/test.parquet")]
        
        result = await cryo_fetcher.fetch_logs(18000000, 18000100)
        
        assert result.success is True
        assert result.start_block == 18000000
        assert result.end_block == 18000100
        assert result.fetched_blocks == 101
        mock_subprocess.assert_called_once()
    
    @patch('asyncio.create_subprocess_exec')
    @patch('subprocess.run')
    @pytest.mark.asyncio
    async def test_fetch_logs_validation_failure(self, mock_run, mock_subprocess, cryo_fetcher):
        """Test log fetching with validation failure."""
        # Mock failed validation
        mock_result = Mock()
        mock_result.returncode = 1
        mock_run.return_value = mock_result
        
        result = await cryo_fetcher.fetch_logs(18000000, 18000100)
        
        assert result.success is False
        assert "validation failed" in result.error
        mock_subprocess.assert_not_called()
    
    @patch('asyncio.create_subprocess_exec')
    @patch('subprocess.run')
    @pytest.mark.asyncio
    async def test_fetch_logs_cryo_failure(self, mock_run, mock_subprocess, cryo_fetcher):
        """Test log fetching with cryo command failure."""
        # Mock successful validation
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "cryo 0.1.0"
        mock_run.return_value = mock_result
        
        # Mock failed cryo execution
        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_process.communicate.return_value = (b'', b'Cryo error\n')
        mock_subprocess.return_value = mock_process
        
        result = await cryo_fetcher.fetch_logs(18000000, 18000100)
        
        assert result.success is False
        assert "Cryo command failed" in result.error
    
    @patch.object(CryoFetcher, 'get_latest_block')
    @patch.object(CryoFetcher, 'fetch_logs')
    @pytest.mark.asyncio
    async def test_fetch_transfers(self, mock_fetch_logs, mock_get_latest_block, cryo_fetcher):
        """Test transfer fetching."""
        # Mock latest block
        mock_get_latest_block.return_value = 18500000
        
        # Mock successful log fetch
        mock_result = FetchResult(
            success=True,
            data_path="/tmp/transfers",
            fetched_blocks=100,
            start_block=18499700,
            end_block=18500000
        )
        mock_fetch_logs.return_value = mock_result
        
        result = await cryo_fetcher.fetch_transfers(hours_back=1)
        
        assert result.success is True
        mock_get_latest_block.assert_called_once()
        mock_fetch_logs.assert_called()
    
    @patch.object(CryoFetcher, 'get_latest_block')
    @patch.object(CryoFetcher, 'fetch_logs')
    @pytest.mark.asyncio
    async def test_fetch_pool_events(self, mock_fetch_logs, mock_get_latest_block, cryo_fetcher):
        """Test pool event fetching."""
        # Mock latest block
        mock_get_latest_block.return_value = 18500000
        
        # Mock successful log fetch
        mock_result = FetchResult(
            success=True,
            data_path="/tmp/pools",
            fetched_blocks=6000000,
            start_block=12369621,
            end_block=18500000
        )
        mock_fetch_logs.return_value = mock_result
        
        result = await cryo_fetcher.fetch_pool_events("uniswap_v3")
        
        assert result.success is True
        mock_get_latest_block.assert_called_once()
        mock_fetch_logs.assert_called()
    
    def test_cryo_options_format(self, cryo_fetcher):
        """Test cryo options are properly formatted."""
        options = cryo_fetcher.cryo_options
        
        assert "--rpc" in options
        assert cryo_fetcher.rpc_url in options
        assert "--inner-request-size" in options
        assert str(cryo_fetcher.blocks_per_request) in options
        assert "--u256-types" in options
        assert "binary" in options