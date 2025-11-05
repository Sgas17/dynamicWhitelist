"""Tests for Ethereum-specific fetcher functionality."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ....fetchers.base import FetchResult
from ....fetchers.ethereum_fetcher import EthereumFetcher


class TestEthereumFetcher:
    """Test cases for EthereumFetcher."""

    @pytest.fixture
    def ethereum_fetcher(self):
        """Create an EthereumFetcher instance."""
        return EthereumFetcher("https://eth.llamarpc.com")

    def test_ethereum_fetcher_initialization(self, ethereum_fetcher):
        """Test Ethereum fetcher initializes correctly."""
        assert ethereum_fetcher.chain == "ethereum"
        assert ethereum_fetcher.blocks_per_minute == 5
        assert ethereum_fetcher.blocks_per_request == 10000
        assert ethereum_fetcher.rpc_url == "https://eth.llamarpc.com"

    @pytest.mark.asyncio
    async def test_calculate_block_range(self, ethereum_fetcher):
        """Test block range calculation."""
        from_block, to_block = await ethereum_fetcher.calculate_block_range(1)  # 1 hour

        expected_blocks = (
            60 * ethereum_fetcher.blocks_per_minute
        )  # 300 blocks (1 hour * 60 minutes * 5 blocks/minute)
        assert (to_block - from_block) == expected_blocks
        assert isinstance(from_block, int)
        assert isinstance(to_block, int)

    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.fetch_logs")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.get_latest_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._get_last_processed_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._cleanup_last_file")
    @pytest.mark.asyncio
    async def test_fetch_uniswap_v3_pools(
        self,
        mock_cleanup,
        mock_get_last_block,
        mock_get_latest,
        mock_fetch_logs,
        ethereum_fetcher,
    ):
        """Test Uniswap V3 pool fetching."""
        # Mock dependencies
        mock_get_latest.return_value = 18000000
        mock_get_last_block.return_value = None  # No checkpoint
        mock_cleanup.return_value = True

        mock_result = FetchResult(
            success=True,
            data_path="/tmp/uniswap_v3_pools",
            fetched_blocks=6000000,
            start_block=12369621,
            end_block=18000000,
        )
        mock_fetch_logs.return_value = mock_result

        result = await ethereum_fetcher.fetch_uniswap_v3_pools()

        assert result.success is True
        assert result.metadata["protocol"] == "uniswap_v3"
        assert result.metadata["deployment_block"] == 12369621

        # Verify correct contracts were used
        mock_fetch_logs.assert_called_once()
        call_args = mock_fetch_logs.call_args
        contracts = call_args.kwargs["contracts"]
        assert len(contracts) == 3  # Uniswap, Sushiswap, PancakeSwap V3 factories
        assert "0x1F98431c8aD98523631AE4a59f267346ea31F984" in contracts  # Uniswap V3

    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.fetch_logs")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.get_latest_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._get_last_processed_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._cleanup_last_file")
    @pytest.mark.asyncio
    async def test_fetch_uniswap_v4_pools(
        self,
        mock_cleanup,
        mock_get_last_block,
        mock_get_latest,
        mock_fetch_logs,
        ethereum_fetcher,
    ):
        """Test Uniswap V4 pool fetching."""
        # Mock dependencies
        mock_get_latest.return_value = 23000000
        mock_get_last_block.return_value = None  # No checkpoint
        mock_cleanup.return_value = True

        mock_result = FetchResult(
            success=True,
            data_path="/tmp/uniswap_v4_pools",
            fetched_blocks=1300000,
            start_block=21688329,
            end_block=23000000,
        )
        mock_fetch_logs.return_value = mock_result

        result = await ethereum_fetcher.fetch_uniswap_v4_pools()

        assert result.success is True
        assert result.metadata["protocol"] == "uniswap_v4"
        assert result.metadata["deployment_block"] == 21688329

        # Verify correct V4 pool manager contract was used
        mock_fetch_logs.assert_called_once()
        call_args = mock_fetch_logs.call_args
        contracts = call_args.kwargs["contracts"]
        assert len(contracts) == 1
        assert (
            contracts[0] == "0x000000000004444c5dc75cB358380D2e3dE08A90"
        )  # V4 pool manager

    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.fetch_logs")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.get_latest_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._get_last_processed_block")
    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher._cleanup_last_file")
    @pytest.mark.asyncio
    async def test_fetch_uniswap_v3_pools_with_checkpoint(
        self,
        mock_cleanup,
        mock_get_last_block,
        mock_get_latest,
        mock_fetch_logs,
        ethereum_fetcher,
    ):
        """Test Uniswap V3 pool fetching with checkpoint resumption."""
        # Mock checkpoint exists
        mock_get_latest.return_value = 18000000
        mock_get_last_block.return_value = 15000000  # Checkpoint block
        mock_cleanup.return_value = True

        mock_result = FetchResult(
            success=True,
            data_path="/tmp/uniswap_v3_pools",
            fetched_blocks=3000000,
            start_block=15000000,  # Should start from checkpoint
            end_block=18000000,
        )
        mock_fetch_logs.return_value = mock_result

        result = await ethereum_fetcher.fetch_uniswap_v3_pools(from_checkpoint=True)

        assert result.success is True

        # Should have called cleanup since resuming from checkpoint
        mock_cleanup.assert_called_once()

        # Should start from checkpoint block, not deployment block
        mock_fetch_logs.assert_called_once()
        call_args = mock_fetch_logs.call_args
        assert call_args.kwargs["start_block"] == 15000000

    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.fetch_transfers")
    @pytest.mark.asyncio
    async def test_fetch_recent_transfers(self, mock_fetch_transfers, ethereum_fetcher):
        """Test recent transfer fetching."""
        mock_result = FetchResult(
            success=True,
            data_path="/tmp/recent_transfers",
            fetched_blocks=300,
            start_block=18499700,
            end_block=18500000,
        )
        mock_fetch_transfers.return_value = mock_result

        result = await ethereum_fetcher.fetch_recent_transfers(hours_back=1)

        assert result.success is True
        mock_fetch_transfers.assert_called_once()

        # Check that correct hours_back parameter was used
        call_args = mock_fetch_transfers.call_args
        assert call_args.kwargs["hours_back"] == 1

    @patch("src.fetchers.ethereum_fetcher.EthereumFetcher.fetch_logs")
    @pytest.mark.asyncio
    async def test_fetch_liquidity_events(self, mock_fetch_logs, ethereum_fetcher):
        """Test liquidity event fetching."""
        mock_result = FetchResult(
            success=True,
            data_path="/tmp/liquidity_events",
            fetched_blocks=1000,
            start_block=18999700,
            end_block=19000000,
        )
        mock_fetch_logs.return_value = mock_result

        result = await ethereum_fetcher.fetch_liquidity_events(
            protocol="uniswap_v3", hours_back=1
        )

        assert result.success is True
        mock_fetch_logs.assert_called_once()

        # Check that liquidity event-related parameters were used
        call_args = mock_fetch_logs.call_args
        assert "events" in call_args.kwargs
        events = call_args.kwargs["events"]
        assert len(events) == 2  # mint and burn events
        assert result.metadata["protocol"] == "uniswap_v3"
        assert result.metadata["hours_back"] == 1

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @pytest.mark.asyncio
    async def test_get_last_processed_block_no_files(
        self, mock_glob, mock_exists, ethereum_fetcher
    ):
        """Test _get_last_processed_block when no files exist."""
        mock_exists.return_value = False

        result = await ethereum_fetcher._get_last_processed_block(
            "uniswap_v3_pools", "/tmp/test"
        )

        assert result is None

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @pytest.mark.asyncio
    async def test_get_last_processed_block_with_files(
        self, mock_glob, mock_exists, ethereum_fetcher
    ):
        """Test _get_last_processed_block extracts highest end block."""
        mock_exists.return_value = True

        # Mock parquet files with different block ranges
        mock_files = []
        for end_block in [15000000, 16000000, 17000000]:
            mock_file = MagicMock()
            mock_file.stem = f"ethereum__logs__14000000_to_{end_block}"
            mock_files.append(mock_file)

        mock_glob.return_value = mock_files

        result = await ethereum_fetcher._get_last_processed_block(
            "uniswap_v3_pools", "/tmp/test"
        )

        assert result == 17000000  # Should return the highest end block

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.stat")
    @patch("pathlib.Path.unlink")
    @pytest.mark.asyncio
    async def test_cleanup_last_file_success(
        self, mock_unlink, mock_stat, mock_glob, mock_exists, ethereum_fetcher
    ):
        """Test _cleanup_last_file removes most recent file."""
        mock_exists.return_value = True

        # Mock two files with different modification times
        old_file = MagicMock()
        old_file.stat.return_value.st_mtime = 1000000
        new_file = MagicMock()
        new_file.stat.return_value.st_mtime = 2000000

        mock_glob.return_value = [old_file, new_file]

        result = await ethereum_fetcher._cleanup_last_file("/tmp/test")

        assert result is True
        new_file.unlink.assert_called_once()  # Should remove the newest file
        old_file.unlink.assert_not_called()

    @patch("pathlib.Path.exists")
    @pytest.mark.asyncio
    async def test_cleanup_last_file_no_directory(self, mock_exists, ethereum_fetcher):
        """Test _cleanup_last_file when directory doesn't exist."""
        mock_exists.return_value = False

        result = await ethereum_fetcher._cleanup_last_file("/tmp/nonexistent")

        assert result is False
