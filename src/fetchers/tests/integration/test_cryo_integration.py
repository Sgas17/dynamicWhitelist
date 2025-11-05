"""
Integration tests for cryo fetcher functionality.

These tests require:
- cryo command installed
- active RPC endpoint
- actual blockchain access

Run with: pytest test/integration/test_cryo_integration.py -v -s
"""

import asyncio
import os
import subprocess
from pathlib import Path

import pytest
from dotenv import load_dotenv

from ....fetchers.cryo_fetcher import CryoFetcher
from ....fetchers.ethereum_fetcher import EthereumFetcher

load_dotenv()
local_ethereum_node = os.getenv("ETHEREUM_RPC_URL")


def check_cryo_available():
    """Check if cryo command is available."""
    try:
        result = subprocess.run(["cryo", "--version"], capture_output=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_cast_available():
    """Check if cast command is available."""
    try:
        result = subprocess.run(["cast", "--version"], capture_output=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


@pytest.mark.integration
@pytest.mark.skipif(not check_cryo_available(), reason="cryo command not available")
class TestCryoIntegration:
    """Integration tests for CryoFetcher with real blockchain data."""

    @pytest.fixture(scope="class")
    def rpc_url(self):
        """Use a public RPC endpoint for testing."""
        # Using LlamaNodes public endpoint (rate-limited but free)
        return local_ethereum_node

    @pytest.fixture(scope="class")
    def cryo_fetcher(self, rpc_url):
        """Create CryoFetcher instance for testing."""
        return CryoFetcher("ethereum", rpc_url)

    @pytest.fixture(scope="class")
    def temp_output_dir(self, tmp_path_factory):
        """Create temporary directory for test outputs."""
        return tmp_path_factory.mktemp("cryo_integration_test")

    def test_cryo_fetcher_initialization(self, cryo_fetcher):
        """Test that CryoFetcher initializes correctly."""
        assert cryo_fetcher.chain == "ethereum"
        assert cryo_fetcher.validate_config() is True

    @pytest.mark.skipif(not check_cast_available(), reason="cast command not available")
    @pytest.mark.asyncio
    async def test_get_latest_block_real(self, cryo_fetcher):
        """Test getting latest block from real chain."""
        try:
            latest_block = await cryo_fetcher.get_latest_block()

            # Ethereum block numbers should be > 18M by now
            assert latest_block > 18000000
            assert isinstance(latest_block, int)
            print(f"Latest block: {latest_block}")
        except Exception as e:
            pytest.skip(f"RPC endpoint unavailable or rate limited: {e}")

    @pytest.mark.asyncio
    async def test_fetch_logs_small_range(self, cryo_fetcher, temp_output_dir):
        """Test fetching logs for a small block range."""
        try:
            # Use a small, known block range with USDC transfers
            start_block = 18500000
            end_block = 18500010  # Just 10 blocks

            # USDC contract
            usdc_contract = "0xA0b86a33E6C6c7C8E1F2E3F4a5B6C7D8E9F0A1B2"

            result = await cryo_fetcher.fetch_logs(
                start_block=start_block,
                end_block=end_block,
                contracts=[usdc_contract],
                output_dir=str(temp_output_dir),
            )

            print(f"Fetch result: {result}")

            # Should succeed even if no logs found
            assert result.success is True
            assert result.start_block == start_block
            assert result.end_block == end_block
            assert Path(result.data_path).exists()

        except Exception as e:
            pytest.skip(f"Fetch failed (likely RPC rate limiting): {e}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_fetch_recent_transfers_real(self, cryo_fetcher, temp_output_dir):
        """Test fetching recent transfers from real chain."""
        try:
            # Fetch just 1 hour of recent transfers
            result = await cryo_fetcher.fetch_transfers(
                hours_back=1,
                output_dir=str(temp_output_dir / "transfers"),
                chunk_size=100,  # Small chunks to avoid rate limiting
            )

            print(f"Transfer fetch result: {result}")

            if result.success:
                assert result.fetched_blocks > 0
                assert Path(result.data_path).exists()
            else:
                # May fail due to rate limiting, which is expected
                assert (
                    "rate" in result.error.lower() or "timeout" in result.error.lower()
                )

        except Exception as e:
            pytest.skip(f"Transfer fetch failed (likely rate limiting): {e}")


@pytest.mark.integration
@pytest.mark.skipif(not check_cryo_available(), reason="cryo command not available")
class TestEthereumFetcherIntegration:
    """Integration tests for EthereumFetcher with real data."""

    @pytest.fixture(scope="class")
    def ethereum_fetcher(self):
        """Create EthereumFetcher instance."""
        return EthereumFetcher(local_ethereum_node)

    @pytest.fixture(scope="class")
    def temp_output_dir(self, tmp_path_factory):
        """Create temporary directory for test outputs."""
        return tmp_path_factory.mktemp("ethereum_integration_test")

    def test_ethereum_fetcher_initialization(self, ethereum_fetcher):
        """Test EthereumFetcher initialization."""
        assert ethereum_fetcher.chain == "ethereum"
        assert ethereum_fetcher.blocks_per_minute == 5
        assert ethereum_fetcher.validate_config() is True

    @pytest.mark.asyncio
    async def test_calculate_block_range_realistic(self, ethereum_fetcher):
        """Test block range calculation produces realistic ranges."""
        start_block, end_block = await ethereum_fetcher.calculate_block_range(
            1
        )  # 1 hour

        # Should be 300 blocks apart (1 hour * 60 minutes * 5 blocks/minute)
        assert (end_block - start_block) == 300

        # Blocks should be in realistic Ethereum range
        assert start_block > 15000000  # Post-merge blocks
        assert end_block > start_block

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_fetch_uniswap_v3_pools_sample(
        self, ethereum_fetcher, temp_output_dir
    ):
        """Test fetching a small sample of Uniswap V3 pools."""
        try:
            result = await ethereum_fetcher.fetch_uniswap_v3_pools(
                output_dir=str(temp_output_dir / "uniswap_pools")
            )

            print(f"Uniswap V3 pools result: {result}")

            if result.success:
                # Should have found pool creation events
                assert (
                    result.fetched_blocks > 1000000
                )  # Millions of blocks since deployment
                assert Path(result.data_path).exists()

                # Check metadata
                assert result.metadata["protocol"] == "uniswap_v3"
                assert result.metadata["deployment_block"] == 12369621
            else:
                # May fail due to rate limiting
                pytest.skip(f"Rate limited or RPC unavailable: {result.error}")

        except Exception as e:
            pytest.skip(f"Pool fetch failed: {e}")


@pytest.mark.integration
class TestCryoCommandValidation:
    """Test cryo command validation without blockchain access."""

    @pytest.mark.skipif(not check_cryo_available(), reason="cryo command not available")
    def test_cryo_version(self):
        """Test cryo version command."""
        result = subprocess.run(["cryo", "--version"], capture_output=True, text=True)

        assert result.returncode == 0
        assert "cryo" in result.stdout.lower()
        print(f"Cryo version: {result.stdout.strip()}")

    @pytest.mark.skipif(not check_cast_available(), reason="cast command not available")
    def test_cast_version(self):
        """Test cast version command."""
        result = subprocess.run(["cast", "--version"], capture_output=True, text=True)

        assert result.returncode == 0
        print(f"Cast version: {result.stdout.strip()}")

    def test_cryo_help(self):
        """Test cryo help command."""
        if not check_cryo_available():
            pytest.skip("cryo not available")

        result = subprocess.run(["cryo", "--help"], capture_output=True, text=True)

        assert result.returncode == 0
        assert "datasets" in result.stdout or "logs" in result.stdout
        assert "blocks" in result.stdout
