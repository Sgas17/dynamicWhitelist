"""
Tests for pool processor functionality.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import polars as pl
import pytest
from hexbytes import HexBytes

from ..base import ProcessorResult
from ..pool_processors import UniswapV3PoolProcessor, UniswapV4PoolProcessor


class TestUniswapV3PoolProcessor:
    """Test Uniswap V3 pool processor functionality."""

    def test_initialization(self):
        """Test processor initialization."""
        processor = UniswapV3PoolProcessor("ethereum")

        assert processor.chain == "ethereum"
        assert processor.protocol == "uniswap_v3"
        assert processor.lp_type == "UniswapV3"
        assert len(processor.factory_addresses) == 3
        assert processor.get_identifier() == "ethereum_uniswap_v3_processor"

    def test_validate_config_success(self):
        """Test successful config validation."""
        processor = UniswapV3PoolProcessor("ethereum")

        result = processor.validate_config()

        assert result is True

    def test_validate_config_no_factories(self):
        """Test config validation with no factory addresses."""
        processor = UniswapV3PoolProcessor("unknown_chain")

        result = processor.validate_config()

        assert result is False

    @pytest.mark.asyncio
    async def test_process_no_events(self):
        """Test processing with no events."""
        processor = UniswapV3PoolProcessor("ethereum")

        # Mock empty parquet reading
        with patch("polars.read_parquet") as mock_read:
            mock_df = Mock()
            mock_df.filter.return_value.sort.return_value.is_empty.return_value = True
            mock_read.return_value = mock_df

            result = await processor.process(
                start_block=1000,
                events_path="/tmp/test_events",
                output_file="/tmp/test_output.json",
            )

        assert result.success is True
        assert result.processed_count == 0
        assert len(result.data) == 0

    def test_process_single_event_valid(self):
        """Test processing a single valid event."""
        processor = UniswapV3PoolProcessor("ethereum")

        # Mock event data
        event = {
            "address": HexBytes("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
            "topic1": HexBytes(
                "0x000000000000000000000000A0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e"
            ),  # Mock token0 (valid 40 char)
            "topic2": HexBytes(
                "0x000000000000000000000000B0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e"
            ),  # Mock token1 (valid 40 char)
            "topic3": HexBytes(
                "0x0000000000000000000000000000000000000000000000000000000000000bb8"
            ),  # fee: 3000
            "data": HexBytes(
                "0x000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000C0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e"
            ),  # tick_spacing: 10, pool_address (valid 40 char)
            "block_number": 18000000,
        }

        # Mock eth_abi decode calls
        with patch("eth_abi.abi.decode") as mock_decode:
            with patch("eth_utils.address.to_checksum_address") as mock_checksum:
                # Setup mock returns
                mock_decode.side_effect = [
                    [
                        "0xA0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e"
                    ],  # token0 (valid address)
                    [
                        "0xB0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e"
                    ],  # token1 (valid address)
                    [3000],  # fee
                    [
                        10,
                        "0xC0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e",
                    ],  # tick_spacing, pool_address (valid address)
                ]
                mock_checksum.side_effect = [
                    "0xA0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e",
                    "0xB0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e",
                    "0xC0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e",
                ]

                result = processor._process_single_event(event)

        assert result is not None
        assert (
            result["address"].lower()
            == "0xC0b86a33E6b38Dea8F7a7b7b1b1b2E4e4e4e4e4e".lower()
        )
        assert result["fee"] == 3000
        assert result["tick_spacing"] == 10
        assert result["type"] == "UniswapV3"
        assert result["chain"] == "ethereum"

    def test_process_single_event_invalid_factory(self):
        """Test processing event from invalid factory."""
        processor = UniswapV3PoolProcessor("ethereum")

        # Event from unknown factory
        event = {
            "address": HexBytes("0x1111111111111111111111111111111111111111"),
            "block_number": 18000000,
        }

        result = processor._process_single_event(event)

        assert result is None

    def test_deduplicate_pools(self):
        """Test pool deduplication functionality."""
        processor = UniswapV3PoolProcessor("ethereum")

        pools = [
            {"address": "0x123", "fee": 3000},
            {"address": "0x456", "fee": 500},
            {"address": "0x123", "fee": 3000},  # Duplicate
            {"address": "0x789", "fee": 10000},
        ]

        unique_pools = processor._deduplicate_pools(pools)

        assert len(unique_pools) == 3
        addresses = [pool["address"] for pool in unique_pools]
        assert "0x123" in addresses
        assert "0x456" in addresses
        assert "0x789" in addresses
        assert addresses.count("0x123") == 1  # No duplicates

    def test_to_serializable(self):
        """Test value serialization."""
        processor = UniswapV3PoolProcessor("ethereum")

        # Test different value types
        assert processor._to_serializable(None) is None
        assert processor._to_serializable("test") == "test"
        assert processor._to_serializable(123) == 123
        assert processor._to_serializable(HexBytes("0x123")) == "0x0123"
        assert processor._to_serializable(b"\x01\x23") == "0x0123"


class TestUniswapV4PoolProcessor:
    """Test Uniswap V4 pool processor functionality."""

    def test_initialization(self):
        """Test processor initialization."""
        processor = UniswapV4PoolProcessor("ethereum")

        assert processor.chain == "ethereum"
        assert processor.protocol == "uniswap_v4"
        assert processor.lp_type == "UniswapV4"
        assert len(processor.factory_addresses) == 0  # V4 not deployed yet

    def test_validate_config(self):
        """Test config validation (always passes for now)."""
        processor = UniswapV4PoolProcessor("ethereum")

        result = processor.validate_config()

        assert result is True

    @pytest.mark.asyncio
    async def test_process_placeholder(self):
        """Test V4 processor placeholder implementation."""
        processor = UniswapV4PoolProcessor("ethereum")

        result = await processor.process()

        assert result.success is True
        assert result.processed_count == 0
        assert len(result.data) == 0
        assert "placeholder" in result.metadata["message"]
