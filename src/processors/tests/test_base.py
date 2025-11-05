"""
Tests for base processor functionality.
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from ..base import BaseProcessor, ProcessorError, ProcessorResult


class TestProcessorResult:
    """Test ProcessorResult dataclass functionality."""

    def test_success_result(self):
        """Test creating a successful result."""
        result = ProcessorResult(
            success=True,
            data=[{"token": "0x123", "amount": 1000}],
            processed_count=1,
            metadata={"chain": "ethereum"},
        )

        assert result.success is True
        assert result.failed is False
        assert result.processed_count == 1
        assert result.data[0]["token"] == "0x123"
        assert result.metadata["chain"] == "ethereum"

    def test_failed_result(self):
        """Test creating a failed result."""
        result = ProcessorResult(
            success=False, error="Connection timeout", processed_count=0
        )

        assert result.success is False
        assert result.failed is True
        assert result.processed_count == 0
        assert result.error == "Connection timeout"


class MockProcessor(BaseProcessor):
    """Mock processor for testing base functionality."""

    def __init__(self, chain: str = "ethereum", protocol: str = "test"):
        super().__init__(chain, protocol)
        self.process_called = False
        self.validate_called = False

    async def process(self, **kwargs) -> ProcessorResult:
        """Mock process implementation."""
        self.process_called = True
        return ProcessorResult(success=True, data=[{"test": "data"}], processed_count=1)

    def validate_config(self) -> bool:
        """Mock validation implementation."""
        self.validate_called = True
        return True


class TestBaseProcessor:
    """Test BaseProcessor abstract class functionality."""

    def test_initialization(self):
        """Test processor initialization."""
        processor = MockProcessor("ethereum", "uniswap_v3")

        assert processor.chain == "ethereum"
        assert processor.protocol == "uniswap_v3"
        assert processor.get_identifier() == "ethereum_uniswap_v3_processor"
        assert processor.logger is not None

    @pytest.mark.asyncio
    async def test_process_method(self):
        """Test process method is called correctly."""
        processor = MockProcessor()

        result = await processor.process(test_param="value")

        assert processor.process_called is True
        assert result.success is True
        assert result.processed_count == 1
        assert result.data[0]["test"] == "data"

    def test_validate_config(self):
        """Test config validation."""
        processor = MockProcessor()

        is_valid = processor.validate_config()

        assert processor.validate_called is True
        assert is_valid is True

    def test_log_result_success(self, caplog):
        """Test logging successful results."""
        import logging

        caplog.set_level(logging.INFO)

        processor = MockProcessor()
        result = ProcessorResult(
            success=True, processed_count=5, data=[{"test": "data"}]
        )

        processor.log_result(result)

        assert "Processing completed: 5 items processed" in caplog.text

    def test_log_result_failure(self, caplog):
        """Test logging failed results."""
        processor = MockProcessor()
        result = ProcessorResult(success=False, error="Test error message")

        processor.log_result(result)

        assert "Processing failed: Test error message" in caplog.text
