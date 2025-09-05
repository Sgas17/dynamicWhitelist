"""Test configuration for fetchers."""
import pytest
from unittest.mock import AsyncMock, Mock
from src.config import ConfigManager


@pytest.fixture
def config_manager():
    """Provide a test configuration manager."""
    return ConfigManager()


@pytest.fixture
def mock_subprocess():
    """Mock subprocess for cryo command execution."""
    mock_process = Mock()
    mock_process.returncode = 0
    mock_process.stdout = b'{"success": true}'
    mock_process.stderr = b""
    return mock_process


@pytest.fixture
def sample_cryo_response():
    """Sample cryo response data."""
    return {
        "block_number": 18500000,
        "transaction_hash": "0x123...",
        "log_index": 0,
        "address": "0xa0b86a33e6c6c7c8e1f2e3f4a5b6c7d8e9f0a1b2",
        "topics": ["0x1234..."],
        "data": "0x5678..."
    }