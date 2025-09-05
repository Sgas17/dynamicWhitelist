"""
Pytest configuration for processor tests.
"""

import pytest
from unittest.mock import AsyncMock, Mock

@pytest.fixture
def mock_config_manager():
    """Mock ConfigManager for testing."""
    mock_config = Mock()
    mock_config.base.DATA_DIR = "/tmp/test_data"
    mock_config.database.REDIS_HOST = "localhost"
    mock_config.database.REDIS_PORT = 6379
    mock_config.database.REDIS_DB = 0
    mock_config.database.REDIS_PASSWORD = None
    return mock_config

@pytest.fixture
def mock_redis_client():
    """Mock Redis client for testing."""
    mock_client = AsyncMock()
    mock_client.hset.return_value = True
    mock_client.expire.return_value = True
    mock_client.close.return_value = None
    return mock_client