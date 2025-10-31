"""
Unit tests for TokenWhitelistNatsPublisher.

Tests token whitelist publishing to NATS topics including:
- Full whitelist publishing
- Add delta publishing
- Remove delta publishing
- Filter counting
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.core.storage.token_whitelist_publisher import TokenWhitelistNatsPublisher


@pytest.fixture
def sample_tokens():
    """Sample token whitelist for testing."""
    return {
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": {
            "symbol": "USDC",
            "decimals": 6,
            "name": "USD Coin",
            "filters": ["cross_chain", "top_transferred"]
        },
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {
            "symbol": "WETH",
            "decimals": 18,
            "name": "Wrapped Ether",
            "filters": ["cross_chain", "hyperliquid"]
        },
        "0xdAC17F958D2ee523a2206206994597C13D831ec7": {
            "symbol": "USDT",
            "decimals": 6,
            "name": "Tether USD",
            "filters": ["top_transferred"]
        }
    }


@pytest.fixture
def mock_nats_client():
    """Mock NATS client."""
    mock_nc = AsyncMock()
    mock_nc.publish = AsyncMock()
    mock_nc.close = AsyncMock()
    return mock_nc


@pytest.mark.asyncio
async def test_publish_full_whitelist(sample_tokens, mock_nats_client):
    """Test publishing complete token whitelist."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=False
            )

            # Verify full topic was published
            assert results["full"] is True
            assert mock_nats_client.publish.call_count == 1

            # Verify message structure
            call_args = mock_nats_client.publish.call_args[0]
            subject = call_args[0]
            assert subject == "whitelist.tokens.ethereum.full"


@pytest.mark.asyncio
async def test_publish_add_delta(sample_tokens, mock_nats_client):
    """Test publishing added tokens delta."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            # First publish - should publish add delta
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=False,
                publish_deltas=True
            )

            # All tokens are new, so add should succeed
            assert results["add"] is True
            assert results["remove"] is True  # No removals
            assert mock_nats_client.publish.call_count == 1  # Only add delta


@pytest.mark.asyncio
async def test_publish_remove_delta(sample_tokens, mock_nats_client):
    """Test publishing removed tokens delta."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            # First publish - establish baseline
            await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=False,
                publish_deltas=True
            )

            # Reset mock
            mock_nats_client.publish.reset_mock()

            # Second publish with fewer tokens
            reduced_tokens = {
                k: v for k, v in list(sample_tokens.items())[:2]
            }

            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=reduced_tokens,
                publish_full=False,
                publish_deltas=True
            )

            # Should publish remove delta
            assert results["remove"] is True
            # At least one call for remove
            assert mock_nats_client.publish.call_count >= 1


@pytest.mark.asyncio
async def test_filter_counts(sample_tokens, mock_nats_client):
    """Test filter counting in metadata."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=False
            )

            assert results["full"] is True

            # Verify filter counts in published message
            call_args = mock_nats_client.publish.call_args[0]
            payload = call_args[1]

            import json
            message = json.loads(payload.decode())

            # Check metadata has filter counts
            assert "metadata" in message
            assert "filter_counts" in message["metadata"]
            filter_counts = message["metadata"]["filter_counts"]

            # Verify counts
            assert filter_counts["cross_chain"] == 2
            assert filter_counts["top_transferred"] == 2
            assert filter_counts["hyperliquid"] == 1


@pytest.mark.asyncio
async def test_empty_tokens(mock_nats_client):
    """Test handling empty token list."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens={},
                publish_full=True,
                publish_deltas=True
            )

            # Should return False for empty list
            assert results["full"] is False
            assert results["add"] is False
            assert results["remove"] is False
            assert mock_nats_client.publish.call_count == 0


@pytest.mark.asyncio
async def test_connection_failure():
    """Test handling NATS connection failure."""
    with patch('nats.connect', side_effect=Exception("Connection failed")):
        with pytest.raises(Exception):
            async with TokenWhitelistNatsPublisher() as publisher:
                pass


@pytest.mark.asyncio
async def test_publish_failure(sample_tokens, mock_nats_client):
    """Test handling publish failure."""
    mock_nats_client.publish.side_effect = Exception("Publish failed")

    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=False
            )

            # Should return False on failure
            assert results["full"] is False


@pytest.mark.asyncio
async def test_multiple_chains(sample_tokens, mock_nats_client):
    """Test publishing to multiple chains independently."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            # Publish to ethereum
            results1 = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=True
            )

            # Publish to base
            results2 = await publisher.publish_token_whitelist(
                chain="base",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=True
            )

            # Both should succeed
            assert results1["full"] is True
            assert results2["full"] is True

            # Should have published to different subjects
            calls = mock_nats_client.publish.call_args_list
            subjects = [call[0][0] for call in calls]

            assert "whitelist.tokens.ethereum.full" in subjects
            assert "whitelist.tokens.base.full" in subjects


@pytest.mark.asyncio
async def test_delta_tracking_across_updates(sample_tokens, mock_nats_client):
    """Test delta tracking across multiple updates."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            # First update
            await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_deltas=True
            )

            # Second update with same tokens
            mock_nats_client.publish.reset_mock()
            results = await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_deltas=True
            )

            # No adds or removes - only full
            assert results["add"] is True  # No additions is success
            assert results["remove"] is True  # No removals is success
            # Should only publish full whitelist
            assert mock_nats_client.publish.call_count == 1


@pytest.mark.asyncio
async def test_token_metadata_structure(sample_tokens, mock_nats_client):
    """Test published token metadata structure."""
    with patch('nats.connect', return_value=mock_nats_client):
        async with TokenWhitelistNatsPublisher() as publisher:
            await publisher.publish_token_whitelist(
                chain="ethereum",
                tokens=sample_tokens,
                publish_full=True,
                publish_deltas=False  # Only test full message
            )

            # Extract published message
            call_args = mock_nats_client.publish.call_args[0]
            payload = call_args[1]

            import json
            message = json.loads(payload.decode())

            # Verify message structure
            assert "chain" in message
            assert "timestamp" in message
            assert "tokens" in message
            assert "metadata" in message

            # Verify token structure
            for token_addr, token_data in message["tokens"].items():
                assert "symbol" in token_data
                assert "decimals" in token_data
                assert "name" in token_data
                assert "filters" in token_data
                assert isinstance(token_data["filters"], list)
