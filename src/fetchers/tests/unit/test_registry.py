"""Tests for fetcher registry functionality."""

import pytest

from ....fetchers import get_fetcher, list_fetchers
from ....fetchers.ethereum_fetcher import EthereumFetcher


class TestFetcherRegistry:
    """Test cases for fetcher registry system."""

    def test_list_fetchers(self):
        """Test listing available fetchers."""
        fetchers = list_fetchers()

        assert isinstance(fetchers, list)
        assert len(fetchers) > 0
        assert "ethereum" in fetchers

    def test_get_fetcher_ethereum(self):
        """Test getting Ethereum fetcher class."""
        fetcher_class = get_fetcher("ethereum")

        assert fetcher_class == EthereumFetcher
        # Test instantiation
        fetcher = fetcher_class("https://eth.llamarpc.com")
        assert fetcher.chain == "ethereum"

    def test_get_fetcher_invalid_chain(self):
        """Test getting fetcher for invalid chain."""
        with pytest.raises(ValueError, match="No fetcher available for chain"):
            get_fetcher("invalid_chain")

    def test_get_fetcher_case_insensitive(self):
        """Test fetcher lookup is case insensitive."""
        fetcher_lower = get_fetcher("ethereum")
        fetcher_upper = get_fetcher("ETHEREUM")
        fetcher_mixed = get_fetcher("Ethereum")

        assert fetcher_lower == EthereumFetcher
        assert fetcher_upper == EthereumFetcher
        assert fetcher_mixed == EthereumFetcher
        assert fetcher_lower == fetcher_upper == fetcher_mixed
