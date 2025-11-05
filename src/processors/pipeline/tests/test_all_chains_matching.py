"""
Tests for the process_with_all_chains method that collects all chain addresses per token.

This demonstrates collecting multiple chain addresses for each token rather than just the best match.
"""

from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from ...fetchers.exchange_fetchers import ExchangeToken
from ..token_matching_processor import TokenMatchingProcessor


class TestAllChainsMatching:
    """Test the new all-chains matching functionality."""

    @pytest.fixture
    def multi_chain_tokens(self) -> List[ExchangeToken]:
        """Tokens that exist on multiple chains."""
        return [
            ExchangeToken("USDC/USD", "USDC", "USD", "1", "hyperliquid", True, "swap"),
            ExchangeToken("BTC/USD", "BTC", "USD", "2", "hyperliquid", True, "swap"),
            ExchangeToken("ETH/USD", "ETH", "USD", "3", "hyperliquid", True, "swap"),
            ExchangeToken("LINK/USD", "LINK", "USD", "4", "hyperliquid", True, "swap"),
        ]

    @pytest.fixture
    def comprehensive_multi_chain_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """CoinGecko data with tokens on multiple chains."""
        return {
            "ethereum": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "ethereum",
                    "address": "0xa0b86a33e6a75c3c5b06b6b1f06b7c4dea73bb6e",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "bitcoin",
                    "symbol": "WBTC",
                    "name": "Wrapped Bitcoin",
                    "market_cap_rank": 1,
                    "platform": "ethereum",
                    "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                    "decimals": 8,
                    "total_supply": "150000",
                },
                {
                    "coingecko_id": "ethereum",
                    "symbol": "WETH",
                    "name": "Wrapped Ether",
                    "market_cap_rank": 2,
                    "platform": "ethereum",
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "decimals": 18,
                    "total_supply": "7000000",
                },
                {
                    "coingecko_id": "chainlink",
                    "symbol": "LINK",
                    "name": "Chainlink",
                    "market_cap_rank": 15,
                    "platform": "ethereum",
                    "address": "0x514910771af9ca656af840dff83e8264ecf986ca",
                    "decimals": 18,
                    "total_supply": "1000000000",
                },
            ],
            "base": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "base",
                    "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "ethereum",
                    "symbol": "WETH",
                    "name": "Wrapped Ether",
                    "market_cap_rank": 2,
                    "platform": "base",
                    "address": "0x4200000000000000000000000000000000000006",
                    "decimals": 18,
                    "total_supply": "7000000",
                },
            ],
            "arbitrum-one": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "arbitrum-one",
                    "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "bitcoin",
                    "symbol": "WBTC",
                    "name": "Wrapped Bitcoin",
                    "market_cap_rank": 1,
                    "platform": "arbitrum-one",
                    "address": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                    "decimals": 8,
                    "total_supply": "150000",
                },
                {
                    "coingecko_id": "chainlink",
                    "symbol": "LINK",
                    "name": "Chainlink",
                    "market_cap_rank": 15,
                    "platform": "arbitrum-one",
                    "address": "0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
                    "decimals": 18,
                    "total_supply": "1000000000",
                },
            ],
        }

    @pytest.mark.asyncio
    async def test_process_with_all_chains_basic(
        self, multi_chain_tokens, comprehensive_multi_chain_data
    ):
        """Test basic functionality of process_with_all_chains method."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            tokens = result.data

            # Should process all tokens
            assert len(tokens) == len(multi_chain_tokens)

            # Check token structure
            for token in tokens:
                assert "symbol" in token
                assert "chain_addresses" in token
                assert "supported_chains" in token
                assert "chain_count" in token
                assert "coingecko_id" in token
                assert "token_name" in token

                # Should have at least one chain
                assert len(token["chain_addresses"]) > 0
                assert token["chain_count"] == len(token["chain_addresses"])
                assert set(token["supported_chains"]) == set(
                    token["chain_addresses"].keys()
                )

    @pytest.mark.asyncio
    async def test_multi_chain_address_collection(
        self, multi_chain_tokens, comprehensive_multi_chain_data
    ):
        """Test that tokens are found on multiple chains with different addresses."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            tokens = result.data

            # Find USDC token (should be on all 3 chains)
            usdc_token = next((t for t in tokens if t["symbol"] == "USDC"), None)
            assert usdc_token is not None

            # USDC should be found on all three chains
            assert usdc_token["chain_count"] == 3
            assert "ethereum" in usdc_token["chain_addresses"]
            assert "base" in usdc_token["chain_addresses"]
            assert "arbitrum" in usdc_token["chain_addresses"]

            # Each chain should have different addresses
            eth_address = usdc_token["chain_addresses"]["ethereum"]["address"]
            base_address = usdc_token["chain_addresses"]["base"]["address"]
            arb_address = usdc_token["chain_addresses"]["arbitrum"]["address"]

            assert eth_address == "0xa0b86a33e6a75c3c5b06b6b1f06b7c4dea73bb6e"
            assert base_address == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            assert arb_address == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

            # All should have same confidence (exact match)
            assert usdc_token["chain_addresses"]["ethereum"]["confidence"] == 0.95
            assert usdc_token["chain_addresses"]["base"]["confidence"] == 0.95
            assert usdc_token["chain_addresses"]["arbitrum"]["confidence"] == 0.95

    @pytest.mark.asyncio
    async def test_chain_distribution_stats(
        self, multi_chain_tokens, comprehensive_multi_chain_data
    ):
        """Test statistics about chain distribution."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            metadata = result.metadata

            # Check overall stats
            assert metadata["processed_tokens"] == 4  # All 4 tokens should match
            assert (
                metadata["total_addresses"] > 4
            )  # Should have more addresses than tokens

            # Check chain coverage
            coverage = metadata["coverage_by_chain"]
            assert "ethereum" in coverage
            assert coverage["ethereum"] >= 3  # Most tokens should be on ethereum

            # Check multi-chain vs single-chain distribution
            assert metadata["multi_chain_tokens"] > 0  # Some tokens on multiple chains
            assert (
                metadata["single_chain_tokens"] >= 0
            )  # Some might be single-chain only

            # Check distribution breakdown
            distribution = metadata["multi_chain_distribution"]
            assert isinstance(distribution, dict)

            print("\n✓ Chain distribution stats:")
            print(f"  Total addresses: {metadata['total_addresses']}")
            print(f"  Coverage by chain: {coverage}")
            print(f"  Multi-chain tokens: {metadata['multi_chain_tokens']}")
            print(f"  Single-chain tokens: {metadata['single_chain_tokens']}")
            print(f"  Distribution: {distribution}")

    @pytest.mark.asyncio
    async def test_whitelist_format_output(
        self, multi_chain_tokens, comprehensive_multi_chain_data
    ):
        """Test generating whitelist format from all-chains data."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.8,
            )

            assert result.success is True
            tokens = result.data

            # Generate whitelist format grouped by chain
            chain_whitelists = {}

            for token in tokens:
                for chain, chain_data in token["chain_addresses"].items():
                    if chain not in chain_whitelists:
                        chain_whitelists[chain] = []

                    chain_whitelists[chain].append(
                        {
                            "symbol": token["symbol"],
                            "address": chain_data["address"],
                            "decimals": chain_data["decimals"],
                            "name": token["token_name"],
                            "coingecko_id": token["coingecko_id"],
                            "confidence": chain_data["confidence"],
                            "match_type": chain_data["match_type"],
                            "exchange_market_id": token["market_id"],
                        }
                    )

            # Verify whitelist structure
            assert len(chain_whitelists) > 0

            print(f"\n✓ Generated whitelists for {len(chain_whitelists)} chains:")

            for chain, chain_tokens in chain_whitelists.items():
                print(f"\n  {chain.upper()} ({len(chain_tokens)} tokens):")

                for token in chain_tokens[:3]:  # Show first 3
                    print(
                        f"    • {token['symbol']}: {token['address'][:10]}... ({token['confidence']:.1%})"
                    )

                if len(chain_tokens) > 3:
                    print(f"    ... and {len(chain_tokens) - 3} more")

            # Verify cross-chain tokens appear in multiple whitelists
            usdc_appearances = sum(
                1
                for tokens in chain_whitelists.values()
                for token in tokens
                if token["symbol"] == "USDC"
            )
            assert usdc_appearances > 1  # USDC should appear on multiple chains

    @pytest.mark.asyncio
    async def test_comparison_with_single_match_method(
        self, multi_chain_tokens, comprehensive_multi_chain_data
    ):
        """Compare all-chains method with single-match method."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            # Run both methods
            single_result = await processor.process(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            all_chains_result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert single_result.success is True
            assert all_chains_result.success is True

            # Single method returns TokenMatch objects
            single_matches = single_result.data
            # All-chains method returns token dictionaries
            all_chain_tokens = all_chains_result.data

            # Should process same number of tokens
            assert len(single_matches) == len(all_chain_tokens)

            # All-chains should have more total addresses
            single_addresses = len(single_matches)  # One address per token
            all_addresses = all_chains_result.metadata["total_addresses"]

            assert all_addresses >= single_addresses

            print("\n✓ Method comparison:")
            print(
                f"  Single-match: {single_addresses} addresses for {len(single_matches)} tokens"
            )
            print(
                f"  All-chains: {all_addresses} addresses for {len(all_chain_tokens)} tokens"
            )
            print(
                f"  Improvement: {all_addresses - single_addresses} additional addresses"
            )

    @pytest.mark.asyncio
    async def test_save_all_chains_output(
        self, multi_chain_tokens, comprehensive_multi_chain_data, tmp_path
    ):
        """Test saving all-chains output to file."""
        processor = TokenMatchingProcessor()
        output_file = tmp_path / "all_chains_whitelist.json"

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_multi_chain_data,
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=multi_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
                output_file=str(output_file),
            )

            assert result.success is True
            assert output_file.exists()

            # Load and verify file content
            import ujson

            with open(output_file, "r") as f:
                data = ujson.load(f)

            assert "metadata" in data
            assert "tokens" in data

            # Check metadata
            metadata = data["metadata"]
            assert metadata["method"] == "process_with_all_chains"
            assert metadata["total_tokens"] > 0
            assert metadata["total_addresses"] > 0

            # Check token structure in file
            tokens = data["tokens"]
            assert len(tokens) > 0

            for token in tokens:
                assert "symbol" in token
                assert "chain_addresses" in token
                assert "supported_chains" in token
                assert "chain_count" in token

                # Verify chain addresses structure
                for chain, chain_data in token["chain_addresses"].items():
                    assert "address" in chain_data
                    assert "confidence" in chain_data
                    assert "match_type" in chain_data
                    assert "decimals" in chain_data

            print(f"\n✓ Saved output to {output_file}")
            print(f"  File size: {output_file.stat().st_size} bytes")
            print(f"  Tokens: {len(tokens)}")
            print(f"  Total addresses: {metadata['total_addresses']}")


class TestRealWorldScenarios:
    """Test realistic scenarios with mixed single/multi-chain tokens."""

    @pytest.mark.asyncio
    async def test_mixed_token_scenario(self):
        """Test a realistic mix of single-chain and multi-chain tokens."""
        processor = TokenMatchingProcessor()

        # Mix of tokens - some multi-chain, some single-chain, some unmatched
        mixed_tokens = [
            ExchangeToken(
                "USDC/USD", "USDC", "USD", "1", "hyperliquid", True, "swap"
            ),  # Multi-chain
            ExchangeToken(
                "BTC/USD", "BTC", "USD", "2", "hyperliquid", True, "swap"
            ),  # Multi-chain
            ExchangeToken(
                "ETH/USD", "ETH", "USD", "3", "hyperliquid", True, "swap"
            ),  # Multi-chain
            ExchangeToken(
                "UNI/USD", "UNI", "USD", "4", "hyperliquid", True, "swap"
            ),  # Ethereum only
            ExchangeToken(
                "ARB/USD", "ARB", "USD", "5", "hyperliquid", True, "swap"
            ),  # Arbitrum only
            ExchangeToken(
                "FAKE/USD", "FAKE", "USD", "6", "hyperliquid", True, "swap"
            ),  # No match
        ]

        # Realistic database with mixed coverage
        mixed_db_data = {
            "ethereum": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "ethereum",
                    "address": "0xa0b86a33e6a75c3c5b06b6b1f06b7c4dea73bb6e",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "bitcoin",
                    "symbol": "WBTC",
                    "name": "Wrapped Bitcoin",
                    "market_cap_rank": 1,
                    "platform": "ethereum",
                    "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                    "decimals": 8,
                    "total_supply": "150000",
                },
                {
                    "coingecko_id": "ethereum",
                    "symbol": "WETH",
                    "name": "Wrapped Ether",
                    "market_cap_rank": 2,
                    "platform": "ethereum",
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "decimals": 18,
                    "total_supply": "7000000",
                },
                {
                    "coingecko_id": "uniswap",
                    "symbol": "UNI",
                    "name": "Uniswap",
                    "market_cap_rank": 20,
                    "platform": "ethereum",
                    "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                    "decimals": 18,
                    "total_supply": "1000000000",
                },
            ],
            "base": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "base",
                    "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "ethereum",
                    "symbol": "WETH",
                    "name": "Wrapped Ether",
                    "market_cap_rank": 2,
                    "platform": "base",
                    "address": "0x4200000000000000000000000000000000000006",
                    "decimals": 18,
                    "total_supply": "7000000",
                },
            ],
            "arbitrum-one": [
                {
                    "coingecko_id": "usd-coin",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "market_cap_rank": 5,
                    "platform": "arbitrum-one",
                    "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                    "decimals": 6,
                    "total_supply": "25000000000",
                },
                {
                    "coingecko_id": "bitcoin",
                    "symbol": "WBTC",
                    "name": "Wrapped Bitcoin",
                    "market_cap_rank": 1,
                    "platform": "arbitrum-one",
                    "address": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                    "decimals": 8,
                    "total_supply": "150000",
                },
                {
                    "coingecko_id": "arbitrum",
                    "symbol": "ARB",
                    "name": "Arbitrum",
                    "market_cap_rank": 30,
                    "platform": "arbitrum-one",
                    "address": "0x912ce59144191c1204e64559fe8253a0e49e6548",
                    "decimals": 18,
                    "total_supply": "10000000000",
                },
            ],
        }

        with patch.object(
            processor, "_load_coingecko_metadata", return_value=mixed_db_data
        ):
            result = await processor.process_with_all_chains(
                exchange_tokens=mixed_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            tokens = result.data
            metadata = result.metadata

            # Should match 5 out of 6 tokens (all except FAKE)
            assert len(tokens) == 5
            assert metadata["unmatched_tokens"] == 1

            # Check distribution
            multi_chain_count = metadata["multi_chain_tokens"]
            single_chain_count = metadata["single_chain_tokens"]

            assert multi_chain_count > 0  # USDC, BTC, ETH should be multi-chain
            assert single_chain_count > 0  # UNI, ARB should be single-chain

            print("\n✓ Mixed scenario results:")
            print(f"  Matched tokens: {len(tokens)}/{len(mixed_tokens)}")
            print(f"  Multi-chain tokens: {multi_chain_count}")
            print(f"  Single-chain tokens: {single_chain_count}")
            print(f"  Total addresses: {metadata['total_addresses']}")
            print(f"  Coverage by chain: {metadata['coverage_by_chain']}")

            # Verify specific expected behaviors
            usdc = next((t for t in tokens if t["symbol"] == "USDC"), None)
            assert usdc is not None
            assert usdc["chain_count"] == 3  # Should be on all 3 chains

            uni = next((t for t in tokens if t["symbol"] == "UNI"), None)
            assert uni is not None
            assert uni["chain_count"] == 1  # Should only be on Ethereum
            assert "ethereum" in uni["chain_addresses"]

            arb = next((t for t in tokens if t["symbol"] == "ARB"), None)
            assert arb is not None
            assert arb["chain_count"] == 1  # Should only be on Arbitrum
            assert "arbitrum" in arb["chain_addresses"]
