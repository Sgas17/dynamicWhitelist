"""
End-to-end integration tests demonstrating the complete Hyperliquid → CoinGecko → Whitelist pipeline.

This test suite shows how to:
1. Fetch all Hyperliquid swap markets
2. Match tokens against CoinGecko database across multiple chains
3. Generate comprehensive whitelists for each chain
"""

import pytest
from unittest.mock import patch
from typing import List, Dict, Any

from ..token_matching_processor import TokenMatchingProcessor
from ...fetchers.exchange_fetchers import HyperliquidFetcher, ExchangeToken


class TestEndToEndPipeline:
    """End-to-end integration tests for the complete pipeline."""

    @pytest.fixture
    def comprehensive_hyperliquid_tokens(self) -> List[ExchangeToken]:
        """Comprehensive set of Hyperliquid tokens representing real market data."""
        return [
            # Major cryptocurrencies
            ExchangeToken("BTC/USD", "BTC", "USD", "0", "hyperliquid", True, "swap"),
            ExchangeToken("ETH/USD", "ETH", "USD", "1", "hyperliquid", True, "swap"),
            ExchangeToken("USDC/USD", "USDC", "USD", "2", "hyperliquid", True, "swap"),
            # DeFi tokens
            ExchangeToken("LINK/USD", "LINK", "USD", "3", "hyperliquid", True, "swap"),
            ExchangeToken("UNI/USD", "UNI", "USD", "4", "hyperliquid", True, "swap"),
            ExchangeToken("AAVE/USD", "AAVE", "USD", "5", "hyperliquid", True, "swap"),
            # Layer 2 tokens
            ExchangeToken(
                "MATIC/USD", "MATIC", "USD", "6", "hyperliquid", True, "swap"
            ),
            ExchangeToken("ARB/USD", "ARB", "USD", "7", "hyperliquid", True, "swap"),
            ExchangeToken("OP/USD", "OP", "USD", "8", "hyperliquid", True, "swap"),
            # Meme coins
            ExchangeToken("DOGE/USD", "DOGE", "USD", "9", "hyperliquid", True, "swap"),
            ExchangeToken("PEPE/USD", "PEPE", "USD", "10", "hyperliquid", True, "swap"),
            # Stablecoins
            ExchangeToken("USDT/USD", "USDT", "USD", "11", "hyperliquid", True, "swap"),
            # Tokens that might not have matches
            ExchangeToken(
                "FAKECOIN/USD", "FAKECOIN", "USD", "999", "hyperliquid", True, "swap"
            ),
        ]

    @pytest.fixture
    def comprehensive_coingecko_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Comprehensive CoinGecko database mock covering multiple chains."""
        return {
            "ethereum": [
                # Bitcoin variants
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
                # Ethereum
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
                # Stablecoins
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
                    "coingecko_id": "tether",
                    "symbol": "USDT",
                    "name": "Tether",
                    "market_cap_rank": 3,
                    "platform": "ethereum",
                    "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "decimals": 6,
                    "total_supply": "83000000000",
                },
                # DeFi tokens
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
                {
                    "coingecko_id": "aave",
                    "symbol": "AAVE",
                    "name": "Aave",
                    "market_cap_rank": 25,
                    "platform": "ethereum",
                    "address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                    "decimals": 18,
                    "total_supply": "16000000",
                },
                # Meme coins
                {
                    "coingecko_id": "pepe",
                    "symbol": "PEPE",
                    "name": "Pepe",
                    "market_cap_rank": 35,
                    "platform": "ethereum",
                    "address": "0x6982508145454ce325ddbe47a25d4ec3d2311933",
                    "decimals": 18,
                    "total_supply": "420690000000000",
                },
            ],
            "base": [
                # Stablecoins (cross-chain)
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
                # ETH on Base
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
                # DeFi tokens on Base
                {
                    "coingecko_id": "aave",
                    "symbol": "AAVE",
                    "name": "Aave",
                    "market_cap_rank": 25,
                    "platform": "base",
                    "address": "0x6e3e4fbc98fcce28a95e13d4b1b22b6f6fa9fb2b",
                    "decimals": 18,
                    "total_supply": "16000000",
                },
            ],
            "arbitrum-one": [
                # Bitcoin on Arbitrum
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
                # DeFi tokens
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
                {
                    "coingecko_id": "uniswap",
                    "symbol": "UNI",
                    "name": "Uniswap",
                    "market_cap_rank": 20,
                    "platform": "arbitrum-one",
                    "address": "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0",
                    "decimals": 18,
                    "total_supply": "1000000000",
                },
                # Arbitrum native token
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

    @pytest.mark.asyncio
    async def test_full_pipeline_token_coverage(
        self, comprehensive_hyperliquid_tokens, comprehensive_coingecko_data
    ):
        """Test complete pipeline shows good token coverage across chains."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_coingecko_data,
        ):
            result = await processor.process(
                exchange_tokens=comprehensive_hyperliquid_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            matches = result.data

            # Should have decent match rate
            match_rate = len(matches) / len(comprehensive_hyperliquid_tokens)
            assert match_rate > 0.7  # At least 70% match rate

            # Check coverage statistics
            metadata = result.metadata
            assert metadata["matched_tokens"] > 8  # Should match most major tokens
            assert metadata["unmatched_tokens"] < 5  # Few unmatched tokens

            # Verify chain distribution
            coverage = metadata["coverage_by_chain"]
            assert "ethereum" in coverage
            assert coverage["ethereum"] > 5  # Ethereum should have most tokens

            # Check match types - should have exact and mapped matches
            match_types = metadata["match_type_stats"]
            assert "exact_symbol" in match_types
            assert "mapped_symbol" in match_types

            print(
                f"\n✓ Matched {len(matches)}/{len(comprehensive_hyperliquid_tokens)} tokens"
            )
            print(f"✓ Match rate: {match_rate:.1%}")
            print(f"✓ Chain coverage: {list(coverage.keys())}")
            print(f"✓ Match types: {list(match_types.keys())}")

    @pytest.mark.asyncio
    async def test_whitelist_generation_per_chain(
        self, comprehensive_hyperliquid_tokens, comprehensive_coingecko_data
    ):
        """Test generating whitelist data per chain from matched tokens."""
        processor = TokenMatchingProcessor()

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_coingecko_data,
        ):
            result = await processor.process(
                exchange_tokens=comprehensive_hyperliquid_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.8,  # Higher confidence for whitelist
            )

            assert result.success is True
            matches = result.data

            # Group matches by chain for whitelist generation
            chain_whitelists = {}
            for match in matches:
                chain = match.chain
                if chain not in chain_whitelists:
                    chain_whitelists[chain] = []

                chain_whitelists[chain].append(
                    {
                        "token_symbol": match.symbol,
                        "token_address": match.chain_address,
                        "coingecko_id": match.coingecko_id,
                        "token_name": match.token_name,
                        "decimals": match.decimals,
                        "confidence": match.confidence,
                        "match_type": match.match_type,
                        "exchange": match.exchange_token.exchange,
                        "market_id": match.exchange_token.market_id,
                    }
                )

            # Verify whitelist structure
            assert len(chain_whitelists) > 0

            for chain, tokens in chain_whitelists.items():
                assert len(tokens) > 0

                # Check token structure
                for token in tokens:
                    assert "token_symbol" in token
                    assert "token_address" in token
                    assert "coingecko_id" in token
                    assert token["confidence"] >= 0.8
                    assert token["exchange"] == "hyperliquid"

            print(f"\n✓ Generated whitelists for {len(chain_whitelists)} chains:")
            for chain, tokens in chain_whitelists.items():
                print(f"  {chain}: {len(tokens)} tokens")

                # Show sample tokens
                for i, token in enumerate(tokens[:3]):
                    print(
                        f"    {i + 1}. {token['token_symbol']} ({token['token_address'][:10]}...)"
                    )

    @pytest.mark.asyncio
    async def test_demonstrate_cross_chain_tokens(self, comprehensive_coingecko_data):
        """Demonstrate tokens that exist on multiple chains."""
        processor = TokenMatchingProcessor()

        # Focus on tokens that should exist on multiple chains
        cross_chain_tokens = [
            ExchangeToken("USDC/USD", "USDC", "USD", "1", "hyperliquid", True, "swap"),
            ExchangeToken("BTC/USD", "BTC", "USD", "2", "hyperliquid", True, "swap"),
            ExchangeToken("ETH/USD", "ETH", "USD", "3", "hyperliquid", True, "swap"),
            ExchangeToken("LINK/USD", "LINK", "USD", "4", "hyperliquid", True, "swap"),
        ]

        with patch.object(
            processor,
            "_load_coingecko_metadata",
            return_value=comprehensive_coingecko_data,
        ):
            result = await processor.process(
                exchange_tokens=cross_chain_tokens,
                target_chains=["ethereum", "base", "arbitrum"],
                min_confidence=0.7,
            )

            assert result.success is True
            matches = result.data

            # Group matches by symbol to see cross-chain distribution
            symbol_chains = {}
            for match in matches:
                symbol = match.symbol
                if symbol not in symbol_chains:
                    symbol_chains[symbol] = []
                symbol_chains[symbol].append(match.chain)

            print("\n✓ Cross-chain token analysis:")
            for symbol, chains in symbol_chains.items():
                unique_chains = list(set(chains))
                print(
                    f"  {symbol}: {len(unique_chains)} chains ({', '.join(unique_chains)})"
                )

            # Note: Current implementation takes best match per token,
            # so this shows what the current behavior is
            assert len(matches) == len(cross_chain_tokens)  # One match per input token

    @pytest.mark.asyncio
    async def test_mock_hyperliquid_fetch_integration(
        self, comprehensive_coingecko_data
    ):
        """Test integration between HyperliquidFetcher and TokenMatchingProcessor."""

        # Mock Hyperliquid API response
        mock_markets = [
            {
                "symbol": "BTC/USD",
                "base": "BTC",
                "quote": "USD",
                "id": "0",
                "active": True,
            },
            {
                "symbol": "ETH/USD",
                "base": "ETH",
                "quote": "USD",
                "id": "1",
                "active": True,
            },
            {
                "symbol": "USDC/USD",
                "base": "USDC",
                "quote": "USD",
                "id": "2",
                "active": True,
            },
            {
                "symbol": "LINK/USD",
                "base": "LINK",
                "quote": "USD",
                "id": "3",
                "active": True,
            },
            {
                "symbol": "UNI/USD",
                "base": "UNI",
                "quote": "USD",
                "id": "4",
                "active": True,
            },
        ]

        fetcher = HyperliquidFetcher()
        processor = TokenMatchingProcessor()

        # Step 1: Fetch from Hyperliquid
        with patch.object(
            fetcher.ccxt_exchange, "fetchSwapMarkets", return_value=mock_markets
        ):
            fetch_result = await fetcher.fetch_markets("swap")

            assert fetch_result.success is True
            exchange_tokens = fetch_result.metadata["tokens"]
            assert len(exchange_tokens) == 5

            print(f"\n✓ Fetched {len(exchange_tokens)} tokens from Hyperliquid")

            # Step 2: Process through token matching
            with patch.object(
                processor,
                "_load_coingecko_metadata",
                return_value=comprehensive_coingecko_data,
            ):
                match_result = await processor.process(
                    exchange_tokens=exchange_tokens,
                    target_chains=["ethereum", "base", "arbitrum"],
                    min_confidence=0.7,
                )

                assert match_result.success is True
                matches = match_result.data

                # Should match most tokens
                match_rate = len(matches) / len(exchange_tokens)
                assert match_rate > 0.8  # At least 80% match rate

                print(
                    f"✓ Matched {len(matches)}/{len(exchange_tokens)} tokens ({match_rate:.1%})"
                )
                print(f"✓ Coverage: {match_result.metadata['coverage_by_chain']}")

                # Step 3: Generate final whitelist structure
                whitelist_data = {
                    "metadata": {
                        "source": "hyperliquid",
                        "fetched_tokens": len(exchange_tokens),
                        "matched_tokens": len(matches),
                        "match_rate": f"{match_rate:.1%}",
                        "chains": list(
                            match_result.metadata["coverage_by_chain"].keys()
                        ),
                        "confidence_threshold": 0.7,
                    },
                    "tokens_by_chain": {},
                }

                # Group by chain
                for match in matches:
                    chain = match.chain
                    if chain not in whitelist_data["tokens_by_chain"]:
                        whitelist_data["tokens_by_chain"][chain] = []

                    whitelist_data["tokens_by_chain"][chain].append(
                        {
                            "symbol": match.symbol,
                            "address": match.chain_address,
                            "name": match.token_name,
                            "decimals": match.decimals,
                            "coingecko_id": match.coingecko_id,
                            "confidence": match.confidence,
                            "exchange_market_id": match.exchange_token.market_id,
                        }
                    )

                # Verify final structure
                assert "metadata" in whitelist_data
                assert "tokens_by_chain" in whitelist_data
                assert len(whitelist_data["tokens_by_chain"]) > 0

                print("✓ Generated whitelist structure:")
                for chain, tokens in whitelist_data["tokens_by_chain"].items():
                    print(f"  {chain}: {len(tokens)} tokens")


@pytest.mark.integration
class TestRealDataPipeline:
    """Tests that can be run against real APIs and database."""

    @pytest.mark.asyncio
    async def test_real_hyperliquid_to_database_pipeline(self):
        """
        Test complete pipeline with real Hyperliquid data and database.

        Run with: pytest -m integration -k real_hyperliquid_to_database_pipeline
        """
        pytest.skip("Run manually with real API access and database connection")
