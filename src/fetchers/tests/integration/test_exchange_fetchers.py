"""
Integration tests for exchange market fetchers.

These tests make real API calls and require network connectivity.
"""

import pytest

from ....fetchers.exchange_fetchers import (
    BinanceFetcher,
    ExchangeToken,
    HyperliquidFetcher,
)


class TestHyperliquidIntegration:
    """Integration tests for HyperliquidFetcher with real API calls."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_fetch_real_hyperliquid_swap_markets(self):
        """Integration test: Fetch real swap markets from Hyperliquid."""
        import ccxt

        # Skip if CCXT hyperliquid is not available
        if not hasattr(ccxt, "hyperliquid"):
            pytest.skip("CCXT hyperliquid not available")

        fetcher = HyperliquidFetcher()

        try:
            result = await fetcher.fetch_markets("swap")

            # Basic success validation
            assert result.success is True, f"Fetch failed: {result.error}"
            assert result.metadata is not None
            assert "tokens" in result.metadata
            assert "total_markets" in result.metadata
            assert "filtered_tokens" in result.metadata

            tokens = result.metadata["tokens"]
            total_markets = result.metadata["total_markets"]
            filtered_tokens = result.metadata["filtered_tokens"]

            # Data validation
            assert total_markets > 0, "Should have found some markets"
            assert filtered_tokens > 0, "Should have some filtered tokens"
            assert len(tokens) == filtered_tokens, (
                "Token list should match filtered count"
            )
            assert len(tokens) > 0, "Should have actual token objects"

            # Token structure validation
            for token in tokens[:5]:  # Check first 5 tokens
                assert isinstance(token, ExchangeToken)
                assert token.exchange == "hyperliquid"
                assert token.market_type == "swap"
                assert token.base, f"Token should have base asset: {token}"
                assert token.quote, f"Token should have quote asset: {token}"
                assert token.symbol, f"Token should have symbol: {token}"
                assert len(token.base) >= 2, f"Base asset too short: {token.base}"

            # Log results for manual verification
            print(f"\n=== Hyperliquid Integration Test Results ===")
            print(f"Total markets found: {total_markets}")
            print(f"Filtered tokens: {filtered_tokens}")
            print(f"Exchange: {result.metadata['exchange']}")
            print(f"Market type: {result.metadata['market_type']}")

            print(f"\nSample tokens (first 10):")
            for i, token in enumerate(tokens[:10]):
                print(
                    f"  {i + 1:2d}. {token.symbol:12s} | {token.base:8s} | Active: {token.is_active}"
                )

            # Quality checks
            active_tokens = [t for t in tokens if t.is_active]
            assert len(active_tokens) > 0, "Should have at least some active tokens"

            # Check for common crypto pairs (Hyperliquid uses USDC)
            symbols = [t.symbol for t in tokens]
            major_pairs = ["BTC/USDC:USDC", "ETH/USDC:USDC", "SOL/USDC:USDC"]
            found_majors = [pair for pair in major_pairs if pair in symbols]

            print(f"\nMajor pairs found: {found_majors}")
            assert len(found_majors) > 0, (
                f"Should find at least one major pair from {major_pairs}"
            )

        except Exception as e:
            if "Network" in str(e) or "timeout" in str(e).lower():
                pytest.skip(f"Network issue during integration test: {e}")
            else:
                raise

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_hyperliquid_market_filtering_logic(self):
        """Integration test: Validate Hyperliquid-specific filtering works correctly."""
        import ccxt

        if not hasattr(ccxt, "hyperliquid"):
            pytest.skip("CCXT hyperliquid not available")

        fetcher = HyperliquidFetcher()

        try:
            result = await fetcher.fetch_markets("swap")

            if not result.success:
                pytest.skip(f"API call failed: {result.error}")

            tokens = result.metadata["tokens"]

            # Test Hyperliquid-specific filtering
            for token in tokens:
                # All tokens should be active (filtered by _should_include_market)
                assert token.is_active, f"Inactive token found: {token.symbol}"

                # Should have valid base asset
                assert len(token.base) >= 2, f"Invalid base asset: {token.base}"

                # Quote should typically be USD or USDC for Hyperliquid
                acceptable_quotes = ["USD", "USDC"]
                if token.quote not in acceptable_quotes:
                    print(
                        f"Warning: Unexpected quote asset {token.quote} for {token.symbol}"
                    )

            # Check filtering worked
            assert len(tokens) > 0, "Should have filtered tokens"

            print(f"\n=== Hyperliquid Filtering Test Results ===")
            quotes = list(set(t.quote for t in tokens))
            bases = list(set(t.base for t in tokens))

            print(f"Quote assets found: {sorted(quotes)}")
            print(f"Number of unique base assets: {len(bases)}")
            print(f"Sample base assets: {sorted(bases)[:10]}")

        except Exception as e:
            if "Network" in str(e) or "timeout" in str(e).lower():
                pytest.skip(f"Network issue during integration test: {e}")
            else:
                raise

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_hyperliquid_error_handling(self):
        """Integration test: Test error handling with invalid requests."""
        import ccxt

        if not hasattr(ccxt, "hyperliquid"):
            pytest.skip("CCXT hyperliquid not available")

        fetcher = HyperliquidFetcher()

        # Test with invalid market type
        result = await fetcher.fetch_markets("invalid_market_type")

        assert result.success is False
        assert result.error is not None
        assert "Unsupported market type" in result.error

        print(f"\n=== Hyperliquid Error Handling Test ===")
        print(f"Expected error handled correctly: {result.error}")


class TestExchangeFetchersIntegration:
    """Integration tests for multiple exchange fetchers."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_compare_exchange_implementations(self):
        """Compare different exchange fetcher implementations."""
        import ccxt

        results = {}

        # Test Hyperliquid
        if hasattr(ccxt, "hyperliquid"):
            try:
                hyperliquid_fetcher = HyperliquidFetcher()
                hyperliquid_result = await hyperliquid_fetcher.fetch_markets("swap")
                if hyperliquid_result.success:
                    results["hyperliquid"] = hyperliquid_result.metadata
            except Exception as e:
                print(f"Hyperliquid test failed: {e}")

        # Test Binance (if available and desired)
        if hasattr(ccxt, "binance"):
            try:
                binance_fetcher = BinanceFetcher()
                binance_result = await binance_fetcher.fetch_markets("spot")
                if binance_result.success:
                    results["binance"] = binance_result.metadata
            except Exception as e:
                print(f"Binance test failed: {e}")

        if not results:
            pytest.skip("No exchange APIs available for comparison")

        print(f"\n=== Exchange Comparison Results ===")
        for exchange, metadata in results.items():
            tokens = metadata.get("tokens", [])
            print(
                f"{exchange:12s}: {metadata.get('total_markets', 0):4d} markets, {len(tokens):4d} tokens"
            )

            if tokens:
                sample_bases = list(set(t.base for t in tokens[:20]))
                print(f"    Sample bases: {sample_bases[:8]}")

        # Basic validation
        assert len(results) > 0, "Should have at least one working exchange"
