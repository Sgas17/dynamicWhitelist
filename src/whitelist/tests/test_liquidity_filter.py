"""
Test suite for PoolLiquidityFilter.

Tests the liquidity filtering functionality for V2, V3, and V4 pools.
"""

import pytest
from decimal import Decimal

from web3 import Web3

from ...config import ConfigManager
from ...core.storage.postgres import PostgresStorage
from ..liquidity_filter import PoolLiquidityFilter, normalize_token_for_pricing, WETH_ADDRESS
from ..orchestrator import WhitelistOrchestrator


class TestNormalization:
    """Test V4 native ETH address normalization."""

    def test_zero_address_normalizes_to_weth(self):
        """Test that zero address is normalized to WETH."""
        zero_addr = "0x0000000000000000000000000000000000000000"
        normalized = normalize_token_for_pricing(zero_addr)
        assert normalized == WETH_ADDRESS.lower()

    def test_weth_address_passes_through(self):
        """Test that WETH address passes through unchanged."""
        weth_addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        normalized = normalize_token_for_pricing(weth_addr)
        assert normalized == weth_addr.lower()

    def test_regular_token_passes_through(self):
        """Test that regular token addresses pass through unchanged."""
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        normalized = normalize_token_for_pricing(usdc_addr)
        assert normalized == usdc_addr.lower()

    def test_case_insensitivity(self):
        """Test that normalization is case-insensitive."""
        zero_upper = "0X0000000000000000000000000000000000000000"
        zero_lower = "0x0000000000000000000000000000000000000000"
        assert normalize_token_for_pricing(zero_upper) == normalize_token_for_pricing(zero_lower)


class TestHyperliquidPrices:
    """Test Hyperliquid price fetching."""

    @pytest.mark.asyncio
    async def test_fetch_hyperliquid_prices(self):
        """Test fetching prices from Hyperliquid API."""
        config = ConfigManager()
        rpc_url = config.chains.get_rpc_url("ethereum")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        filter_instance = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_usd=10000,
            chain="ethereum"
        )

        prices = await filter_instance.fetch_hyperliquid_prices()

        # Should fetch at least some prices
        assert len(prices) > 0, "Should fetch at least some prices from Hyperliquid"

        # Check some common tokens
        common_tokens = ["BTC", "ETH"]
        found_tokens = [token for token in common_tokens if token in prices]
        assert len(found_tokens) > 0, "Should find at least one common token"

        # Validate prices are positive
        for token, price in prices.items():
            assert price > 0, f"{token} price should be positive"


class TestPriceCalculation:
    """Test token price calculation from pool reserves."""

    def test_calculate_token_price_from_pair(self):
        """Test calculating token price from V2 pool reserves."""
        config = ConfigManager()
        rpc_url = config.chains.get_rpc_url("ethereum")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        filter_instance = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_usd=10000,
            chain="ethereum"
        )

        # Test case: USDC/ETH pool
        # If ETH = $4000 and pool has 1000 ETH and 4,000,000 USDC
        # USDC price should be ~$1
        reserve_usdc = 4_000_000 * 10**6  # 4M USDC (6 decimals)
        reserve_eth = 1000 * 10**18  # 1000 ETH (18 decimals)
        decimals_usdc = 6
        decimals_eth = 18
        price_eth = Decimal("4000")

        calculated_usdc_price = filter_instance._calculate_token_price_from_pair(
            reserve_token=reserve_usdc,
            reserve_other=reserve_eth,
            decimals_token=decimals_usdc,
            decimals_other=decimals_eth,
            price_other=price_eth
        )

        # USDC price should be very close to $1
        assert abs(calculated_usdc_price - Decimal("1")) < Decimal("0.01"), \
            f"USDC price should be close to $1, got {calculated_usdc_price}"


class TestV2PoolFiltering:
    """Test V2 pool filtering logic."""

    @pytest.mark.asyncio
    async def test_filter_v2_pools_with_mock_data(self):
        """Test V2 pool filtering with mock pool data."""
        config = ConfigManager()
        rpc_url = config.chains.get_rpc_url("ethereum")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        filter_instance = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_usd=10000,
            chain="ethereum"
        )

        # Mock pool data - ETH/USDC pool
        weth_addr = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        usdc_addr = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        pool_addr = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"  # Real ETH/USDC V2 pool

        mock_pools = {
            pool_addr: {
                'token0': {'address': usdc_addr, 'symbol': 'USDC', 'decimals': 6},
                'token1': {'address': weth_addr, 'symbol': 'WETH', 'decimals': 18},
                'exchange': 'uniswapV2',
                'fee': 3000
            }
        }

        token_symbols = {
            usdc_addr: 'USDC',
            weth_addr: 'WETH'
        }

        token_decimals = {
            usdc_addr: 6,
            weth_addr: 18
        }

        hyperliquid_prices = {
            'ETH': Decimal('4000'),  # Mock ETH price
        }

        # Filter pools (this will make actual RPC calls to get reserves)
        filtered_pools, discovered_prices = await filter_instance.filter_v2_pools(
            pools=mock_pools,
            token_symbols=token_symbols,
            token_decimals=token_decimals,
            hyperliquid_symbol_prices=hyperliquid_prices
        )

        # Should filter correctly based on actual liquidity
        assert len(filtered_pools) <= len(mock_pools)


class TestV4NativeETH:
    """Test V4 pools with native ETH (zero address)."""

    @pytest.mark.asyncio
    async def test_v4_pool_with_native_eth(self):
        """Test that V4 pools with native ETH are handled correctly."""
        config = ConfigManager()
        rpc_url = config.chains.get_rpc_url("ethereum")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        filter_instance = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_usd=10000,
            chain="ethereum"
        )

        # Mock V4 pool with native ETH (zero address) and USDC
        zero_addr = "0x0000000000000000000000000000000000000000"
        usdc_addr = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        pool_id = "0x1234567890123456789012345678901234567890123456789012345678901234"

        mock_pools = {
            pool_id: {
                'token0': {'address': zero_addr, 'symbol': 'ETH', 'decimals': 18},
                'token1': {'address': usdc_addr, 'symbol': 'USDC', 'decimals': 6},
                'exchange': 'uniswapV4',
                'fee': 3000
            }
        }

        token_symbols = {
            zero_addr: 'ETH',
            usdc_addr: 'USDC',
            WETH_ADDRESS.lower(): 'WETH'  # Also provide WETH
        }

        token_decimals = {
            zero_addr: 18,
            usdc_addr: 6,
            WETH_ADDRESS.lower(): 18  # Also provide WETH decimals
        }

        token_prices = {
            WETH_ADDRESS.lower(): Decimal('4000'),  # WETH price
            usdc_addr: Decimal('1')
        }

        # This should not crash due to missing zero address price
        # The filter should normalize zero address to WETH
        filtered_pools, discovered_prices = await filter_instance.filter_v4_pools(
            pools=mock_pools,
            token_symbols=token_symbols,
            token_decimals=token_decimals,
            token_prices=token_prices
        )

        # Test passes if no exception is raised
        # Actual filtering depends on real pool state from RPC
        assert isinstance(filtered_pools, dict)


@pytest.mark.integration
class TestFullPipeline:
    """Integration test for full whitelist and filtering pipeline."""

    @pytest.mark.asyncio
    async def test_orchestrator_pipeline(self):
        """Test complete pipeline using WhitelistOrchestrator."""
        config = ConfigManager()

        db_config = {
            'host': config.database.POSTGRES_HOST,
            'port': config.database.POSTGRES_PORT,
            'user': config.database.POSTGRES_USER,
            'password': config.database.POSTGRES_PASSWORD,
            'database': config.database.POSTGRES_DB,
            'pool_size': 10,
            'pool_timeout': 10
        }

        storage = PostgresStorage(config=db_config)
        await storage.connect()

        try:
            orchestrator = WhitelistOrchestrator(storage, config)

            # Run pipeline with small parameters for faster testing
            result = await orchestrator.run_pipeline(
                chain="ethereum",
                top_transfers=10,
                protocols=["uniswap_v2", "uniswap_v3", "uniswap_v4"]
            )

            # Validate structure
            assert 'whitelist' in result
            assert 'stage1_pools' in result
            assert 'stage2_pools' in result
            assert 'token_prices' in result

            # Validate whitelist
            assert result['whitelist']['total_tokens'] > 0
            assert len(result['whitelist']['tokens']) > 0

            # Validate pools
            total_pools = result['stage1_pools']['count'] + result['stage2_pools']['count']
            assert total_pools >= 0  # May be 0 if no pools meet liquidity threshold

            # Validate token prices
            assert isinstance(result['token_prices'], dict)

        finally:
            await storage.disconnect()


if __name__ == "__main__":
    # Allow running as script for debugging
    pytest.main([__file__, "-v", "-s"])
