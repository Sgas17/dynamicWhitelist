"""
Live tests for Uniswap V3 data batch fetcher.

This module tests the UniswapV3DataBatcher with real blockchain calls
using actual pool addresses to verify functionality works end-to-end.
"""

import logging

import pytest
from web3 import Web3

# Set logging level for this module to see detailed output
logging.getLogger().setLevel(logging.INFO)

from src.config import ConfigManager

from ..base import BatchConfig
from ..uniswap_v3_data import UniswapV3DataBatcher, fetch_uniswap_v3_data

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def web3_connection():
    """Setup Web3 connection using config manager."""
    try:
        config_manager = ConfigManager()
        # Get Ethereum chain config
        chain_config = config_manager.chains.get_chain_config("ethereum")
        rpc_url = chain_config["rpc_url"]

        logger.info(f"Connecting to: {rpc_url}")
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to {rpc_url}")

        logger.info(f"Connected to chain ID: {web3.eth.chain_id}")
        return web3

    except Exception as e:
        logger.error(f"Failed to setup Web3: {e}")
        raise


@pytest.fixture(scope="class")
def test_pool_addresses():
    """Test pool addresses - these are real V3 pool addresses on Ethereum mainnet."""
    return [
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC/WETH 0.05%
        "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",  # USDC/WETH 0.3%
        "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",  # WETH/WBTC 0.3%
    ]


class TestLiveV3Data:
    """Live test class for Uniswap V3 data batch fetcher."""

    @pytest.mark.asyncio
    async def test_single_pool(self, web3_connection, test_pool_addresses):
        """Test fetching data for a single pool."""
        # Create batcher
        batcher = UniswapV3DataBatcher(web3_connection)

        # Test single pool
        test_pool = [test_pool_addresses[0]]
        result = await batcher.batch_call(test_pool)

        # Assertions
        assert result.success, f"Single pool test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 1, f"Expected 1 pool, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify pool data format
        for pool_address, data in result.data.items():
            assert "liquidity" in data, "Missing liquidity"
            assert "sqrtPriceX96" in data, "Missing sqrtPriceX96"
            assert "tick" in data, "Missing tick"
            assert "block_number" in data, "Missing block_number"

            print(f"✅ Pool {pool_address}:")
            print(f"   Liquidity: {data['liquidity']}")
            print(f"   sqrtPriceX96: {data['sqrtPriceX96']}")
            print(f"   Tick: {data['tick']}")
            print(f"   Block: {data['block_number']}")
            logger.info(
                f"✅ Pool {pool_address}: Liquidity={data['liquidity']}, SqrtPrice={data['sqrtPriceX96']}, Tick={data['tick']}, Block={data['block_number']}"
            )

    @pytest.mark.asyncio
    async def test_multiple_pools(self, web3_connection, test_pool_addresses):
        """Test fetching data for multiple pools."""
        # Create batcher with smaller batch size
        config = BatchConfig(batch_size=2)
        batcher = UniswapV3DataBatcher(web3_connection, config=config)

        # Test multiple pools
        test_pool_list = test_pool_addresses[:3]  # Test 3 pools
        result = await batcher.batch_call(test_pool_list)

        # Assertions
        assert result.success, f"Multiple pools test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 3, f"Expected 3 pools, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify all pools have valid data
        for pool_address, data in result.data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_address}"
            assert "sqrtPriceX96" in data, (
                f"Missing sqrtPriceX96 for pool {pool_address}"
            )
            assert "tick" in data, f"Missing tick for pool {pool_address}"
            assert "block_number" in data, (
                f"Missing block_number for pool {pool_address}"
            )

            print(f"✅ Pool {pool_address}:")
            print(f"   Liquidity: {data['liquidity']}")
            print(f"   Sqrt Price X96: {data['sqrtPriceX96']}")
            print(f"   Tick: {data['tick']}")
            logger.info(
                f"✅ Pool {pool_address}: Liquidity={data['liquidity']}, SqrtPrice={data['sqrtPriceX96']}, Tick={data['tick']}"
            )

        logger.info(f"✅ Successfully processed {len(result.data)} V3 pools")

    @pytest.mark.asyncio
    async def test_chunked_fetch(self, web3_connection, test_pool_addresses):
        """Test chunked fetching with many pools."""
        # Create batcher with small batch size to force chunking
        config = BatchConfig(batch_size=1)  # Force individual calls
        batcher = UniswapV3DataBatcher(web3_connection, config=config)

        # Test all pools (should create multiple chunks with batch_size=1)
        pool_data = await batcher.fetch_pools_chunked(test_pool_addresses)

        expected_chunks = len(test_pool_addresses)

        # Assertions
        assert pool_data, "No data returned from chunked fetch"
        assert len(pool_data) == len(test_pool_addresses), (
            f"Expected {len(test_pool_addresses)} pools, got {len(pool_data)}"
        )

        # Verify all pools have valid data
        for pool_address, data in pool_data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_address}"
            assert "sqrtPriceX96" in data, (
                f"Missing sqrtPriceX96 for pool {pool_address}"
            )
            assert "tick" in data, f"Missing tick for pool {pool_address}"
            assert "block_number" in data, (
                f"Missing block_number for pool {pool_address}"
            )

        logger.info(
            f"✅ Chunked fetch: {len(pool_data)} pools in {expected_chunks} chunks"
        )

    @pytest.mark.asyncio
    async def test_convenience_function(self, web3_connection, test_pool_addresses):
        """Test the convenience function."""
        # Test convenience function
        test_pool_list = test_pool_addresses[:2]
        pool_data = await fetch_uniswap_v3_data(
            web3_connection, test_pool_list, batch_size=10
        )

        # Assertions
        assert pool_data, "No data returned from convenience function"
        assert len(pool_data) == 2, f"Expected 2 pools, got {len(pool_data)}"

        # Verify all pools have valid data
        for pool_address, data in pool_data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_address}"
            assert "sqrtPriceX96" in data, (
                f"Missing sqrtPriceX96 for pool {pool_address}"
            )
            assert "block_number" in data, (
                f"Missing block_number for pool {pool_address}"
            )

        logger.info(f"✅ Convenience function: processed {len(pool_data)} pools")

    @pytest.mark.asyncio
    async def test_invalid_pool_addresses(self, web3_connection, test_pool_addresses):
        """Test handling of invalid pool addresses."""
        batcher = UniswapV3DataBatcher(web3_connection)

        # Mix valid and invalid pool addresses
        mixed_pool_addresses = [
            test_pool_addresses[0],  # Valid
            "0xinvalid",  # Invalid
            "",  # Empty
            "0x123",  # Too short
            test_pool_addresses[1],  # Valid
        ]

        result = await batcher.batch_call(mixed_pool_addresses)

        # Should either succeed with only valid pool addresses or fail gracefully
        if result.success:
            # Should process only the valid pool addresses
            expected_valid = 2
            assert len(result.data) == expected_valid, (
                f"Expected {expected_valid} valid pool addresses, got {len(result.data)}"
            )

            # Verify valid pool addresses have correct data
            for pool_address, data in result.data.items():
                assert "liquidity" in data, f"Missing liquidity for pool {pool_address}"
                assert "sqrtPriceX96" in data, (
                    f"Missing sqrtPriceX96 for pool {pool_address}"
                )
                assert "tick" in data, f"Missing tick for pool {pool_address}"

            logger.info(
                f"✅ Filtered invalid pool addresses: {len(result.data)} valid out of {len(mixed_pool_addresses)} total"
            )
        else:
            # Should fail gracefully with appropriate error
            assert "No valid pool addresses" in result.error, (
                f"Unexpected error: {result.error}"
            )
            logger.info("✅ Correctly rejected invalid pool addresses")

    @pytest.mark.asyncio
    async def test_empty_pool_address_list(self, web3_connection):
        """Test handling of empty pool address list."""
        batcher = UniswapV3DataBatcher(web3_connection)

        result = await batcher.batch_call([])

        # Should fail gracefully
        assert not result.success, "Empty pool address list should fail"
        assert "No valid pool addresses" in result.error, (
            f"Unexpected error: {result.error}"
        )
        assert result.data == {}, "Should return empty data"

        logger.info("✅ Correctly handled empty pool address list")


# Run with: uv run pytest src/batchers/tests/test_live_v3_data.py -v -s --log-cli-level=INFO
