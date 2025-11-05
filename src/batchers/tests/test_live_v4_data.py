"""
Live tests for Uniswap V4 data batch fetcher.

This module tests the UniswapV4DataBatcher with real blockchain calls
using actual pool IDs to verify functionality works end-to-end.
"""

import logging

import pytest
from web3 import Web3

# Set logging level for this module to see detailed output
logging.getLogger().setLevel(logging.INFO)

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.batchers.base import BatchConfig
from src.batchers.uniswap_v4_data import UniswapV4DataBatcher, fetch_uniswap_v4_data
from src.config import ConfigManager

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
def test_pool_ids():
    """Test pool IDs - these are real V4 pool IDs."""
    # Note: These are example pool IDs - in practice, you'd need real V4 pool IDs
    # V4 pools are identified by bytes32 pool IDs derived from pool parameters
    return [
        "0x21c67e77068de97969ba93d4aab21826d33ca12bb9f565d8496e8fda8a82ca27",  # ETH/USDC
        "0x54c72c46df32f2cc455e84e41e191b26ed73a29452cdd3d82f511097af9f427e",  # ETH/WBTC
        "0xb2b5618903d74bbac9e9049a035c3827afc4487cde3b994a1568b050f4c8e2e4",  # WETH/LINK
    ]


class TestLiveV4Data:
    """Live test class for Uniswap V4 data batch fetcher."""

    @pytest.mark.asyncio
    async def test_single_pool(self, web3_connection, test_pool_ids):
        """Test fetching data for a single pool."""
        # Create batcher
        batcher = UniswapV4DataBatcher(web3_connection)

        # Test single pool
        test_pool = [test_pool_ids[0]]
        result = await batcher.batch_call(test_pool)

        # Assertions
        assert result.success, f"Single pool test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 1, f"Expected 1 pool, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify pool data format
        for pool_id, data in result.data.items():
            assert "liquidity" in data, "Missing liquidity"
            assert "sqrtPriceX96" in data, "Missing sqrtPriceX96"
            assert "tick" in data, "Missing tick"
            assert "block_number" in data, "Missing block_number"

            print(f"✅ Pool {pool_id}:")
            print(f"   Liquidity: {data['liquidity']}")
            print(f"   sqrtPriceX96: {data['sqrtPriceX96']}")
            print(f"   Tick: {data['tick']}")
            print(f"   Block: {data['block_number']}")
            logger.info(
                f"✅ Pool {pool_id}: Liquidity={data['liquidity']}, SqrtPrice={data['sqrtPriceX96']}, Tick={data['tick']}, Block={data['block_number']}"
            )

    @pytest.mark.asyncio
    async def test_multiple_pools(self, web3_connection, test_pool_ids):
        """Test fetching data for multiple pools."""
        # Create batcher with smaller batch size
        config = BatchConfig(batch_size=2)
        batcher = UniswapV4DataBatcher(web3_connection, config=config)

        # Test multiple pools
        test_pool_list = test_pool_ids[:3]  # Test 3 pools
        result = await batcher.batch_call(test_pool_list)

        # Assertions
        assert result.success, f"Multiple pools test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 3, f"Expected 3 pools, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify all pools have valid data
        for pool_id, data in result.data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_id}"
            assert "sqrtPriceX96" in data, f"Missing sqrtPriceX96 for pool {pool_id}"
            assert "tick" in data, f"Missing tick for pool {pool_id}"
            assert "block_number" in data, f"Missing block_number for pool {pool_id}"

            print(f"✅ Pool {pool_id}:")
            print(f"   Liquidity: {data['liquidity']}")
            print(f"   Sqrt Price X96: {data['sqrtPriceX96']}")
            print(f"   Tick: {data['tick']}")
            logger.info(
                f"✅ Pool {pool_id}: Liquidity={data['liquidity']}, SqrtPrice={data['sqrtPriceX96']}, Tick={data['tick']}"
            )

        logger.info(f"✅ Successfully processed {len(result.data)} V4 pools")

    @pytest.mark.asyncio
    async def test_chunked_fetch(self, web3_connection, test_pool_ids):
        """Test chunked fetching with many pools."""
        # Create batcher with small batch size to force chunking
        config = BatchConfig(batch_size=1)  # Force individual calls
        batcher = UniswapV4DataBatcher(web3_connection, config=config)

        # Test all pools (should create multiple chunks with batch_size=1)
        pool_data = await batcher.fetch_pools_chunked(test_pool_ids)

        expected_chunks = len(test_pool_ids)

        # Assertions
        assert pool_data, "No data returned from chunked fetch"
        assert len(pool_data) == len(test_pool_ids), (
            f"Expected {len(test_pool_ids)} pools, got {len(pool_data)}"
        )

        # Verify all pools have valid data
        for pool_id, data in pool_data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_id}"
            assert "sqrtPriceX96" in data, f"Missing sqrtPriceX96 for pool {pool_id}"
            assert "tick" in data, f"Missing tick for pool {pool_id}"
            assert "block_number" in data, f"Missing block_number for pool {pool_id}"

        logger.info(
            f"✅ Chunked fetch: {len(pool_data)} pools in {expected_chunks} chunks"
        )

    @pytest.mark.asyncio
    async def test_convenience_function(self, web3_connection, test_pool_ids):
        """Test the convenience function."""
        # Test convenience function
        test_pool_list = test_pool_ids[:2]
        pool_data = await fetch_uniswap_v4_data(
            web3_connection, test_pool_list, batch_size=10
        )

        # Assertions
        assert pool_data, "No data returned from convenience function"
        assert len(pool_data) == 2, f"Expected 2 pools, got {len(pool_data)}"

        # Verify all pools have valid data
        for pool_id, data in pool_data.items():
            assert "liquidity" in data, f"Missing liquidity for pool {pool_id}"
            assert "sqrtPriceX96" in data, f"Missing sqrtPriceX96 for pool {pool_id}"
            assert "block_number" in data, f"Missing block_number for pool {pool_id}"

        logger.info(f"✅ Convenience function: processed {len(pool_data)} pools")

    @pytest.mark.asyncio
    async def test_invalid_pool_ids(self, web3_connection, test_pool_ids):
        """Test handling of invalid pool IDs."""
        batcher = UniswapV4DataBatcher(web3_connection)

        # Mix valid and invalid pool IDs
        mixed_pool_ids = [
            test_pool_ids[0],  # Valid
            "0xinvalid",  # Invalid
            "",  # Empty
            "0x123",  # Too short
            test_pool_ids[1],  # Valid
        ]

        result = await batcher.batch_call(mixed_pool_ids)

        # Should either succeed with only valid pool IDs or fail gracefully
        if result.success:
            # Should process only the valid pool IDs
            expected_valid = 2
            assert len(result.data) == expected_valid, (
                f"Expected {expected_valid} valid pool IDs, got {len(result.data)}"
            )

            # Verify valid pool IDs have correct data
            for pool_id, data in result.data.items():
                assert "liquidity" in data, f"Missing liquidity for pool {pool_id}"
                assert "sqrtPriceX96" in data, (
                    f"Missing sqrtPriceX96 for pool {pool_id}"
                )
                assert "tick" in data, f"Missing tick for pool {pool_id}"

            logger.info(
                f"✅ Filtered invalid pool IDs: {len(result.data)} valid out of {len(mixed_pool_ids)} total"
            )
        else:
            # Should fail gracefully with appropriate error
            assert "No valid pool IDs" in result.error, (
                f"Unexpected error: {result.error}"
            )
            logger.info("✅ Correctly rejected invalid pool IDs")

    @pytest.mark.asyncio
    async def test_empty_pool_id_list(self, web3_connection):
        """Test handling of empty pool ID list."""
        batcher = UniswapV4DataBatcher(web3_connection)

        result = await batcher.batch_call([])

        # Should fail gracefully
        assert not result.success, "Empty pool ID list should fail"
        assert "No valid pool IDs" in result.error, f"Unexpected error: {result.error}"
        assert result.data == {}, "Should return empty data"

        logger.info("✅ Correctly handled empty pool ID list")


# Run with: uv run pytest src/batchers/tests/test_live_v4_data.py -v -s --log-cli-level=INFO
