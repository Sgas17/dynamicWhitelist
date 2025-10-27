"""
Live tests for Uniswap V2 reserves batch fetcher.

This module tests the UniswapV2ReservesBatcher with real blockchain calls
using actual pair addresses to verify functionality works end-to-end.
"""

import pytest
import logging
from web3 import Web3

# Set logging level for this module to see detailed output
logging.getLogger().setLevel(logging.INFO)

from src.config import ConfigManager
from ..uniswap_v2_reserves import UniswapV2ReservesBatcher, fetch_uniswap_v2_reserves
from ..base import BatchConfig

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def web3_connection():
    """Setup Web3 connection using config manager."""
    try:
        config_manager = ConfigManager()
        # Get Ethereum chain config
        chain_config = config_manager.chains.get_chain_config('ethereum')
        rpc_url = chain_config['rpc_url']

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
def test_pairs():
    """Test pair addresses - well-known Uniswap V2 pairs."""
    return [
        "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",  # ETH/USDC
        "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",  # ETH/USDT
        "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",  # ETH/DAI
        "0xbb2b8038a1640196fbe3e38816f3e67cba72d940",  # WBTC/ETH
        "0xd3d2e2692501a5c9ca623199d38826e513033a17"   # UNI/ETH
    ]


class TestLiveReserves:
    """Live test class for Uniswap V2 reserves batch fetcher."""


    @pytest.mark.asyncio
    async def test_single_pair(self, web3_connection, test_pairs):
        """Test fetching reserves for a single pair."""
        # Create batcher
        batcher = UniswapV2ReservesBatcher(web3_connection)

        # Test single pair
        test_pair = [test_pairs[0]]
        result = await batcher.batch_call(test_pair)

        # Assertions
        assert result.success, f"Single pair test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 1, f"Expected 1 pair, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify reserve data format
        for pair, data in result.data.items():
            assert 'reserve0' in data, "Missing reserve0"
            assert 'reserve1' in data, "Missing reserve1"

            reserve0 = int(data['reserve0'], 16)
            reserve1 = int(data['reserve1'], 16)

            assert reserve0 >= 0, "Invalid reserve0"
            assert reserve1 >= 0, "Invalid reserve1"

            print(f"✅ Pair {pair}: R0={reserve0}, R1={reserve1}")
            logger.info(f"✅ Pair {pair}: R0={reserve0}, R1={reserve1}")

    @pytest.mark.asyncio
    async def test_multiple_pairs(self, web3_connection, test_pairs):
        """Test fetching reserves for multiple pairs."""
        # Create batcher with smaller batch size
        config = BatchConfig(batch_size=3)
        batcher = UniswapV2ReservesBatcher(web3_connection, config=config)

        # Test multiple pairs
        test_pair_list = test_pairs[:4]  # Test 4 pairs
        result = await batcher.batch_call(test_pair_list)

        # Assertions
        assert result.success, f"Multiple pairs test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 4, f"Expected 4 pairs, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify all pairs have valid reserve data
        for pair, data in result.data.items():
            assert 'reserve0' in data, f"Missing reserve0 for pair {pair}"
            assert 'reserve1' in data, f"Missing reserve1 for pair {pair}"

            reserve0 = int(data['reserve0'], 16)
            reserve1 = int(data['reserve1'], 16)

            assert reserve0 >= 0, f"Invalid reserve0 for pair {pair}"
            assert reserve1 >= 0, f"Invalid reserve1 for pair {pair}"

        logger.info(f"✅ Successfully processed {len(result.data)} pairs")

    @pytest.mark.asyncio
    async def test_chunked_fetch(self, web3_connection, test_pairs):
        """Test chunked fetching with many pairs."""
        # Create batcher with small batch size to force chunking
        config = BatchConfig(batch_size=2)
        batcher = UniswapV2ReservesBatcher(web3_connection, config=config)

        # Test all pairs (should create multiple chunks with batch_size=2)
        reserves = await batcher.fetch_reserves_chunked(test_pairs)

        expected_chunks = len(test_pairs) // 2 + (1 if len(test_pairs) % 2 else 0)

        # Assertions
        assert reserves, "No data returned from chunked fetch"
        assert len(reserves) == len(test_pairs), f"Expected {len(test_pairs)} pairs, got {len(reserves)}"

        # Verify all pairs have valid reserve data
        for pair, data in reserves.items():
            assert 'reserve0' in data, f"Missing reserve0 for pair {pair}"
            assert 'reserve1' in data, f"Missing reserve1 for pair {pair}"

            reserve0 = int(data['reserve0'], 16)
            reserve1 = int(data['reserve1'], 16)

            assert reserve0 >= 0, f"Invalid reserve0 for pair {pair}"
            assert reserve1 >= 0, f"Invalid reserve1 for pair {pair}"

        # Calculate totals for logging
        total_reserve0 = sum(int(data['reserve0'], 16) for data in reserves.values())
        total_reserve1 = sum(int(data['reserve1'], 16) for data in reserves.values())

        logger.info(f"✅ Chunked fetch: {len(reserves)} pairs in {expected_chunks} chunks")
        logger.info(f"   Total reserves: R0={total_reserve0:,}, R1={total_reserve1:,}")

    @pytest.mark.asyncio
    async def test_convenience_function(self, web3_connection, test_pairs):
        """Test the convenience function."""
        # Test convenience function
        test_pair_list = test_pairs[:3]
        reserves = await fetch_uniswap_v2_reserves(
            web3_connection,
            test_pair_list,
            batch_size=10
        )

        # Assertions
        assert reserves, "No data returned from convenience function"
        assert len(reserves) == 3, f"Expected 3 pairs, got {len(reserves)}"

        # Verify all pairs have valid reserve data
        for pair, data in reserves.items():
            assert 'reserve0' in data, f"Missing reserve0 for pair {pair}"
            assert 'reserve1' in data, f"Missing reserve1 for pair {pair}"

            reserve0 = int(data['reserve0'], 16)
            reserve1 = int(data['reserve1'], 16)

            assert reserve0 >= 0, f"Invalid reserve0 for pair {pair}"
            assert reserve1 >= 0, f"Invalid reserve1 for pair {pair}"

        logger.info(f"✅ Convenience function: processed {len(reserves)} pairs")

    @pytest.mark.asyncio
    async def test_invalid_addresses(self, web3_connection, test_pairs):
        """Test handling of invalid addresses."""
        batcher = UniswapV2ReservesBatcher(web3_connection)

        # Mix valid and invalid addresses
        mixed_addresses = [
            test_pairs[0],  # Valid
            "0xinvalid",    # Invalid
            "",             # Empty
            test_pairs[1]   # Valid
        ]

        result = await batcher.batch_call(mixed_addresses)

        # Should either succeed with only valid addresses or fail gracefully
        if result.success:
            # Should process only the valid addresses
            expected_valid = 2
            assert len(result.data) == expected_valid, f"Expected {expected_valid} valid addresses, got {len(result.data)}"

            # Verify valid addresses have correct data
            for pair, data in result.data.items():
                assert 'reserve0' in data, f"Missing reserve0 for pair {pair}"
                assert 'reserve1' in data, f"Missing reserve1 for pair {pair}"

            logger.info(f"✅ Filtered invalid addresses: {len(result.data)} valid out of {len(mixed_addresses)} total")
        else:
            # Should fail gracefully with appropriate error
            assert "No valid addresses" in result.error, f"Unexpected error: {result.error}"
            logger.info("✅ Correctly rejected invalid addresses")

    @pytest.mark.asyncio
    async def test_empty_address_list(self, web3_connection):
        """Test handling of empty address list."""
        batcher = UniswapV2ReservesBatcher(web3_connection)

        result = await batcher.batch_call([])

        # Should fail gracefully
        assert not result.success, "Empty address list should fail"
        assert "No valid addresses" in result.error, f"Unexpected error: {result.error}"
        assert result.data == {}, "Should return empty data"

        logger.info("✅ Correctly handled empty address list")

# Run with: uv run pytest src/batchers/tests/test_live_reserves.py -v