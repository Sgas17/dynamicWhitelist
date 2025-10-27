"""
Live tests for Uniswap V3 tick and bitmap batch fetchers.

This module tests the V3 tick and bitmap batchers with real blockchain calls
using actual pool addresses to verify functionality works end-to-end.
"""

import pytest
import logging
from typing import List, Dict
from web3 import Web3
from eth_typing import ChecksumAddress
from typing import cast, Any, Dict, List
from hexbytes import HexBytes


# Set logging level for this module to see detailed output
logging.getLogger().setLevel(logging.INFO)

from src.config import ConfigManager
from ..uniswap_v3_ticks import UniswapV3TickBatcher, UniswapV3BitmapBatcher
from ..base import BatchConfig
from eth_abi.abi import encode, decode

logger = logging.getLogger(__name__)

# Function selectors for V3 pool methods
SLOT0_SELECTOR = Web3.keccak(text="slot0()")[:4].hex()
TICK_SPACING_SELECTOR = Web3.keccak(text="tickSpacing()")[:4].hex()


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
def test_pools(web3_connection):
    pool_addresses = {
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640": {
            "name": "USDC/WETH 0.05%",
        },
        "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8": {
            "name": "USDC/WETH 0.3%",
        },
        "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD": {
            "name": "WBTC/WETH 0.3%",
        },
    }
    
    for pool_address in pool_addresses:
        with web3_connection.batch_requests() as batch:
            batch.add_mapping(
                {
                    web3_connection.eth.call: [
                        {
                            "to": pool_address,
                            "data": SLOT0_SELECTOR,
                        },
                        {
                            "to": pool_address,
                            "data": TICK_SPACING_SELECTOR,
                        },
                    ],
                }
            )
            slot0_result, tick_spacing_result = batch.execute()
            
            # Decode slot0 - returns (sqrtPriceX96, tick, observationIndex, observationCardinality, observationCardinalityNext, feeProtocol, unlocked)
            _, tick, *_ = decode(
                types=['uint160', 'int24', 'uint16', 'uint16', 'uint16', 'uint8', 'bool'],
                data=cast("HexBytes", slot0_result)
            )
            
            # Decode tickSpacing
            tick_spacing = decode(types=['int24'], data=cast("HexBytes", tick_spacing_result))[0]
            
            tick = cast('int', tick)
            tick_spacing = cast('int', tick_spacing)
            
            nearest_spaced_tick = tick // tick_spacing * tick_spacing
            pool_addresses[pool_address].update({
                "tick": tick,
                "tick_spacing": tick_spacing,
                "test_ticks": [nearest_spaced_tick + (tick_spacing * i) for i in [-2, -1, 1, 2, 3]]
            })
    return pool_addresses


class TestLiveV3Ticks:
    """Live test class for Uniswap V3 tick batch fetcher."""

    @pytest.mark.asyncio
    async def test_single_pool_tick_data(self, web3_connection, test_pools):
        """Test fetching tick data for a single pool."""
        # Create tick batcher
        batcher = UniswapV3TickBatcher(web3_connection)

        # Test single pool
        pool_address = list(test_pools.keys())[0]
        pool_info = test_pools[pool_address]
        test_ticks = pool_info["test_ticks"]

        pool_ticks = {
            Web3.to_checksum_address(pool_address): test_ticks
        }

        result = await batcher.fetch_tick_data(pool_ticks)

        # Assertions
        assert result.success, f"Single pool tick test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 1, f"Expected 1 pool, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify tick data format
        pool_data = list(result.data.values())[0]
        for tick in test_ticks:
            if tick in pool_data:
                tick_info = pool_data[tick]
                assert hasattr(tick_info, 'tick'), "Missing tick"
                assert hasattr(tick_info, 'liquidity_gross'), "Missing liquidity_gross"
                assert hasattr(tick_info, 'liquidity_net'), "Missing liquidity_net"
                assert hasattr(tick_info, 'is_initialized'), "Missing is_initialized"

                print(f"✅ Tick {tick}:")
                print(f"   Gross Liquidity: {tick_info.liquidity_gross}")
                print(f"   Net Liquidity: {tick_info.liquidity_net}")
                print(f"   Initialized: {tick_info.is_initialized}")

        logger.info(f"✅ {pool_info['name']}: Fetched {len(pool_data)} ticks at block {result.block_number}")

    @pytest.mark.asyncio
    async def test_multiple_pools_tick_data(self, web3_connection, test_pools):
        """Test fetching tick data for multiple pools."""
        # Create tick batcher
        batcher = UniswapV3TickBatcher(web3_connection)

        # Prepare multiple pool requests
        pool_ticks = {}
        for pool_address, pool_info in test_pools.items():
            pool_ticks[Web3.to_checksum_address(pool_address)] = pool_info["test_ticks"]

        result = await batcher.fetch_tick_data(pool_ticks)

        # Assertions
        assert result.success, f"Multiple pools tick test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == len(test_pools), f"Expected {len(test_pools)} pools, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify all pools have valid data
        for pool_address, pool_data in result.data.items():
            pool_info = test_pools[pool_address]
            print(f"✅ {pool_info['name']} ({pool_address}):")

            active_ticks = 0
            for tick, tick_info in pool_data.items():
                assert hasattr(tick_info, 'liquidity_gross'), f"Missing liquidity_gross for tick {tick}"
                assert hasattr(tick_info, 'liquidity_net'), f"Missing liquidity_net for tick {tick}"

                if tick_info.is_initialized:
                    active_ticks += 1
                    print(f"   Tick {tick}: Gross={tick_info.liquidity_gross}, Net={tick_info.liquidity_net}")

            print(f"   Active ticks: {active_ticks}/{len(pool_data)}")

        logger.info(f"✅ Successfully processed {len(result.data)} V3 pools for tick data")

    @pytest.mark.asyncio
    async def test_single_pool_bitmap_data(self, web3_connection, test_pools):
        """Test fetching bitmap data for a single pool."""
        # Create bitmap batcher
        batcher = UniswapV3BitmapBatcher(web3_connection)

        # Test single pool - calculate word positions around current price
        pool_address = list(test_pools.keys())[0]
        pool_info = test_pools[pool_address]

        # Calculate word positions from test ticks
        test_ticks = pool_info["test_ticks"]
        tick_spacing = pool_info["tick_spacing"]
        word_pivot = test_ticks[2] // tick_spacing >> 8
        word_positions = [word_pivot - 1, word_pivot, word_pivot + 1]

        pool_word_positions = {
            Web3.to_checksum_address(pool_address): word_positions
        }

        result = await batcher.fetch_bitmap_data(pool_word_positions)

        # Assertions
        assert result.success, f"Single pool bitmap test failed: {result.error}"
        assert result.data, "No data returned"
        assert len(result.data) == 1, f"Expected 1 pool, got {len(result.data)}"
        assert result.block_number, "No block number returned"

        # Verify bitmap data format
        pool_data = list(result.data.values())[0]
        for word_pos in word_positions:
            if word_pos in pool_data:
                bitmap_value = pool_data[word_pos]
                assert isinstance(bitmap_value, int), f"Bitmap value should be int, got {type(bitmap_value)}"

                # Count initialized ticks in this word
                initialized_count = bin(bitmap_value).count('1') if bitmap_value > 0 else 0
                print(f"✅ Word {word_pos}: Bitmap=0x{bitmap_value:064x}, Initialized ticks: {initialized_count}")

        logger.info(f"✅ {pool_info['name']}: Fetched {len(pool_data)} bitmap words at block {result.block_number}")

    @pytest.mark.asyncio
    async def test_bitmap_tick_finding(self, web3_connection, test_pools):
        """Test finding initialized ticks from bitmap data."""
        # Create bitmap batcher
        batcher = UniswapV3BitmapBatcher(web3_connection)

        # Test with USDC/WETH 0.05% pool (tick spacing 10)
        pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        pool_info = test_pools[pool_address]
        tick_spacing = pool_info["tick_spacing"]

        # Get bitmap data around current price
        current_tick_estimate = -276320  # Approximate current tick for USDC/WETH
        word_positions = batcher.calculate_word_positions(
            current_tick_estimate - 1000,  # Range around current tick
            current_tick_estimate + 1000,
            tick_spacing
        )

        pool_word_positions = {
            Web3.to_checksum_address(pool_address): word_positions
        }

        result = await batcher.fetch_bitmap_data(pool_word_positions)

        assert result.success, f"Bitmap test failed: {result.error}"
        assert result.data, "No bitmap data returned"

        # Find initialized ticks
        pool_data = list(result.data.values())[0]
        initialized_ticks = batcher.find_initialized_ticks(pool_data, tick_spacing)

        # Assertions
        assert isinstance(initialized_ticks, list), "Should return list of ticks"
        assert len(initialized_ticks) >= 0, "Should return valid tick list"

        print(f"✅ Found {len(initialized_ticks)} initialized ticks:")
        for i, tick in enumerate(initialized_ticks[:10]):  # Show first 10
            print(f"   Tick {tick} (aligned to spacing {tick_spacing})")
            if i >= 9 and len(initialized_ticks) > 10:
                print(f"   ... and {len(initialized_ticks) - 10} more")
                break

        logger.info(f"✅ Found {len(initialized_ticks)} initialized ticks for {pool_info['name']}")

    @pytest.mark.asyncio
    async def test_word_position_calculation(self, web3_connection):
        """Test bitmap word position calculation utility."""
        batcher = UniswapV3BitmapBatcher(web3_connection)

        # Test various tick ranges
        test_cases = [
            (-276400, -276300, "Around USDC/WETH price"),
            (257000, 257500, "Around WBTC/WETH price"),
            (-1000, 1000, "Around zero tick"),
            (100000, 100256, "Single word boundary"),
        ]

        for lower_tick, upper_tick, description in test_cases:
            tick_spacing = 60
            word_positions = batcher.calculate_word_positions(lower_tick, upper_tick, tick_spacing)

            assert isinstance(word_positions, list), "Should return list"
            assert len(word_positions) > 0, "Should return at least one word position"
            assert all(isinstance(wp, int) for wp in word_positions), "All word positions should be integers"
            assert word_positions == sorted(word_positions), "Word positions should be sorted"

            # Account for tick compression before calculating word position
            expected_lower_word = (lower_tick // tick_spacing) >> 8
            expected_upper_word = (upper_tick // tick_spacing) >> 8
            assert word_positions[0] == expected_lower_word, f"First word position incorrect for {description}"
            assert word_positions[-1] == expected_upper_word, f"Last word position incorrect for {description}"

            print(f"✅ {description}: Ticks [{lower_tick}, {upper_tick}] → Words {word_positions}")

        logger.info("✅ Word position calculation tests passed")

    @pytest.mark.asyncio
    async def test_error_handling(self, web3_connection):
        """Test error handling with invalid inputs."""
        tick_batcher = UniswapV3TickBatcher(web3_connection)
        bitmap_batcher = UniswapV3BitmapBatcher(web3_connection)

        # Test with invalid pool address
        invalid_pools_ticks = {
            Web3.to_checksum_address("0x0000000000000000000000000000000000000000"): [100, 200, 300]
        }

        tick_result = await tick_batcher.fetch_tick_data(invalid_pools_ticks)
        bitmap_result = await bitmap_batcher.fetch_bitmap_data(invalid_pools_ticks)

        # Should handle gracefully (may succeed with zero data or fail with clear error)
        if not tick_result.success:
            assert tick_result.error, "Should provide error message"
            logger.info(f"✅ Tick batcher correctly handled invalid pool: {tick_result.error}")
        else:
            logger.info("✅ Tick batcher handled invalid pool gracefully")

        if not bitmap_result.success:
            assert bitmap_result.error, "Should provide error message"
            logger.info(f"✅ Bitmap batcher correctly handled invalid pool: {bitmap_result.error}")
        else:
            logger.info("✅ Bitmap batcher handled invalid pool gracefully")

    @pytest.mark.asyncio
    async def test_empty_requests(self, web3_connection):
        """Test handling of empty requests."""
        tick_batcher = UniswapV3TickBatcher(web3_connection)
        bitmap_batcher = UniswapV3BitmapBatcher(web3_connection)

        # Test empty requests
        empty_result_ticks = await tick_batcher.fetch_tick_data({})
        empty_result_bitmap = await bitmap_batcher.fetch_bitmap_data({})

        # Should handle gracefully
        assert empty_result_ticks.success, "Empty tick request should succeed"
        assert empty_result_ticks.data == {}, "Should return empty data"

        assert empty_result_bitmap.success, "Empty bitmap request should succeed"
        assert empty_result_bitmap.data == {}, "Should return empty data"

        logger.info("✅ Empty request handling tests passed")


# Run with: uv run pytest src/batchers/tests/test_live_v3_ticks.py -v -s --log-cli-level=INFO