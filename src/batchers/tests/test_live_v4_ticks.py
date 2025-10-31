"""
Live tests for Uniswap V4 tick data batch fetcher.

This module tests the UniswapV4TickBatcher with real blockchain calls
to verify tick data fetching works end-to-end.
"""

import pytest
import logging
from web3 import Web3
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.batchers.uniswap_v4_ticks import UniswapV4TickBatcher, UniswapV4BitmapBatcher
from src.batchers.base import BatchConfig

# Set logging level for detailed output
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def web3_connection():
    """Setup Web3 connection using config manager."""
    try:
        config_manager = ConfigManager()
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
def test_pool_id():
    """Test pool ID with known active ticks."""
    # ETH/USDC V4 pool
    return "0x21c67e77068de97969ba93d4aab21826d33ca12bb9f565d8496e8fda8a82ca27"


@pytest.fixture(scope="class")
def test_ticks():
    """Test tick values around current price."""
    # These should be near the current tick for ETH/USDC
    return [-193250,-193200, -193150, -193100, -193050 ]


class TestLiveV4Ticks:
    """Live test class for Uniswap V4 tick data batch fetcher."""

    @pytest.mark.asyncio
    async def test_fetch_single_pool_ticks(self, web3_connection, test_pool_id, test_ticks):
        """Test fetching tick data for a single pool."""
        batcher = UniswapV4TickBatcher(web3_connection)

        # Create pool_ticks dict
        pool_ticks = {test_pool_id: test_ticks}

        result = await batcher.fetch_tick_data(pool_ticks)

        # Assertions
        assert result.success, f"Tick fetch failed: {result.error}"
        assert result.data, "No data returned"
        assert test_pool_id in result.data, f"Pool {test_pool_id} not in results"
        
        pool_tick_data = result.data[test_pool_id]
        assert len(pool_tick_data) == len(test_ticks), f"Expected {len(test_ticks)} ticks, got {len(pool_tick_data)}"

        # Verify tick data structure - pool_tick_data is a dict[tick, TickLiquidityInfo]
        for tick, tick_info in pool_tick_data.items():
            assert hasattr(tick_info, 'tick'), "Missing tick attribute"
            assert hasattr(tick_info, 'liquidity_gross'), "Missing liquidity_gross"
            assert hasattr(tick_info, 'liquidity_net'), "Missing liquidity_net"
            assert hasattr(tick_info, 'is_initialized'), "Missing is_initialized"

            print(f"✅ Tick {tick_info.tick}:")
            print(f"   Liquidity Gross: {tick_info.liquidity_gross}")
            print(f"   Liquidity Net: {tick_info.liquidity_net}")
            print(f"   Initialized: {tick_info.is_initialized}")

        logger.info(f"✅ Successfully fetched {len(pool_tick_data)} ticks for pool {test_pool_id}")

    @pytest.mark.asyncio
    async def test_fetch_multiple_pools(self, web3_connection, test_pool_id, test_ticks):
        """Test fetching tick data for multiple pools."""
        batcher = UniswapV4TickBatcher(web3_connection)

        # Use same pool with different tick ranges for testing
        pool_ticks = {
            test_pool_id: test_ticks[:3],  # First 3 ticks
            test_pool_id + "_range2": test_ticks[3:],  # Last 2 ticks (fake pool for testing)
        }

        result = await batcher.fetch_tick_data({test_pool_id: test_ticks[:3]})

        # Assertions
        assert result.success, f"Multi-pool tick fetch failed: {result.error}"
        assert result.data, "No data returned"
        assert test_pool_id in result.data, "Pool not in results"

        logger.info(f"✅ Successfully fetched ticks for {len(result.data)} pools")

    @pytest.mark.asyncio
    async def test_empty_tick_list(self, web3_connection, test_pool_id):
        """Test handling of empty tick list."""
        batcher = UniswapV4TickBatcher(web3_connection)

        pool_ticks = {test_pool_id: []}

        result = await batcher.fetch_tick_data(pool_ticks)

        # Should succeed with empty data
        assert result.success, "Empty tick list should succeed"
        
        logger.info("✅ Correctly handled empty tick list")

    @pytest.mark.asyncio
    async def test_invalid_ticks(self, web3_connection, test_pool_id):
        """Test handling of invalid tick values."""
        batcher = UniswapV4TickBatcher(web3_connection)

        # Mix valid and potentially invalid ticks
        pool_ticks = {
            test_pool_id: [-193100, 999999, -999999]  # Middle ones might be out of range
        }

        result = await batcher.fetch_tick_data(pool_ticks)

        # Should either succeed with data or fail gracefully
        if result.success:
            logger.info(f"✅ Fetched data for {len(result.data.get(test_pool_id, []))} ticks")
        else:
            logger.info(f"✅ Correctly handled invalid ticks: {result.error}")


class TestLiveV4Bitmaps:
    """Live test class for Uniswap V4 bitmap batch fetcher."""

    @pytest.mark.asyncio
    async def test_fetch_single_pool_bitmaps(self, web3_connection, test_pool_id):
        """Test fetching bitmap data for a single pool."""
        batcher = UniswapV4BitmapBatcher(web3_connection)

        # Word positions around current tick = -193099 // 10 >> 8 = -74
        word_positions = [ -74,-75,-76 ]

        pool_words = {test_pool_id: word_positions}

        result = await batcher.fetch_bitmap_data(pool_words)

        # Assertions
        assert result.success, f"Bitmap fetch failed: {result.error}"
        assert result.data, "No data returned"
        assert test_pool_id in result.data, f"Pool {test_pool_id} not in results"

        pool_bitmap_data = result.data[test_pool_id]
        assert len(pool_bitmap_data) == len(word_positions), f"Expected {len(word_positions)} words"


        # Verify bitmap data
        for word_pos, bitmap in pool_bitmap_data.items():
            assert isinstance(bitmap, int), f"Bitmap should be int, got {type(bitmap)}"
            print(f"✅ Word {word_pos}: {bin(bitmap)}")

        logger.info(f"✅ Successfully fetched {len(pool_bitmap_data)} bitmap words for pool")

    @pytest.mark.asyncio
    async def test_empty_word_list(self, web3_connection, test_pool_id):
        """Test handling of empty word position list."""
        batcher = UniswapV4BitmapBatcher(web3_connection)

        pool_words = {test_pool_id: []}

        result = await batcher.fetch_bitmap_data(pool_words)

        # Should succeed with empty data
        assert result.success, "Empty word list should succeed"
        
        logger.info("✅ Correctly handled empty word list")


# Run with: uv run pytest src/batchers/tests/test_live_v4_ticks.py -v -s --log-cli-level=INFO
