"""
Smart V4 liquidity analyzer that finds swappable ticks around current price.

Builds on existing UniswapV4DataBatcher to:
1. Get current pool state (slot0 + liquidity)
2. Calculate tick range based on price percentage
3. Find initialized ticks via our new bitmap getter
4. Get detailed tick liquidity via our new tick getter
5. Filter by minimum liquidity threshold for swaps
"""

import json
import os
from typing import Dict, List, Union, Optional, Tuple
from dataclasses import dataclass

from web3 import Web3
from eth_abi import encode, decode

from .uniswap_v4_data import UniswapV4DataBatcher
from .base import BatchError


@dataclass
class TickLiquidityInfo:
    """Information about a specific tick's liquidity."""
    tick: int
    liquidity_gross: int
    liquidity_net: int
    is_swappable: bool
    distance_from_current: int


@dataclass
class PoolLiquidityAnalysis:
    """Complete liquidity analysis for a V4 pool."""
    pool_id: str
    current_tick: int
    current_sqrt_price: int
    current_liquidity: str
    analyzed_range: Tuple[int, int]
    total_initialized_ticks: int
    swappable_ticks: List[TickLiquidityInfo]
    total_swappable_liquidity: int
    block_number: int


class V4SmartLiquidityAnalyzer:
    """
    Smart analyzer for V4 pool liquidity around current price.

    Uses existing V4DataBatcher + new tick contracts for comprehensive analysis.
    """

    def __init__(self, web3: Web3):
        """
        Initialize the analyzer.

        Args:
            web3: Web3 instance
        """
        self.web3 = web3
        self.v4_batcher = UniswapV4DataBatcher(web3)

        # Load our new tick analysis contracts
        self._load_tick_contracts()

    def _load_tick_contracts(self):
        """Load bytecode for our new tick analysis contracts."""
        try:
            # Load tick getter bytecode
            tick_getter_path = os.path.join(
                os.path.dirname(__file__),
                "..", "..", "foundry", "out", "UniswapV4TickGetter.sol",
                "UniswapV4TickGetter.json"
            )
            with open(tick_getter_path, 'r') as f:
                self.tick_getter_bytecode = json.load(f)['bytecode']['object']

            # Load bitmap getter bytecode
            bitmap_getter_path = os.path.join(
                os.path.dirname(__file__),
                "..", "..", "foundry", "out", "UniswapV4TickGetter.sol",
                "UniswapV4TickBitmapGetter.json"
            )
            with open(bitmap_getter_path, 'r') as f:
                self.bitmap_getter_bytecode = json.load(f)['bytecode']['object']

        except Exception as e:
            raise BatchError(f"Failed to load tick analysis contracts: {e}")

    def calculate_tick_range(self, current_tick: int, percentage: float, tick_spacing: int = 60) -> Tuple[int, int]:
        """
        Calculate tick range based on percentage from current price using correct logarithmic mathematics.

        Args:
            current_tick: Current pool tick
            percentage: Percentage range around current price (e.g., 5.0 for ±5%)
            tick_spacing: Pool's tick spacing

        Returns:
            Tuple of (lower_tick, upper_tick)
        """
        import math

        # Use correct logarithmic relationship: price = 1.0001^tick
        # Convert percentage to multiplier (e.g., 1% -> 1.01)
        price_multiplier = 1 + (percentage / 100.0)

        # Calculate tick difference using logarithmic relationship
        tick_diff = math.log(price_multiplier) / math.log(1.0001)
        tick_delta = int(round(tick_diff))

        # Calculate bounds
        lower_tick = current_tick - tick_delta
        upper_tick = current_tick + tick_delta

        # Ensure ticks are aligned to tick spacing
        lower_tick = (lower_tick // tick_spacing) * tick_spacing
        upper_tick = (upper_tick // tick_spacing) * tick_spacing

        # Ensure we don't have the same bounds
        if lower_tick == upper_tick:
            if lower_tick > current_tick:
                lower_tick -= tick_spacing
            else:
                upper_tick += tick_spacing

        return lower_tick, upper_tick

    def get_bitmap_word_range(self, lower_tick: int, upper_tick: int, tick_spacing: int = 60) -> List[int]:
        """
        Calculate bitmap word positions needed for tick range.

        Args:
            lower_tick: Lower bound tick
            upper_tick: Upper bound tick
            tick_spacing: Pool's tick spacing (defaults to 60 for V4)

        Returns:
            List of bitmap word positions
        """
        # Compress ticks by tick spacing, then each bitmap word covers 256 compressed ticks
        lower_compressed = lower_tick // tick_spacing
        upper_compressed = upper_tick // tick_spacing

        lower_word = lower_compressed >> 8  # Divide by 256
        upper_word = upper_compressed >> 8  # Divide by 256

        return list(range(lower_word, upper_word + 1))

    async def fetch_tick_bitmaps(self, pool_id: str, word_positions: List[int]) -> Dict[int, int]:
        """
        Batch fetch tick bitmaps for given word positions.

        Args:
            pool_id: V4 pool ID (hex string)
            word_positions: List of bitmap word positions

        Returns:
            Dict mapping word_position -> bitmap_value
        """
        if not word_positions:
            return {}

        try:
            # Convert pool ID to bytes32
            pool_id_bytes = bytes.fromhex(pool_id.replace('0x', ''))

            # Encode bitmap request
            requests = [(pool_id_bytes, word_positions)]
            constructor_args = encode(['(bytes32,int16[])[]'], [requests])
            call_data = self.bitmap_getter_bytecode + constructor_args.hex()

            # Make the call
            result = self.web3.eth.call({'data': call_data, 'gas': 10000000})
            block_number, bitmaps = decode(['uint256', 'uint256[][]'], result)

            # Convert to dict
            bitmap_dict = {}
            for i, word_pos in enumerate(word_positions):
                if i < len(bitmaps[0]):
                    bitmap_dict[word_pos] = bitmaps[0][i]

            return bitmap_dict

        except Exception as e:
            raise BatchError(f"Failed to fetch tick bitmaps: {e}")

    def find_initialized_ticks(self, bitmaps: Dict[int, int], tick_spacing: int = 60) -> List[int]:
        """
        Find all initialized ticks from bitmap data.

        Args:
            bitmaps: Dict mapping word_position -> bitmap_value
            tick_spacing: Pool's tick spacing

        Returns:
            List of initialized tick values
        """
        initialized_ticks = []

        for word_pos, bitmap in bitmaps.items():
            if bitmap == 0:
                continue  # No initialized ticks in this word

            # Check each bit in the bitmap
            for bit_pos in range(256):
                if bitmap & (1 << bit_pos):
                    # This tick is initialized - convert from compressed tick to actual tick
                    compressed_tick = (word_pos << 8) + bit_pos
                    actual_tick = compressed_tick * tick_spacing
                    if actual_tick not in initialized_ticks:
                        initialized_ticks.append(actual_tick)

        return sorted(initialized_ticks)

    async def fetch_tick_liquidity(self, pool_id: str, ticks: List[int]) -> Dict[int, Tuple[int, int]]:
        """
        Batch fetch liquidity data for given ticks.

        Args:
            pool_id: V4 pool ID (hex string)
            ticks: List of tick values

        Returns:
            Dict mapping tick -> (liquidity_gross, liquidity_net)
        """
        if not ticks:
            return {}

        try:
            # Convert pool ID to bytes32
            pool_id_bytes = bytes.fromhex(pool_id.replace('0x', ''))

            # Encode tick data request
            requests = [(pool_id_bytes, ticks)]
            constructor_args = encode(['(bytes32,int24[])[]'], [requests])
            call_data = self.tick_getter_bytecode + constructor_args.hex()

            # Make the call
            result = self.web3.eth.call({'data': call_data, 'gas': 10000000})
            block_number, tick_data = decode(['uint256', 'uint128[2][][]'], result)

            # Convert to dict
            liquidity_dict = {}
            for i, tick in enumerate(ticks):
                if i < len(tick_data[0]):
                    gross, net = tick_data[0][i]
                    liquidity_dict[tick] = (gross, int(net))  # Handle signed int128

            return liquidity_dict

        except Exception as e:
            raise BatchError(f"Failed to fetch tick liquidity: {e}")

    async def analyze_pool_liquidity(
        self,
        pool_id: str,
        percentage_range: float = 10.0,
        min_liquidity: int = 1000000,
        tick_spacing: int = 60
    ) -> PoolLiquidityAnalysis:
        """
        Complete analysis of pool liquidity around current price.

        Args:
            pool_id: V4 pool ID (hex string)
            percentage_range: How far from current price to analyze (%)
            min_liquidity: Minimum liquidity required for swappable ticks
            tick_spacing: Pool's tick spacing

        Returns:
            Complete pool liquidity analysis
        """
        # Step 1: Get current pool state using existing V4 batcher
        pool_data = await self.v4_batcher.fetch_pools_chunked([pool_id])

        if not pool_data or pool_id.lower() not in pool_data:
            raise BatchError(f"Failed to get pool data for {pool_id}")

        current_pool = pool_data[pool_id.lower()]
        current_tick = current_pool['tick']
        current_sqrt_price = current_pool['sqrtPriceX96']
        current_liquidity = current_pool['liquidity']
        block_number = current_pool['block_number']

        # Step 2: Calculate tick range
        lower_tick, upper_tick = self.calculate_tick_range(current_tick, percentage_range, tick_spacing)

        # Step 3: Get bitmap word positions
        word_positions = self.get_bitmap_word_range(lower_tick, upper_tick, tick_spacing)

        # Step 4: Fetch tick bitmaps
        bitmaps = await self.fetch_tick_bitmaps(pool_id, word_positions)

        # Step 5: Find initialized ticks
        initialized_ticks = self.find_initialized_ticks(bitmaps, tick_spacing)

        # Step 6: Get liquidity data for initialized ticks
        tick_liquidity = {}
        if initialized_ticks:
            tick_liquidity = await self.fetch_tick_liquidity(pool_id, initialized_ticks)

        # Step 7: Filter and analyze swappable ticks
        swappable_ticks = []
        total_swappable_liquidity = 0

        for tick in initialized_ticks:
            if tick in tick_liquidity:
                gross, net = tick_liquidity[tick]
                is_swappable = gross >= min_liquidity

                if is_swappable:
                    total_swappable_liquidity += gross

                swappable_ticks.append(TickLiquidityInfo(
                    tick=tick,
                    liquidity_gross=gross,
                    liquidity_net=net,
                    is_swappable=is_swappable,
                    distance_from_current=abs(tick - current_tick)
                ))

        # Sort by liquidity (highest first)
        swappable_ticks.sort(key=lambda x: x.liquidity_gross, reverse=True)

        # Filter to only swappable ones
        final_swappable = [t for t in swappable_ticks if t.is_swappable]

        return PoolLiquidityAnalysis(
            pool_id=pool_id,
            current_tick=current_tick,
            current_sqrt_price=current_sqrt_price,
            current_liquidity=current_liquidity,
            analyzed_range=(lower_tick, upper_tick),
            total_initialized_ticks=len(initialized_ticks),
            swappable_ticks=final_swappable,
            total_swappable_liquidity=total_swappable_liquidity,
            block_number=block_number
        )

    async def analyze_multiple_pools(
        self,
        pool_ids: List[str],
        percentage_range: float = 10.0,
        min_liquidity: int = 1000000,
        tick_spacing: int = 60
    ) -> Dict[str, PoolLiquidityAnalysis]:
        """
        Analyze liquidity for multiple pools.

        Args:
            pool_ids: List of V4 pool IDs
            percentage_range: How far from current price to analyze (%)
            min_liquidity: Minimum liquidity required for swappable ticks
            tick_spacing: Pool's tick spacing

        Returns:
            Dict mapping pool_id -> analysis results
        """
        results = {}

        for pool_id in pool_ids:
            try:
                analysis = await self.analyze_pool_liquidity(
                    pool_id, percentage_range, min_liquidity, tick_spacing
                )
                results[pool_id] = analysis
            except Exception as e:
                # Log error but continue with other pools
                print(f"Failed to analyze pool {pool_id}: {e}")
                continue

        return results


# Convenience function
async def analyze_v4_pool_liquidity(
    web3: Web3,
    pool_id: str,
    percentage_range: float = 10.0,
    min_liquidity: int = 1000000,
    tick_spacing: int = 60
) -> PoolLiquidityAnalysis:
    """
    Convenience function to analyze a single V4 pool's liquidity.

    Args:
        web3: Web3 instance
        pool_id: V4 pool ID (hex string)
        percentage_range: How far from current price to analyze (%)
        min_liquidity: Minimum liquidity required for swappable ticks
        tick_spacing: Pool's tick spacing

    Returns:
        Complete pool liquidity analysis
    """
    analyzer = V4SmartLiquidityAnalyzer(web3)
    return await analyzer.analyze_pool_liquidity(
        pool_id, percentage_range, min_liquidity, tick_spacing
    )