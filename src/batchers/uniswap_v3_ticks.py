"""
Uniswap V3 Tick and Bitmap Batch Fetchers.

This module provides efficient batch fetching of Uniswap V3 tick and bitmap data
using pre-compiled Solidity contracts via eth.call().
"""

import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from eth_abi import decode, encode
from eth_typing import ChecksumAddress
from web3 import Web3

from .base import BaseBatcher, BatchConfig, BatchError, BatchResult


@dataclass
class TickLiquidityInfo:
    """Information about a specific tick's liquidity."""

    tick: int
    liquidity_gross: int
    liquidity_net: int
    is_initialized: bool


class UniswapV3TickBatcher(BaseBatcher):
    """
    Batch fetcher for Uniswap V3 tick data.

    Uses the UniswapV3TickGetter contract to efficiently fetch
    tick liquidity data for multiple pools and ticks in a single RPC call.
    """

    def __init__(self, web3: Web3, config: Optional[BatchConfig] = None):
        """
        Initialize the V3 tick batcher.

        Args:
            web3: Web3 instance
            config: Batch configuration
        """
        super().__init__(web3, config)

        # Load contract bytecode
        self.contract_bytecode = self._load_contract_bytecode()

    def _load_contract_bytecode(self) -> str:
        """Load the V3 tick getter contract bytecode."""
        try:
            contract_path = os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "foundry",
                "out",
                "UniswapV3TickGetter.sol",
                "UniswapV3TickGetter.json",
            )

            with open(contract_path, "r") as f:
                contract_data = json.load(f)
                return contract_data["bytecode"]["object"]

        except Exception as e:
            raise BatchError(f"Failed to load V3 tick getter bytecode: {e}")

    async def batch_call(
        self, addresses: List[str], block_identifier: Union[int, str] = "latest"
    ) -> BatchResult:
        """
        Implementation of abstract batch_call method for compatibility.
        Not used directly - use fetch_tick_data instead.
        """
        return BatchResult(
            success=False, error="Use fetch_tick_data method instead", data={}
        )

    async def fetch_tick_data(
        self,
        pool_ticks: Dict[ChecksumAddress, List[int]],
        block_number: Optional[int] = None,
    ) -> BatchResult:
        """
        Batch fetch tick data for multiple pools.

        Args:
            pool_ticks: Dict mapping pool addresses to lists of tick values
            block_number: Specific block number (defaults to latest)

        Returns:
            BatchResult containing tick data
        """
        if not pool_ticks:
            return BatchResult(success=True, data={}, block_number=None)

        try:
            # Prepare requests
            requests = []
            for pool_address, ticks in pool_ticks.items():
                requests.append((pool_address, ticks))

            # Encode constructor arguments
            constructor_args = encode(["(address,int24[])[]"], [requests])
            call_data = self.contract_bytecode + constructor_args.hex()

            # Make the call
            block_id = block_number if block_number is not None else "latest"

            # Debug logging
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(f"V3 Batcher: Requesting block_identifier={block_id}")

            result = self.web3.eth.call({"data": call_data}, block_identifier=block_id)

            # Decode response
            block_num, tick_data = decode(["uint256", "bytes32[][]"], result)

            logger.debug(
                f"V3 Batcher: Contract returned block.number={block_num} (requested={block_id})"
            )

            # Process results
            processed_data = {}
            for i, (pool_address, ticks) in enumerate(pool_ticks.items()):
                pool_data = {}
                for j, tick in enumerate(ticks):
                    if i < len(tick_data) and j < len(tick_data[i]):
                        gross = int.from_bytes(tick_data[i][j][:16], byteorder="big")
                        net = int.from_bytes(
                            tick_data[i][j][16:32], byteorder="big", signed=True
                        )
                        pool_data[tick] = TickLiquidityInfo(
                            tick=tick,
                            liquidity_gross=gross,
                            liquidity_net=net,  # Handle signed int128
                            is_initialized=gross > 0,
                        )
                processed_data[pool_address] = pool_data

            return BatchResult(
                success=True, data=processed_data, block_number=int(block_num)
            )

        except Exception as e:
            return BatchResult(
                success=False,
                error=f"Failed to fetch V3 tick data: {e}",
                data={},
                block_number=None,
            )


class UniswapV3BitmapBatcher(BaseBatcher):
    """
    Batch fetcher for Uniswap V3 tick bitmap data.

    Uses the UniswapV3TickBitmapGetter contract to efficiently fetch
    tick bitmap data for multiple pools in a single RPC call.
    """

    def __init__(self, web3: Web3, config: Optional[BatchConfig] = None):
        """
        Initialize the V3 bitmap batcher.

        Args:
            web3: Web3 instance
            config: Batch configuration
        """
        super().__init__(web3, config)

        # Load contract bytecode
        self.contract_bytecode = self._load_contract_bytecode()

    def _load_contract_bytecode(self) -> str:
        """Load the V3 bitmap getter contract bytecode."""
        try:
            contract_path = os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "foundry",
                "out",
                "UniswapV3TickGetter.sol",
                "UniswapV3TickBitmapGetter.json",
            )

            with open(contract_path, "r") as f:
                contract_data = json.load(f)
                return contract_data["bytecode"]["object"]

        except Exception as e:
            raise BatchError(f"Failed to load V3 bitmap getter bytecode: {e}")

    async def batch_call(
        self, addresses: List[str], block_identifier: Union[int, str] = "latest"
    ) -> BatchResult:
        """
        Implementation of abstract batch_call method for compatibility.
        Not used directly - use fetch_bitmap_data instead.
        """
        return BatchResult(
            success=False, error="Use fetch_bitmap_data method instead", data={}
        )

    async def fetch_bitmap_data(
        self,
        pool_word_positions: Dict[ChecksumAddress, List[int]],
        block_number: Optional[int] = None,
    ) -> BatchResult:
        """
        Batch fetch bitmap data for multiple pools.

        Args:
            pool_word_positions: Dict mapping pool addresses to lists of word positions
            block_number: Specific block number (defaults to latest)

        Returns:
            BatchResult containing bitmap data
        """
        if not pool_word_positions:
            return BatchResult(success=True, data={}, block_number=None)

        try:
            # Prepare requests
            requests = []
            for pool_address, word_positions in pool_word_positions.items():
                requests.append((pool_address, word_positions))

            # Encode constructor arguments
            constructor_args = encode(["(address,int16[])[]"], [requests])
            call_data = self.contract_bytecode + constructor_args.hex()

            # Make the call
            block_id = block_number if block_number is not None else "latest"
            result = self.web3.eth.call({"data": call_data}, block_identifier=block_id)

            # Decode response
            block_num, bitmap_data = decode(["uint256", "uint256[][]"], result)

            # Process results
            processed_data = {}
            for i, (pool_address, word_positions) in enumerate(
                pool_word_positions.items()
            ):
                pool_data = {}
                for j, word_pos in enumerate(word_positions):
                    if i < len(bitmap_data) and j < len(bitmap_data[i]):
                        pool_data[word_pos] = bitmap_data[i][j]
                processed_data[pool_address] = pool_data

            return BatchResult(
                success=True, data=processed_data, block_number=int(block_num)
            )

        except Exception as e:
            return BatchResult(
                success=False,
                error=f"Failed to fetch V3 bitmap data: {e}",
                data={},
                block_number=None,
            )

    def find_initialized_ticks(
        self, bitmaps: Dict[int, int], tick_spacing: int = 60
    ) -> List[int]:
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

    @staticmethod
    def calculate_word_positions(
        lower_tick: int, upper_tick: int, tick_spacing: int = 1
    ) -> List[int]:
        """
        Calculate bitmap word positions needed for tick range.

        Args:
            lower_tick: Lower bound tick
            upper_tick: Upper bound tick
            tick_spacing: Pool's tick spacing (defaults to 1, but should be provided)

        Returns:
            List of bitmap word positions
        """
        # Compress ticks by tick spacing, then each bitmap word covers 256 compressed ticks
        lower_compressed = lower_tick // tick_spacing
        upper_compressed = upper_tick // tick_spacing

        lower_word = lower_compressed >> 8  # Divide by 256
        upper_word = upper_compressed >> 8  # Divide by 256

        return list(range(lower_word, upper_word + 1))
