"""
Uniswap V4 Data Batch Fetcher.

This module provides efficient batch fetching of Uniswap V4 pool data
using a pre-compiled Solidity contract via eth.call().
"""

import json
import os
from typing import Dict, List, Union, Optional
from datetime import datetime, timezone

from web3 import Web3
from eth_abi import decode

from .base import ContractBatcher, BatchResult, BatchConfig, BatchError


class UniswapV4DataBatcher(ContractBatcher):
    """
    Batch fetcher for Uniswap V4 pool data.

    Uses the UniswapV4DataGetter contract to efficiently fetch
    pool data for multiple pool IDs in a single RPC call.
    """

    def __init__(
        self,
        web3: Web3,
        chain_id: Optional[int] = None,
        config: Optional[BatchConfig] = None
    ):
        """
        Initialize the V4 data batcher.

        Args:
            web3: Web3 instance
            chain_id: Chain ID (defaults to web3.eth.chain_id)
            config: Batch configuration
        """
        self.chain_id = chain_id or web3.eth.chain_id

        # Load contract bytecode
        contract_bytecode = self._load_contract_bytecode()

        super().__init__(web3, contract_bytecode, config)

    def _load_contract_bytecode(self) -> str:
        """
        Load the V4 contract bytecode.

        Returns:
            Contract bytecode as hex string

        Raises:
            BatchError: If contract file not found or invalid
        """
        try:
            # Construct path to contract file
            contract_path = os.path.join(
                os.path.dirname(__file__),
                "contracts",
                "ethereum",
                "UniswapV4DataGetter.json"
            )

            # Load and parse contract JSON
            with open(contract_path, "r") as f:
                contract_data = json.load(f)

            return contract_data["bytecode"]["object"]

        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            raise BatchError(f"Failed to load V4 contract bytecode: {e}")

    def _prepare_call_data(self, pool_ids: List[str]) -> str:
        """
        Prepare call data by combining bytecode with encoded pool IDs.

        Args:
            pool_ids: List of pool IDs as hex strings (bytes32)

        Returns:
            Complete call data as hex string
        """
        try:
            # Convert hex strings to bytes32
            pool_id_bytes = []
            for pool_id in pool_ids:
                if isinstance(pool_id, str):
                    # Remove 0x prefix if present and ensure 32 bytes
                    clean_id = pool_id.replace('0x', '')
                    if len(clean_id) != 64:  # 32 bytes = 64 hex chars
                        raise ValueError(f"Invalid pool ID length: {pool_id}")
                    pool_id_bytes.append(bytes.fromhex(clean_id))
                else:
                    raise ValueError(f"Pool ID must be hex string: {pool_id}")

            # Encode constructor arguments (bytes32[] pool IDs)
            from eth_abi import encode
            encoded_args = encode(["bytes32[]"], [pool_id_bytes])

            # Combine bytecode with encoded arguments
            call_data = self.contract_bytecode + encoded_args.hex()

            return call_data

        except Exception as e:
            self.logger.error(f"Failed to prepare V4 call data: {e}")
            raise BatchError(f"Failed to prepare V4 call data: {e}")

    async def batch_call(
        self,
        pool_ids: List[str],
        block_identifier: Union[int, str] = 'latest'
    ) -> BatchResult:
        """
        Fetch data for multiple Uniswap V4 pools.

        Args:
            pool_ids: List of pool IDs (bytes32 as hex strings)
            block_identifier: Block to call at

        Returns:
            BatchResult containing pool data for each pool ID
        """
        try:
            # Validate pool IDs
            validated_pool_ids = self._validate_pool_ids(pool_ids)
            if not validated_pool_ids:
                return BatchResult(
                    success=False,
                    data={},
                    error="No valid pool IDs provided"
                )

            # Get current block number
            current_block = self._get_current_block()

            # Execute batch call with retry logic
            raw_response = await self._execute_v4_batch_with_retry(
                validated_pool_ids, block_identifier
            )

            # Decode the response
            pool_data = self._decode_v4_response(
                raw_response, validated_pool_ids
            )

            return BatchResult(
                success=True,
                data=pool_data,
                block_number=current_block,
                timestamp=datetime.now(timezone.utc)
            )

        except Exception as e:
            self.logger.error(f"V4 batch call failed: {e}")
            return BatchResult(
                success=False,
                data={},
                error=str(e)
            )

    def _validate_pool_ids(self, pool_ids: List[str]) -> List[str]:
        """
        Validate and normalize pool IDs.

        Args:
            pool_ids: List of pool IDs as hex strings

        Returns:
            List of validated pool IDs
        """
        validated = []
        for pool_id in pool_ids:
            try:
                # Ensure it's a valid hex string
                if not isinstance(pool_id, str):
                    self.logger.warning(f"Invalid pool ID type: {type(pool_id)}")
                    continue

                # Remove 0x prefix if present
                clean_id = pool_id.replace('0x', '')

                # Ensure it's valid hex and correct length (64 chars = 32 bytes)
                if len(clean_id) != 64:
                    self.logger.warning(f"Invalid pool ID length: {pool_id}")
                    continue

                int(clean_id, 16)  # Test if valid hex
                validated.append('0x' + clean_id)

            except (ValueError, AttributeError) as e:
                self.logger.warning(f"Invalid pool ID {pool_id}: {e}")
                continue

        return validated

    async def _execute_v4_batch_with_retry(
        self,
        pool_ids: List[str],
        block_identifier: Union[int, str] = 'latest'
    ) -> bytes:
        """
        Execute V4 batch call with retry logic.

        Args:
            pool_ids: List of validated pool IDs
            block_identifier: Block to call at

        Returns:
            Raw response bytes
        """
        async def _call():
            call_data = self._prepare_call_data(pool_ids)
            return self._make_batch_call(call_data, block_identifier)

        return await self._retry_operation(_call)

    def _decode_v4_response(
        self,
        raw_response: bytes,
        pool_ids: List[str]
    ) -> Dict[str, Dict[str, any]]:
        """
        Decode the raw response from the V4 batch call.

        Args:
            raw_response: Raw bytes response from eth.call()
            pool_ids: List of pool IDs (in same order as call)

        Returns:
            Dictionary mapping pool IDs to their data
        """
        try:
            # Based on the contract, it returns block number and an array of 2-element arrays
            # Each pool gets [liquidity_data, slot0_data] where slot0 contains price info
            block_number, pools_data = decode(["uint256", "bytes32[2][]"], raw_response)

            decoded_pools = {}
            for i, pool_id in enumerate(pool_ids):
                if i < len(pools_data):
                    liquidity_bytes = pools_data[i][0]
                    slot0_bytes = pools_data[i][1]

                    # Decode slot0 structure: sqrtPriceX96 (20 bytes) + tick (3 bytes, signed) + rest
                    sqrtPriceX96_bytes = slot0_bytes[0:20]  # First 160 bits (20 bytes)
                    sqrtPriceX96 = int.from_bytes(sqrtPriceX96_bytes, byteorder='big')

                    # Tick is a signed 24-bit integer at bytes 20-23
                    tick_bytes = slot0_bytes[20:23]
                    tick = int.from_bytes(tick_bytes, byteorder='big', signed=True)

                    # Extract liquidity as full uint256 (the contract returns it right-aligned)
                    liquidity_value = int.from_bytes(liquidity_bytes, byteorder='big')
                    liquidity = str(liquidity_value)

                    # Parse slot0 data (contains sqrtPriceX96, tick, etc.)
                    # This is a packed encoding from the V4 contract
                    decoded_pools[pool_id.lower()] = {
                        'liquidity': liquidity,
                        'sqrtPriceX96': sqrtPriceX96,
                        'tick': tick,
                        'block_number': block_number
                    }

            return decoded_pools

        except Exception as e:
            self.logger.error(f"Failed to decode V4 response: {e}")
            raise BatchError(f"Failed to decode V4 response: {e}")

    async def fetch_pools_chunked(
        self,
        pool_ids: List[str],
        block_identifier: Union[int, str] = 'latest'
    ) -> Dict[str, Dict[str, any]]:
        """
        Fetch pool data for a large number of pools using chunking.

        If a chunk fails, it will be split into smaller batches and retried
        to isolate invalid pool IDs.

        Args:
            pool_ids: List of pool IDs (can be large)
            block_identifier: Block to call at

        Returns:
            Combined pool data from all chunks
        """
        all_pools = {}
        self.failed_pools = []  # Track failed pools
        chunks = self._chunk_addresses(pool_ids)

        self.logger.info(f"Fetching V4 data for {len(pool_ids)} pools in {len(chunks)} chunks")

        for i, chunk in enumerate(chunks):
            self.logger.debug(f"Processing chunk {i + 1}/{len(chunks)} with {len(chunk)} pools")

            result = await self.batch_call(chunk, block_identifier)

            if result.success:
                all_pools.update(result.data)
            else:
                self.logger.warning(f"V4 chunk {i + 1} failed: {result.error}, splitting into smaller batches...")
                # Retry with smaller batch size to isolate bad IDs
                pools_recovered = await self._retry_failed_chunk(chunk, block_identifier)
                all_pools.update(pools_recovered)
                self.logger.info(f"  Recovered {len(pools_recovered)}/{len(chunk)} pools from failed chunk")

        # Write failed pools to file
        if self.failed_pools:
            self._write_failed_pools_to_file()

        return all_pools

    async def _retry_failed_chunk(
        self,
        pool_ids: List[str],
        block_identifier: Union[int, str] = 'latest',
        min_batch_size: int = 10
    ) -> Dict[str, Dict[str, any]]:
        """
        Retry a failed chunk by splitting into smaller batches.

        Args:
            pool_ids: Pool IDs that failed as a batch
            block_identifier: Block to call at
            min_batch_size: Minimum batch size before giving up

        Returns:
            Successfully fetched pool data
        """
        # Split into smaller chunks (half the size)
        chunk_size = max(min_batch_size, len(pool_ids) // 2)
        recovered_pools = {}

        for i in range(0, len(pool_ids), chunk_size):
            mini_chunk = pool_ids[i:i + chunk_size]

            result = await self.batch_call(mini_chunk, block_identifier)

            if result.success:
                recovered_pools.update(result.data)
            elif len(mini_chunk) > min_batch_size:
                # Recursively split further if still too large
                sub_pools = await self._retry_failed_chunk(
                    mini_chunk,
                    block_identifier,
                    min_batch_size
                )
                recovered_pools.update(sub_pools)
            else:
                # Give up on this small batch - likely contains invalid ID
                self.logger.warning(
                    f"  Failed V4 pools after retries: {', '.join([pid[:16] + '...' for pid in mini_chunk])}"
                )
                self.logger.debug(f"  Full pool IDs: {mini_chunk}")
                # Track failed pools
                self.failed_pools.extend(mini_chunk)

        return recovered_pools

    def _write_failed_pools_to_file(self):
        """Write failed pool IDs to a file for investigation."""
        import json
        from datetime import datetime
        from pathlib import Path

        # Create output directory
        output_dir = Path("data/failed_pools")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write to timestamped file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"v4_failed_pools_{timestamp}.json"

        data = {
            "timestamp": datetime.now().isoformat(),
            "total_failed": len(self.failed_pools),
            "failed_pools": self.failed_pools
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        self.logger.warning(f"💾 Wrote {len(self.failed_pools)} failed V4 pools to {output_file}")


# Convenience function for easy usage
async def fetch_uniswap_v4_data(
    web3: Web3,
    pool_ids: List[str],
    block_identifier: Union[int, str] = 'latest',
    batch_size: int = 50  # Smaller default for V4 as data might be larger
) -> Dict[str, Dict[str, any]]:
    """
    Convenience function to fetch Uniswap V4 pool data.

    Args:
        web3: Web3 instance
        pool_ids: List of pool IDs (bytes32 as hex strings)
        block_identifier: Block to call at
        batch_size: Number of pools per batch

    Returns:
        Dictionary mapping pool IDs to pool data
    """
    config = BatchConfig(batch_size=batch_size)
    batcher = UniswapV4DataBatcher(web3, config=config)
    return await batcher.fetch_pools_chunked(pool_ids, block_identifier)

@staticmethod
def calculate_word_positions(lower_tick: int, upper_tick: int, tick_spacing: int = 1) -> List[int]:
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