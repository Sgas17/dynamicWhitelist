"""
Uniswap V2 Reserves Batch Fetcher.

This module provides efficient batch fetching of Uniswap V2 pair reserves
using a pre-compiled Solidity contract via eth.call().
"""

import json
import os
from typing import Dict, List, Union, Optional
from datetime import datetime, timezone

from web3 import Web3
from eth_abi import decode

from .base import ContractBatcher, BatchResult, BatchConfig, BatchError


class UniswapV2ReservesBatcher(ContractBatcher):
    """
    Batch fetcher for Uniswap V2 pair reserves.

    Uses the UniswapV2ReservesGetter contract to efficiently fetch
    reserve data for multiple pairs in a single RPC call.
    """

    def __init__(
        self,
        web3: Web3,
        chain_id: Optional[int] = None,
        config: Optional[BatchConfig] = None
    ):
        """
        Initialize the reserves batcher.

        Args:
            web3: Web3 instance
            chain_id: Chain ID (defaults to web3.eth.chain_id)
            config: Batch configuration
        """
        self.chain_id = chain_id or web3.eth.chain_id

        # Load contract bytecode based on chain
        contract_bytecode = self._load_contract_bytecode()

        super().__init__(web3, contract_bytecode, config)

    def _load_contract_bytecode(self) -> str:
        """
        Load the appropriate contract bytecode for the current chain.

        Returns:
            Contract bytecode as hex string

        Raises:
            BatchError: If contract file not found or invalid
        """
        try:
            # Determine which contract to use based on chain
            if self.chain_id == 8453:  # Base
                contract_file = "UniswapAeroV2ReservesGetter.json"
            else:  # Ethereum and other chains
                contract_file = "UniswapV2ReservesGetter.json"

            # Construct path to contract file
            contract_path = os.path.join(
                os.path.dirname(__file__),
                "contracts",
                "ethereum",
                contract_file
            )

            # Load and parse contract JSON
            with open(contract_path, "r") as f:
                contract_data = json.load(f)

            return contract_data["bytecode"]["object"]

        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            raise BatchError(f"Failed to load contract bytecode: {e}")

    async def batch_call(
        self,
        pair_addresses: List[str],
        block_identifier: Union[int, str] = 'latest'
    ) -> BatchResult:
        """
        Fetch reserves for multiple Uniswap V2 pairs.

        Args:
            pair_addresses: List of pair contract addresses
            block_identifier: Block to call at

        Returns:
            BatchResult containing reserve data for each pair
        """
        try:
            # Validate addresses
            validated_addresses = self._validate_addresses(pair_addresses)
            if not validated_addresses:
                return BatchResult(
                    success=False,
                    data={},
                    error="No valid addresses provided"
                )

            # Get current block number
            current_block = self._get_current_block()

            # Execute batch call with retry logic
            raw_response = await self._execute_batch_with_retry(
                validated_addresses, block_identifier
            )

            # Decode the response
            reserves_data = self._decode_reserves_response(
                raw_response, validated_addresses
            )

            return BatchResult(
                success=True,
                data=reserves_data,
                block_number=current_block,
                timestamp=datetime.now(timezone.utc)
            )

        except Exception as e:
            self.logger.error(f"Batch call failed: {e}")
            return BatchResult(
                success=False,
                data={},
                error=str(e)
            )

    def _decode_reserves_response(
        self,
        raw_response: bytes,
        pair_addresses: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Decode the raw response from the reserves batch call.

        Args:
            raw_response: Raw bytes response from eth.call()
            pair_addresses: List of pair addresses (in same order as call)

        Returns:
            Dictionary mapping pair addresses to their reserve data
        """
        try:
            if self.chain_id == 1:  # Ethereum mainnet
                return self._decode_ethereum_reserves(raw_response, pair_addresses)
            elif self.chain_id == 8453:  # Base
                return self._decode_base_reserves(raw_response, pair_addresses)
            else:
                # Default to Ethereum format for other chains
                return self._decode_ethereum_reserves(raw_response, pair_addresses)

        except Exception as e:
            self.logger.error(f"Failed to decode reserves response: {e}")
            raise BatchError(f"Failed to decode reserves response: {e}")

    def _decode_ethereum_reserves(
        self,
        raw_response: bytes,
        pair_addresses: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Decode reserves response for Ethereum mainnet format.

        Args:
            raw_response: Raw response bytes
            pair_addresses: List of pair addresses

        Returns:
            Decoded reserves data
        """
        block_number, reserves_data = decode(["uint256", "bytes32[]"], raw_response)

        decoded_reserves = {}
        for i, pair_address in enumerate(pair_addresses):
            if i < len(reserves_data):
                reserve_bytes = reserves_data[i].hex()
                decoded_reserves[pair_address.lower()] = {
                    'reserve0': reserve_bytes[0:28],
                    'reserve1': reserve_bytes[28:56],
                    'block_timestamp_last': int(reserve_bytes[56:64], 16) if len(reserve_bytes) > 56 else 0
                }

        return decoded_reserves

    def _decode_base_reserves(
        self,
        raw_response: bytes,
        pair_addresses: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Decode reserves response for Base chain format.

        Args:
            raw_response: Raw response bytes
            pair_addresses: List of pair addresses

        Returns:
            Decoded reserves data
        """
        block_number, reserves_data = decode(["uint256", "bytes32[2][]"], raw_response)

        decoded_reserves = {}
        for i, pair_address in enumerate(pair_addresses):
            if i < len(reserves_data):
                decoded_reserves[pair_address.lower()] = {
                    'reserve0': reserves_data[i][0].hex(),
                    'reserve1': reserves_data[i][1].hex(),
                    'block_timestamp_last': 0  # Base format doesn't include timestamp
                }

        return decoded_reserves

    async def fetch_reserves_chunked(
        self,
        pair_addresses: List[str],
        block_identifier: Union[int, str] = 'latest'
    ) -> Dict[str, Dict[str, str]]:
        """
        Fetch reserves for a large number of pairs using chunking.

        Args:
            pair_addresses: List of pair addresses (can be large)
            block_identifier: Block to call at

        Returns:
            Combined reserves data from all chunks
        """
        all_reserves = {}
        chunks = self._chunk_addresses(pair_addresses)

        self.logger.info(f"Fetching reserves for {len(pair_addresses)} pairs in {len(chunks)} chunks")

        for i, chunk in enumerate(chunks):
            self.logger.debug(f"Processing chunk {i + 1}/{len(chunks)} with {len(chunk)} pairs")

            result = await self.batch_call(chunk, block_identifier)

            if result.success:
                all_reserves.update(result.data)
            else:
                self.logger.warning(f"Chunk {i + 1} failed: {result.error}")
                # Continue with other chunks rather than failing completely

        return all_reserves


# Convenience function for easy usage
async def fetch_uniswap_v2_reserves(
    web3: Web3,
    pair_addresses: List[str],
    block_identifier: Union[int, str] = 'latest',
    batch_size: int = 100
) -> Dict[str, Dict[str, str]]:
    """
    Convenience function to fetch Uniswap V2 reserves.

    Args:
        web3: Web3 instance
        pair_addresses: List of pair contract addresses
        block_identifier: Block to call at
        batch_size: Number of pairs per batch

    Returns:
        Dictionary mapping pair addresses to reserve data
    """
    config = BatchConfig(batch_size=batch_size)
    batcher = UniswapV2ReservesBatcher(web3, config=config)
    return await batcher.fetch_reserves_chunked(pair_addresses, block_identifier)