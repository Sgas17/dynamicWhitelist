"""
Base classes for blockchain batch calling.

This module provides abstract interfaces for efficiently batching blockchain calls
to reduce RPC overhead and improve performance using direct eth.call() operations.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from eth_abi import decode, encode
from web3 import Web3

from .errors import (  # , ProviderRotator  # Commented out for local node usage
    BatchError,
    ErrorHandler,
)

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Result from a batch operation."""

    success: bool
    data: Dict[str, Any]
    block_number: Optional[int] = None
    timestamp: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class BatchConfig:
    """Configuration for batch operations."""

    batch_size: int = 100
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0


# BatchError is now imported from .errors module


class BaseBatcher(ABC):
    """
    Abstract base class for blockchain batch operations.

    Provides common functionality for batching RPC calls to reduce
    network overhead and improve performance.
    """

    def __init__(self, web3: Web3, config: Optional[BatchConfig] = None):
        self.web3 = web3
        self.config = config or BatchConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.error_handler = ErrorHandler(self.logger)

        # Provider rotation commented out for local node usage
        # TODO: Uncomment when using multiple remote providers
        # if providers and len(providers) > 1:
        #     self.provider_rotator = ProviderRotator(providers)
        # else:
        #     self.provider_rotator = None

    @abstractmethod
    async def batch_call(
        self, addresses: List[str], block_identifier: Union[int, str] = "latest"
    ) -> BatchResult:
        """
        Execute a batch call for the given addresses.

        Args:
            addresses: List of contract addresses to batch call
            block_identifier: Block to call at

        Returns:
            BatchResult with success status and data
        """
        pass

    def _chunk_addresses(self, addresses: List[str]) -> List[List[str]]:
        """Split addresses into chunks based on batch_size."""
        chunk_size = self.config.batch_size
        return [
            addresses[i : i + chunk_size] for i in range(0, len(addresses), chunk_size)
        ]

    async def _retry_operation(self, operation, *args, **kwargs) -> Any:
        """Retry an operation with exponential backoff and intelligent error handling."""
        last_error = None

        for attempt in range(self.config.max_retries):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                last_error = e

                # Log error with context
                self.error_handler.log_error(
                    e,
                    {
                        "attempt": attempt + 1,
                        "max_retries": self.config.max_retries,
                        "operation": operation.__name__
                        if hasattr(operation, "__name__")
                        else str(operation),
                    },
                )

                # Check if we should retry this error
                if not self.error_handler.should_retry(
                    e, attempt, self.config.max_retries
                ):
                    self.logger.info(f"Not retrying error: {e}")
                    raise

                if attempt == self.config.max_retries - 1:
                    raise

                # Calculate delay based on error type
                delay = self.error_handler.get_retry_delay(e, attempt)
                self.logger.info(
                    f"Retrying in {delay}s... (attempt {attempt + 1}/{self.config.max_retries})"
                )
                await asyncio.sleep(delay)

        # This should never be reached, but just in case
        if last_error:
            raise last_error

    def _get_current_block(self) -> int:
        """Get current block number."""
        try:
            return self.web3.eth.block_number
        except Exception as e:
            self.logger.error(f"Failed to get current block: {e}")
            raise BatchError(f"Failed to get current block: {e}")

    def _validate_addresses(self, addresses: List[str]) -> List[str]:
        """Validate and normalize Ethereum addresses."""
        validated = []
        for addr in addresses:
            try:
                validated.append(Web3.to_checksum_address(addr))
            except Exception as e:
                self.logger.warning(f"Invalid address {addr}: {e}")
                continue
        return validated


class ContractBatcher(BaseBatcher):
    """
    Base class for contract-based batch operations using eth.call().

    Uses pre-compiled contract bytecode with constructor arguments
    to batch multiple contract calls efficiently.
    """

    def __init__(
        self, web3: Web3, contract_bytecode: str, config: Optional[BatchConfig] = None
    ):
        super().__init__(web3, config)
        self.contract_bytecode = contract_bytecode

    def _prepare_call_data(self, constructor_args: List[Any]) -> str:
        """
        Prepare call data by combining bytecode with encoded constructor args.

        Args:
            constructor_args: Arguments for the contract constructor

        Returns:
            Complete call data as hex string
        """
        try:
            # Encode constructor arguments
            encoded_args = encode(["address[]"], constructor_args)

            # Combine bytecode with encoded arguments
            call_data = self.contract_bytecode + encoded_args.hex()

            return call_data
        except Exception as e:
            self.logger.error(f"Failed to prepare call data: {e}")
            raise BatchError(f"Failed to prepare call data: {e}")

    def _make_batch_call(
        self, call_data: str, block_identifier: Union[int, str] = "latest"
    ) -> bytes:
        """
        Make a batch call using eth.call() with prepared data.

        Args:
            call_data: Complete call data (bytecode + constructor args)
            block_identifier: Block to call at

        Returns:
            Raw bytes response from the call
        """
        try:
            return self.web3.eth.call(
                {"data": call_data}, block_identifier=block_identifier
            )
        except Exception as e:
            self.logger.error(f"Batch call failed: {e}")
            raise BatchError(f"Batch call failed: {e}")

    async def _execute_batch_with_retry(
        self, addresses: List[str], block_identifier: Union[int, str] = "latest"
    ) -> bytes:
        """
        Execute batch call with retry logic.

        Args:
            addresses: List of addresses to call
            block_identifier: Block to call at

        Returns:
            Raw response bytes
        """

        async def _call():
            call_data = self._prepare_call_data([addresses])
            return self._make_batch_call(call_data, block_identifier)

        return await self._retry_operation(_call)
