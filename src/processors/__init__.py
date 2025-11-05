"""
Modular data processors for blockchain data pipeline.

KISS: Simple, focused processors that handle one specific data source.
"""

from typing import List

from .base import BaseProcessor, ProcessorError, ProcessorResult

# Import available processors
_available_processors = {}

try:
    from .pool_processors import (
        AerodromeV2PoolProcessor,
        AerodromeV3PoolProcessor,
        UniswapV2PoolProcessor,
        UniswapV3PoolProcessor,
        UniswapV4PoolProcessor,
    )

    _available_processors.update(
        {
            "uniswap_v3_pools": UniswapV3PoolProcessor,
            "uniswap_v4_pools": UniswapV4PoolProcessor,
        }
    )
except ImportError:
    pass

try:
    from .transfer_processors import LatestTransfersProcessor

    _available_processors["latest_transfers"] = LatestTransfersProcessor
except ImportError:
    pass

try:
    from .metadata_processors import TokenMetadataProcessor

    _available_processors["token_metadata"] = TokenMetadataProcessor
except ImportError:
    pass

try:
    from .token_matching_processor import TokenMatchingProcessor

    _available_processors["token_matching"] = TokenMatchingProcessor
except ImportError:
    pass

__all__ = [
    "BaseProcessor",
    "ProcessorResult",
    "ProcessorError",
    "get_processor",
    "list_processors",
]


def get_processor(protocol: str, chain: str = "ethereum") -> BaseProcessor:
    """
    Factory function to get a processor instance by protocol and chain.

    Args:
        protocol: Protocol name (e.g., 'uniswap_v2', 'uniswap_v3', 'uniswap_v4', 'aerodrome_v2', 'aerodrome_v3')
        chain: Blockchain chain name (e.g., 'ethereum', 'base', 'arbitrum')

    Returns:
        BaseProcessor: Configured processor instance

    Raises:
        ProcessorError: If protocol is not supported
    """
    processor_map = {
        "uniswap_v2": UniswapV2PoolProcessor,
        "uniswap_v3": UniswapV3PoolProcessor,
        "uniswap_v4": UniswapV4PoolProcessor,
        "aerodrome_v2": AerodromeV2PoolProcessor,
        "aerodrome_v3": AerodromeV3PoolProcessor,
    }

    if protocol not in processor_map:
        supported = ", ".join(processor_map.keys())
        raise ProcessorError(
            f"Unsupported protocol '{protocol}'. Supported: {supported}"
        )

    processor_class = processor_map[protocol]
    return processor_class(chain=chain)


def list_processors() -> List[str]:
    """
    List all available processor protocols.

    Returns:
        List[str]: Available processor protocol names
    """
    return ["uniswap_v2", "uniswap_v3", "uniswap_v4", "aerodrome_v2", "aerodrome_v3"]
