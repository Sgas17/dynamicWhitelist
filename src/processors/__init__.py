"""
Modular data processors for blockchain data pipeline.

KISS: Simple, focused processors that handle one specific data source.
"""

from .base import BaseProcessor, ProcessorResult, ProcessorError

# Import available processors
_available_processors = {}

try:
    from .pool_processors import UniswapV3PoolProcessor, UniswapV4PoolProcessor
    _available_processors.update({
        'uniswap_v3_pools': UniswapV3PoolProcessor,
        'uniswap_v4_pools': UniswapV4PoolProcessor
    })
except ImportError:
    pass

try:
    from .transfer_processors import LatestTransfersProcessor
    _available_processors['latest_transfers'] = LatestTransfersProcessor
except ImportError:
    pass

try:
    from .metadata_processors import TokenMetadataProcessor
    _available_processors['token_metadata'] = TokenMetadataProcessor
except ImportError:
    pass

__all__ = [
    'BaseProcessor',
    'ProcessorResult', 
    'ProcessorError',
    'get_processor',
    'list_processors'
]

def get_processor(processor_type: str) -> type:
    """Get processor class by type."""
    if processor_type not in _available_processors:
        raise ValueError(f"Unknown processor type: {processor_type}")
    return _available_processors[processor_type]

def list_processors():
    """List available processor types."""
    return list(_available_processors.keys())