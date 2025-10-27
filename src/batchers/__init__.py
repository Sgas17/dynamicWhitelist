"""
Blockchain batch calling utilities.

This package provides efficient batch calling functionality for various
blockchain operations, reducing RPC overhead and improving performance.
"""

from .base import BaseBatcher, ContractBatcher, BatchResult, BatchConfig, BatchError
from .uniswap_v2_reserves import UniswapV2ReservesBatcher, fetch_uniswap_v2_reserves
from .uniswap_v3_data import UniswapV3DataBatcher, fetch_uniswap_v3_data
from .uniswap_v4_data import UniswapV4DataBatcher, fetch_uniswap_v4_data

__all__ = [
    'BaseBatcher',
    'ContractBatcher',
    'BatchResult',
    'BatchConfig',
    'BatchError',
    'UniswapV2ReservesBatcher',
    'fetch_uniswap_v2_reserves',
    'UniswapV3DataBatcher',
    'fetch_uniswap_v3_data',
    'UniswapV4DataBatcher',
    'fetch_uniswap_v4_data'
]