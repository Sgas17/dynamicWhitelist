"""
Chain-specific data fetchers with cryo wrapper.

KISS: Simple, focused fetchers that handle blockchain data collection.
"""

from .base import BaseFetcher, FetchResult, FetchError
from .cryo_fetcher import CryoFetcher

# Import chain-specific fetchers
_available_fetchers = {}

try:
    from .ethereum_fetcher import EthereumFetcher
    _available_fetchers['ethereum'] = EthereumFetcher
except ImportError:
    pass

try:
    from .base_fetcher import BaseFetcher as BaseChainFetcher
    _available_fetchers['base'] = BaseChainFetcher
except ImportError:
    pass

# Add exchange fetchers
try:
    from .exchange_fetchers import HyperliquidFetcher, BinanceFetcher
    _available_fetchers['hyperliquid'] = HyperliquidFetcher
    _available_fetchers['binance'] = BinanceFetcher
except ImportError:
    pass

# Supporting blockchain and exchange data sources

__all__ = [
    'BaseFetcher',
    'FetchResult', 
    'FetchError',
    'CryoFetcher',
    'get_fetcher',
    'list_fetchers'
]

def get_fetcher(chain: str) -> type:
    """Get fetcher class for specific chain."""
    chain_lower = chain.lower()
    if chain_lower not in _available_fetchers:
        raise ValueError(f"No fetcher available for chain: {chain}")
    return _available_fetchers[chain_lower]

def list_fetchers():
    """List available chain fetchers."""
    return list(_available_fetchers.keys())