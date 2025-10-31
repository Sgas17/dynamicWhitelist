"""
Core types for whitelist and pool filtering.

Domain models used across the whitelist module for representing pools and prices.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class PoolInfo:
    """
    Pool information for DEX pools.

    Attributes:
        pool_address: Pool contract address
        token0: First token address in the pair
        token1: Second token address in the pair
        protocol: Protocol name (v2, v3, v4)
        factory: Factory contract address
        liquidity: Pool liquidity in USD (optional)
        fee: Fee tier for the pool (optional, V3/V4 specific)
        tick_spacing: Tick spacing for the pool (optional, V3/V4 specific)
        block_number: Block number when pool data was captured (optional)
    """

    pool_address: str
    token0: str
    token1: str
    protocol: str
    factory: str
    liquidity: Optional[Decimal] = None
    fee: Optional[int] = None
    tick_spacing: Optional[int] = None
    block_number: Optional[int] = None


@dataclass
class TokenPrice:
    """
    Token price snapshot from pool data.

    Attributes:
        token_address: Token being priced
        price_in_trusted: Price in terms of trusted token
        trusted_token: Address of the trusted token used for pricing
        pool_address: Pool used to determine the price
        liquidity: Liquidity of the pool used for pricing
    """

    token_address: str
    price_in_trusted: Decimal
    trusted_token: str
    pool_address: str
    liquidity: Decimal
