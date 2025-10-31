"""
Simplified Uniswap V3 Math Functions.

Implements core V3 liquidity calculations without external dependencies.
Based on UniswapV3 contracts and UniswapV3 book.

Key concepts:
- sqrtPriceX96: Square root of price in Q96 fixed-point format (96 bits of precision)
- Tick: logarithmic price representation where price = 1.0001^tick
- Tick spacing: ±100 ticks represents ~±1% price movement
  - ±50 ticks = 0.5% price movement
  - ±100 ticks = 1% price movement
  - ±200 ticks = 2% price movement
"""

# Q96 constants
Q96 = 2 ** 96


def get_amount0_delta(
    *,
    sqrt_ratio_a_x96: int,
    sqrt_ratio_b_x96: int,
    liquidity: int,
    round_up: bool = True
) -> int:
    """
    Calculate token0 amount from sqrt price range and liquidity.

    Formula: amount0 = L * (1/√Pa - 1/√Pb) = L * (√Pb - √Pa) / (√Pa * √Pb)

    Args:
        sqrt_ratio_a_x96: Lower sqrt price in Q96 format
        sqrt_ratio_b_x96: Upper sqrt price in Q96 format
        liquidity: Pool liquidity L
        round_up: Whether to round up (ignored for simplicity)

    Returns:
        Amount of token0
    """
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96

    # Calculate: L * (√Pb - √Pa) * Q96 / (√Pa * √Pb)
    numerator = liquidity * (sqrt_ratio_b_x96 - sqrt_ratio_a_x96) * Q96
    denominator = sqrt_ratio_a_x96 * sqrt_ratio_b_x96

    return numerator // denominator


def get_amount1_delta(
    *,
    sqrt_ratio_a_x96: int,
    sqrt_ratio_b_x96: int,
    liquidity: int,
    round_up: bool = True
) -> int:
    """
    Calculate token1 amount from sqrt price range and liquidity.

    Formula: amount1 = L * (√Pb - √Pa) / Q96

    Args:
        sqrt_ratio_a_x96: Lower sqrt price in Q96 format
        sqrt_ratio_b_x96: Upper sqrt price in Q96 format
        liquidity: Pool liquidity L
        round_up: Whether to round up (ignored for simplicity)

    Returns:
        Amount of token1
    """
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96

    # Calculate: L * (√Pb - √Pa) / Q96
    return (liquidity * (sqrt_ratio_b_x96 - sqrt_ratio_a_x96)) // Q96


def get_sqrt_ratio_at_tick(tick: int) -> int:
    """
    Calculate sqrtPriceX96 from tick.

    Simplified approximation: sqrt(1.0001^tick) * 2^96

    Args:
        tick: The tick value

    Returns:
        sqrtPriceX96
    """
    # Use Python's built-in math for simplicity
    # price = 1.0001 ^ tick
    # sqrtPrice = sqrt(price) = 1.0001 ^ (tick/2)
    # sqrtPriceX96 = sqrtPrice * 2^96

    # More accurate calculation using integer arithmetic
    # Based on the Uniswap V3 tick math
    abs_tick = abs(tick)

    # Start with Q128 representation
    ratio = 0xfffcb933bd6fad37aa2d162d1a594001 if abs_tick & 0x1 else 1 << 128

    # Apply bit shifts based on tick magnitude
    if abs_tick & 0x2:
        ratio = (ratio * 0xfff97272373d413259a46990580e213a) >> 128
    if abs_tick & 0x4:
        ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdcc) >> 128
    if abs_tick & 0x8:
        ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0) >> 128
    if abs_tick & 0x10:
        ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644) >> 128
    if abs_tick & 0x20:
        ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0) >> 128
    if abs_tick & 0x40:
        ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861) >> 128
    if abs_tick & 0x80:
        ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053) >> 128
    if abs_tick & 0x100:
        ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4) >> 128
    if abs_tick & 0x200:
        ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54) >> 128
    if abs_tick & 0x400:
        ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3) >> 128
    if abs_tick & 0x800:
        ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9) >> 128
    if abs_tick & 0x1000:
        ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825) >> 128
    if abs_tick & 0x2000:
        ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5) >> 128
    if abs_tick & 0x4000:
        ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7) >> 128
    if abs_tick & 0x8000:
        ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6) >> 128
    if abs_tick & 0x10000:
        ratio = (ratio * 0x9aa508b5b7a84e1c677de54f3e99bc9) >> 128
    if abs_tick & 0x20000:
        ratio = (ratio * 0x5d6af8dedb81196699c329225ee604) >> 128
    if abs_tick & 0x40000:
        ratio = (ratio * 0x2216e584f5fa1ea926041bedfe98) >> 128
    if abs_tick & 0x80000:
        ratio = (ratio * 0x48a170391f7dc42444e8fa2) >> 128

    # Invert if tick is negative
    if tick > 0:
        ratio = ((1 << 256) - 1) // ratio

    # Convert from Q128 to Q96
    # Right shift by 32 bits (128 - 96 = 32)
    sqrt_price_x96 = ratio >> 32

    return sqrt_price_x96
