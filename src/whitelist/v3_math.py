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
Q96 = 2**96


def get_amount0_delta(
    *,
    sqrt_ratio_a_x96: int,
    sqrt_ratio_b_x96: int,
    liquidity: int,
    round_up: bool = True,
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
    round_up: bool = True,
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
    ratio = 0xFFFCB933BD6FAD37AA2D162D1A594001 if abs_tick & 0x1 else 1 << 128

    # Apply bit shifts based on tick magnitude
    if abs_tick & 0x2:
        ratio = (ratio * 0xFFF97272373D413259A46990580E213A) >> 128
    if abs_tick & 0x4:
        ratio = (ratio * 0xFFF2E50F5F656932EF12357CF3C7FDCC) >> 128
    if abs_tick & 0x8:
        ratio = (ratio * 0xFFE5CACA7E10E4E61C3624EAA0941CD0) >> 128
    if abs_tick & 0x10:
        ratio = (ratio * 0xFFCB9843D60F6159C9DB58835C926644) >> 128
    if abs_tick & 0x20:
        ratio = (ratio * 0xFF973B41FA98C081472E6896DFB254C0) >> 128
    if abs_tick & 0x40:
        ratio = (ratio * 0xFF2EA16466C96A3843EC78B326B52861) >> 128
    if abs_tick & 0x80:
        ratio = (ratio * 0xFE5DEE046A99A2A811C461F1969C3053) >> 128
    if abs_tick & 0x100:
        ratio = (ratio * 0xFCBE86C7900A88AEDCFFC83B479AA3A4) >> 128
    if abs_tick & 0x200:
        ratio = (ratio * 0xF987A7253AC413176F2B074CF7815E54) >> 128
    if abs_tick & 0x400:
        ratio = (ratio * 0xF3392B0822B70005940C7A398E4B70F3) >> 128
    if abs_tick & 0x800:
        ratio = (ratio * 0xE7159475A2C29B7443B29C7FA6E889D9) >> 128
    if abs_tick & 0x1000:
        ratio = (ratio * 0xD097F3BDFD2022B8845AD8F792AA5825) >> 128
    if abs_tick & 0x2000:
        ratio = (ratio * 0xA9F746462D870FDF8A65DC1F90E061E5) >> 128
    if abs_tick & 0x4000:
        ratio = (ratio * 0x70D869A156D2A1B890BB3DF62BAF32F7) >> 128
    if abs_tick & 0x8000:
        ratio = (ratio * 0x31BE135F97D08FD981231505542FCFA6) >> 128
    if abs_tick & 0x10000:
        ratio = (ratio * 0x9AA508B5B7A84E1C677DE54F3E99BC9) >> 128
    if abs_tick & 0x20000:
        ratio = (ratio * 0x5D6AF8DEDB81196699C329225EE604) >> 128
    if abs_tick & 0x40000:
        ratio = (ratio * 0x2216E584F5FA1EA926041BEDFE98) >> 128
    if abs_tick & 0x80000:
        ratio = (ratio * 0x48A170391F7DC42444E8FA2) >> 128

    # Invert if tick is negative
    if tick > 0:
        ratio = ((1 << 256) - 1) // ratio

    # Convert from Q128 to Q96
    # Right shift by 32 bits (128 - 96 = 32)
    sqrt_price_x96 = ratio >> 32

    return sqrt_price_x96
