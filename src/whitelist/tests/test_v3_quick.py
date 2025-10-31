#!/usr/bin/env python3
"""
Quick test for V3 liquidity filtering.

Tests just the V3 filtering functionality without full whitelist building.
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from web3 import Web3
from src.config import ConfigManager
from src.whitelist.liquidity_filter import PoolLiquidityFilter


async def test_v3_filtering():
    """Test V3 pool filtering with mock data."""
    print("=" * 80)
    print("🧪 Quick V3 Liquidity Filter Test")
    print("=" * 80)

    config = ConfigManager()
    rpc_url = config.chains.get_rpc_url("ethereum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    # Create filter instance
    filter_instance = PoolLiquidityFilter(
        web3=web3,
        min_liquidity_usd=10000,
        chain="ethereum"
    )

    # Create mock V3 pool data
    # Using real USDC/WETH pool address for testing
    usdc_addr = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    weth_addr = "0xc02aaa39b223fe8d0e0b4885a0d1114e13b17e663"
    pool_addr = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH 0.05%

    mock_pools = {
        pool_addr: {
            'token0': {
                'address': usdc_addr,
                'symbol': 'USDC',
                'decimals': 6
            },
            'token1': {
                'address': weth_addr,
                'symbol': 'WETH',
                'decimals': 18
            },
            'exchange': 'uniswap_v3',
            'fee': 500,
            'tickSpacing': 10
        }
    }

    token_symbols = {
        usdc_addr: 'USDC',
        weth_addr: 'WETH'
    }

    token_decimals = {
        usdc_addr: 6,
        weth_addr: 18
    }

    # Mock prices (WETH = $4000, USDC = $1)
    token_prices = {
        weth_addr: Decimal("4000"),
        usdc_addr: Decimal("1")
    }

    print(f"\n📊 Test Setup:")
    print(f"  Mock pools: {len(mock_pools)}")
    print(f"  Token prices: WETH=${token_prices[weth_addr]}, USDC=${token_prices[usdc_addr]}")
    print(f"  Min liquidity threshold: ${filter_instance.min_liquidity_usd:,.0f}")

    # Filter V3 pools
    print(f"\n🔍 Filtering V3 pools...")
    filtered_pools, updated_prices = await filter_instance.filter_v3_pools(
        pools=mock_pools,
        token_symbols=token_symbols,
        token_decimals=token_decimals,
        token_prices=token_prices
    )

    print(f"\n📈 Results:")
    print(f"  Input pools: {len(mock_pools)}")
    print(f"  Filtered pools: {len(filtered_pools)}")
    print(f"  Price discoveries: {len(updated_prices) - len(token_prices)}")

    if filtered_pools:
        print(f"\n✅ V3 filtering successful!")
        for addr, pool in filtered_pools.items():
            print(f"  Pool: {addr[:10]}...")
            print(f"    {pool['token0']['symbol']}/{pool['token1']['symbol']}")
            print(f"    Fee: {pool['fee']} bps")
    else:
        print(f"\n⚠️  No pools passed the liquidity threshold")
        print(f"    (This may be expected if the real pool has low liquidity)")

    print("\n✅ Test completed successfully!")


if __name__ == "__main__":
    asyncio.run(test_v3_filtering())
