#!/usr/bin/env python3
"""
Live test for V4SmartLiquidityAnalyzer using real mainnet data.
"""

import asyncio
from web3 import Web3
from src.batchers.v4_smart_analyzer import V4SmartLiquidityAnalyzer

# Use configured RPC endpoint
RPC_URL = "http://100.104.193.35:8545"
web3 = Web3(Web3.HTTPProvider(RPC_URL))

def get_real_v4_pool_ids() -> list[str]:
    """Get real V4 pool IDs for testing."""
    return [
        "0x21c67e77068de97969ba93d4aab21826d33ca12bb9f565d8496e8fda8a82ca27",  # ETH/USDC
        "0x54c72c46df32f2cc455e84e41e191b26ed73a29452cdd3d82f511097af9f427e",  # ETH/WBTC
        "0xb2b5618903d74bbac9e9049a035c3827afc4487cde3b994a1568b050f4c8e2e4",  # WETH/LINK
    ]

async def test_smart_analyzer():
    """Test the V4SmartLiquidityAnalyzer with real pools."""
    print("🧪 Testing V4SmartLiquidityAnalyzer")
    print("=" * 50)

    if not web3.is_connected():
        print(f"❌ Cannot connect to RPC: {RPC_URL}")
        return

    print(f"✅ Connected to Ethereum node (block: {web3.eth.block_number})")

    # Initialize analyzer
    analyzer = V4SmartLiquidityAnalyzer(web3)

    # Test with first pool
    pool_ids = get_real_v4_pool_ids()
    test_pool_id = pool_ids[0]

    print(f"\n🔍 Analyzing pool: {test_pool_id[:20]}...")

    try:
        # Analyze pool liquidity around current price
        analysis = await analyzer.analyze_pool_liquidity(
            pool_id=test_pool_id,
            percentage_range=5.0,  # ±5% from current price
            min_liquidity=1000000,  # 1M minimum liquidity for swaps
            tick_spacing=60
        )

        print(f"✅ Analysis completed!")
        print(f"   Current tick: {analysis.current_tick}")
        print(f"   Current sqrt price: {analysis.current_sqrt_price}")
        print(f"   Current liquidity: {analysis.current_liquidity}")
        print(f"   Analyzed range: {analysis.analyzed_range}")
        print(f"   Total initialized ticks: {analysis.total_initialized_ticks}")
        print(f"   Swappable ticks found: {len(analysis.swappable_ticks)}")
        print(f"   Total swappable liquidity: {analysis.total_swappable_liquidity:,}")
        print(f"   Block number: {analysis.block_number}")

        # Show top 5 swappable ticks
        if analysis.swappable_ticks:
            print(f"\n📊 Top swappable ticks:")
            for i, tick_info in enumerate(analysis.swappable_ticks[:5]):
                print(f"   {i+1}. Tick {tick_info.tick}: "
                      f"gross={tick_info.liquidity_gross:,}, "
                      f"net={tick_info.liquidity_net:,}, "
                      f"distance={tick_info.distance_from_current}")
        else:
            print("   No swappable ticks found with current parameters")

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

async def test_multiple_pools():
    """Test analyzer with multiple pools."""
    print(f"\n🔍 Testing multiple pools...")

    analyzer = V4SmartLiquidityAnalyzer(web3)
    pool_ids = get_real_v4_pool_ids()

    try:
        results = await analyzer.analyze_multiple_pools(
            pool_ids=pool_ids,
            percentage_range=10.0,
            min_liquidity=500000,
            tick_spacing=60
        )

        print(f"✅ Multi-pool analysis completed!")
        print(f"   Pools analyzed: {len(results)}")

        for pool_id, analysis in results.items():
            short_id = pool_id[:20] + "..."
            print(f"   {short_id}: {len(analysis.swappable_ticks)} swappable ticks, "
                  f"{analysis.total_swappable_liquidity:,} total liquidity")

    except Exception as e:
        print(f"❌ Multi-pool analysis failed: {e}")

async def main():
    """Main test function."""
    await test_smart_analyzer()
    await test_multiple_pools()

    print("\n" + "=" * 50)
    print("🏁 Smart analyzer test completed!")

if __name__ == "__main__":
    asyncio.run(main())