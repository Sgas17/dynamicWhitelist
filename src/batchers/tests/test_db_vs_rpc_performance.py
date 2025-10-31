"""
Performance Comparison: Direct DB Access vs RPC Batch Calls

This script compares the performance of fetching all tick data for a Uniswap V3 pool
using two approaches:
1. Direct database access via scrape_rethdb_data (Rust/MDBX)
2. RPC batch calls via web3.py using existing batchers/uniswap_v3_ticks.py

Prerequisites:
- Install scrape_rethdb_data:
  cd ~/scrape_rethdb_data && maturin develop --release --features=python
  OR: uv pip install -e ~/scrape_rethdb_data --config-settings="--features=python"
- Set RETH_DB_PATH environment variable to your Reth database path
- Configure RPC_URL in your .env or config

Usage:
    python test_db_vs_rpc_performance.py [--pool POOL_ADDRESS] [--ticks TICK1,TICK2,...] [--discover-ticks]

Examples:
    # Test with specific ticks
    python test_db_vs_rpc_performance.py --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 --ticks -887220,-887160,887220

    # Discover and test all initialized ticks (realistic workload)
    python test_db_vs_rpc_performance.py --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 --discover-ticks
"""

import asyncio
import time
import os
import sys
from typing import List, Dict, Optional
from dataclasses import dataclass
import argparse

from web3 import Web3
from eth_typing import ChecksumAddress

# Import existing batchers
from src.batchers.uniswap_v3_ticks import UniswapV3TickBatcher, UniswapV3BitmapBatcher
from src.batchers.base import BatchConfig
from src.config import ConfigManager


@dataclass
class PerformanceMetrics:
    """Performance metrics for a single run."""
    method: str
    duration_seconds: float
    tick_count: int
    ticks_per_second: float
    memory_usage_mb: Optional[float] = None
    error: Optional[str] = None


def get_process_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


async def fetch_ticks_via_rpc(
    web3: Web3,
    pool_address: ChecksumAddress,
    ticks: List[int]
) -> PerformanceMetrics:
    """
    Fetch tick data using RPC batch calls via UniswapV3TickBatcher.

    This uses the existing production batcher that deploys a Solidity contract
    and calls it via eth.call() to batch fetch tick data.
    """
    print(f"\n{'='*60}")
    print(f"RPC BATCH METHOD: Fetching {len(ticks)} ticks")
    print(f"{'='*60}")

    start_memory = get_process_memory_mb()
    start_time = time.perf_counter()

    try:
        # Use the production batcher with reasonable config
        config = BatchConfig(
            batch_size=100,  # Batch up to 100 ticks per call
            max_retries=3,
            timeout=30.0
        )
        batcher = UniswapV3TickBatcher(web3, config=config)

        # Prepare pool_ticks dict as expected by the batcher
        pool_ticks = {pool_address: ticks}

        # Fetch data
        result = await batcher.fetch_tick_data(pool_ticks, block_number=None)

        end_time = time.perf_counter()
        end_memory = get_process_memory_mb()

        duration = end_time - start_time
        memory_delta = end_memory - start_memory

        if not result.success:
            return PerformanceMetrics(
                method="RPC Batch",
                duration_seconds=duration,
                tick_count=len(ticks),
                ticks_per_second=0,
                memory_usage_mb=memory_delta,
                error=result.error
            )

        # Verify we got all ticks
        pool_data = result.data.get(pool_address, {})
        successful_ticks = len(pool_data)

        print(f"\n✓ Successfully fetched {successful_ticks}/{len(ticks)} ticks")
        print(f"  Block number: {result.block_number}")
        print(f"  Duration: {duration:.3f}s")
        print(f"  Rate: {successful_ticks/duration:.1f} ticks/sec")
        print(f"  Memory delta: {memory_delta:.2f} MB")

        # Show sample of data
        if pool_data:
            sample_tick = list(pool_data.keys())[0]
            sample_data = pool_data[sample_tick]
            print(f"\n  Sample tick {sample_tick}:")
            print(f"    liquidityGross: {sample_data.liquidity_gross}")
            print(f"    liquidityNet: {sample_data.liquidity_net}")
            print(f"    initialized: {sample_data.is_initialized}")

        return PerformanceMetrics(
            method="RPC Batch",
            duration_seconds=duration,
            tick_count=successful_ticks,
            ticks_per_second=successful_ticks / duration,
            memory_usage_mb=memory_delta
        )

    except Exception as e:
        end_time = time.perf_counter()
        duration = end_time - start_time

        print(f"\n✗ RPC batch failed: {e}")

        return PerformanceMetrics(
            method="RPC Batch",
            duration_seconds=duration,
            tick_count=0,
            ticks_per_second=0,
            error=str(e)
        )


def fetch_ticks_via_db(
    db_path: str,
    pool_address: str,
    ticks: List[int]
) -> PerformanceMetrics:
    """
    Fetch tick data using direct database access via scrape_rethdb_data.

    This uses the Rust library with PyO3 bindings to directly query the MDBX database.
    """
    print(f"\n{'='*60}")
    print(f"DIRECT DB METHOD: Fetching {len(ticks)} ticks")
    print(f"{'='*60}")

    start_memory = get_process_memory_mb()
    start_time = time.perf_counter()

    try:
        # Import the Rust library
        import scrape_rethdb_data

        # Create pool inputs for each tick
        # Note: The library expects a list of PoolInput objects
        pool_inputs = []
        for tick in ticks:
            pool_input = scrape_rethdb_data.PoolInput.new_v3_tick(
                pool_address,
                tick
            )
            pool_inputs.append(pool_input)

        # Collect data from DB
        results = scrape_rethdb_data.collect_pool_data(
            db_path,
            pool_inputs,
            None  # block_number (None = latest)
        )

        end_time = time.perf_counter()
        end_memory = get_process_memory_mb()

        duration = end_time - start_time
        memory_delta = end_memory - start_memory

        # Count successful ticks (non-zero data)
        successful_ticks = len([r for r in results if r.get('tick_data') is not None])

        print(f"\n✓ Successfully fetched {successful_ticks}/{len(ticks)} ticks")
        print(f"  Duration: {duration:.3f}s")
        print(f"  Rate: {successful_ticks/duration:.1f} ticks/sec")
        print(f"  Memory delta: {memory_delta:.2f} MB")

        # Show sample of data
        if results and results[0].get('tick_data'):
            sample = results[0]['tick_data']
            print(f"\n  Sample tick {ticks[0]}:")
            print(f"    liquidityGross: {sample.get('liquidity_gross')}")
            print(f"    liquidityNet: {sample.get('liquidity_net')}")
            print(f"    initialized: {sample.get('initialized')}")

        return PerformanceMetrics(
            method="Direct DB",
            duration_seconds=duration,
            tick_count=successful_ticks,
            ticks_per_second=successful_ticks / duration,
            memory_usage_mb=memory_delta
        )

    except ImportError as e:
        end_time = time.perf_counter()
        duration = end_time - start_time

        print(f"\n✗ Failed to import scrape_rethdb_data: {e}")
        print("   Make sure to install it first:")
        print("   cd ~/scrape_rethdb_data && maturin develop --release --features=python")

        return PerformanceMetrics(
            method="Direct DB",
            duration_seconds=duration,
            tick_count=0,
            ticks_per_second=0,
            error=f"Import error: {e}"
        )

    except Exception as e:
        end_time = time.perf_counter()
        duration = end_time - start_time

        print(f"\n✗ DB access failed: {e}")

        return PerformanceMetrics(
            method="Direct DB",
            duration_seconds=duration,
            tick_count=0,
            ticks_per_second=0,
            error=str(e)
        )


async def discover_initialized_ticks(
    web3: Web3,
    pool_address: ChecksumAddress,
    tick_spacing: int = 60
) -> List[int]:
    """
    Discover all initialized ticks for a pool using bitmap data.

    This fetches the tick bitmaps and finds all initialized ticks,
    which gives us a realistic workload to test performance.
    """
    print(f"\n{'='*60}")
    print(f"DISCOVERING INITIALIZED TICKS")
    print(f"{'='*60}")

    # Get pool's current tick to estimate range
    from web3 import Web3
    slot0_abi = [
        {
            "inputs": [],
            "name": "slot0",
            "outputs": [
                {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
                {"internalType": "int24", "name": "tick", "type": "int24"},
                {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
                {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
                {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
                {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
                {"internalType": "bool", "name": "unlocked", "type": "bool"}
            ],
            "stateMutability": "view",
            "type": "function"
        }
    ]

    contract = web3.eth.contract(address=pool_address, abi=slot0_abi)
    slot0 = contract.functions.slot0().call()
    current_tick = slot0[1]

    print(f"  Current tick: {current_tick}")
    print(f"  Tick spacing: {tick_spacing}")

    # Calculate word positions for a reasonable range around current tick
    # UniswapV3 typical range is around +/- 100,000 ticks from current
    tick_range = 100000
    lower_tick = current_tick - tick_range
    upper_tick = current_tick + tick_range

    word_positions = UniswapV3BitmapBatcher.calculate_word_positions(
        lower_tick, upper_tick, tick_spacing
    )

    print(f"  Tick range: [{lower_tick}, {upper_tick}]")
    print(f"  Bitmap word positions to check: {len(word_positions)}")

    # Fetch bitmaps
    bitmap_batcher = UniswapV3BitmapBatcher(web3)
    pool_word_positions = {pool_address: word_positions}

    print(f"  Fetching {len(word_positions)} bitmap words...")
    result = await bitmap_batcher.fetch_bitmap_data(pool_word_positions)

    if not result.success:
        print(f"  Failed to fetch bitmaps: {result.error}")
        return []

    pool_bitmaps = result.data.get(pool_address, {})
    print(f"  Got {len(pool_bitmaps)} bitmap words")

    # Find initialized ticks
    initialized_ticks = bitmap_batcher.find_initialized_ticks(pool_bitmaps, tick_spacing)

    print(f"  Found {len(initialized_ticks)} initialized ticks")

    if initialized_ticks:
        print(f"  Range: [{min(initialized_ticks)}, {max(initialized_ticks)}]")
        print(f"  Sample ticks: {initialized_ticks[:5]}...")

    return initialized_ticks


def print_comparison(metrics_list: List[PerformanceMetrics]):
    """Print a detailed comparison of performance metrics."""
    print(f"\n{'='*60}")
    print("PERFORMANCE COMPARISON SUMMARY")
    print(f"{'='*60}\n")

    # Table header
    print(f"{'Method':<15} {'Duration':<12} {'Ticks':<10} {'Rate (t/s)':<12} {'Memory (MB)':<12} {'Status':<10}")
    print("-" * 80)

    # Print each result
    for metrics in metrics_list:
        status = "ERROR" if metrics.error else "SUCCESS"
        memory = f"{metrics.memory_usage_mb:.2f}" if metrics.memory_usage_mb else "N/A"

        print(f"{metrics.method:<15} {metrics.duration_seconds:<12.3f} {metrics.tick_count:<10} "
              f"{metrics.ticks_per_second:<12.1f} {memory:<12} {status:<10}")

        if metrics.error:
            print(f"  Error: {metrics.error}")

    # Calculate speedup if both methods succeeded
    db_metrics = next((m for m in metrics_list if m.method == "Direct DB" and not m.error), None)
    rpc_metrics = next((m for m in metrics_list if m.method == "RPC Batch" and not m.error), None)

    if db_metrics and rpc_metrics:
        speedup = rpc_metrics.duration_seconds / db_metrics.duration_seconds
        print(f"\n{'='*60}")
        print(f"Direct DB is {speedup:.2f}x faster than RPC Batch")
        print(f"{'='*60}")

    print()


async def main():
    parser = argparse.ArgumentParser(description="Compare DB vs RPC performance for Uniswap V3 tick data")
    parser.add_argument(
        "--pool",
        default="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC/WETH 0.05%
        help="Pool address (default: USDC/WETH 0.05%)"
    )
    parser.add_argument(
        "--ticks",
        help="Comma-separated list of tick values (e.g., -887220,-887160,887220)"
    )
    parser.add_argument(
        "--discover-ticks",
        action="store_true",
        help="Discover and test all initialized ticks (realistic workload)"
    )
    parser.add_argument(
        "--tick-spacing",
        type=int,
        default=10,
        help="Tick spacing for the pool (default: 10 for 0.05%% fee tier)"
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("RETH_DB_PATH", "/mnt/data/reth/mainnet/db"),
        help="Path to Reth database"
    )

    args = parser.parse_args()

    # Setup Web3 connection
    config_manager = ConfigManager()
    chain_config = config_manager.chains.get_chain_config('ethereum')
    rpc_url = chain_config['rpc_url']

    print(f"Connecting to RPC: {rpc_url}")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    if not web3.is_connected():
        print(f"ERROR: Failed to connect to {rpc_url}")
        return 1

    print(f"Connected to chain ID: {web3.eth.chain_id}")

    # Normalize pool address
    pool_address = Web3.to_checksum_address(args.pool)
    print(f"Testing pool: {pool_address}")

    # Determine which ticks to test
    if args.discover_ticks:
        ticks = await discover_initialized_ticks(web3, pool_address, args.tick_spacing)
        if not ticks:
            print("ERROR: Failed to discover ticks or no initialized ticks found")
            return 1
    elif args.ticks:
        ticks = [int(t.strip()) for t in args.ticks.split(",")]
        print(f"Testing with {len(ticks)} specified ticks: {ticks}")
    else:
        # Default test ticks
        ticks = [-887220, -887160, -887100, 887100, 887160, 887220]
        print(f"Testing with {len(ticks)} default ticks: {ticks}")

    # Verify DB path exists
    if not os.path.exists(args.db_path):
        print(f"WARNING: DB path does not exist: {args.db_path}")
        print("DB test will likely fail. Set RETH_DB_PATH environment variable.")

    # Run performance tests
    metrics_list = []

    # Test 1: RPC Batch method
    rpc_metrics = await fetch_ticks_via_rpc(web3, pool_address, ticks)
    metrics_list.append(rpc_metrics)

    # Test 2: Direct DB method
    db_metrics = fetch_ticks_via_db(args.db_path, pool_address, ticks)
    metrics_list.append(db_metrics)

    # Print comparison
    print_comparison(metrics_list)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
