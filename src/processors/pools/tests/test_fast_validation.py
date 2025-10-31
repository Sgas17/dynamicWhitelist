"""
Fast validation test - process only recent 10K blocks to test accuracy.

PREREQUISITE: Run the full validation test first to create baseline snapshot.
This test loads the existing snapshot and processes recent events incrementally.

This tests the processor logic without requiring hours of data fetching.
"""

import pytest
import logging
from pathlib import Path
from web3 import Web3

from src.core.storage.postgres_liquidity import load_liquidity_snapshot
from src.batchers.uniswap_v3_ticks import UniswapV3TickBatcher
from src.fetchers.ethereum_fetcher import EthereumFetcher
from src.config import ConfigManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test pool
V3_TEST_POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640".lower()
V3_TICK_SPACING = 10
RPC_URL = "http://100.104.193.35:8545"

# Tick range constants
MIN_TICK = -887272
MAX_TICK = 887272

# Event hashes
V3_MINT_EVENT = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
V3_BURN_EVENT = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"


def get_processor():
    """Import and create processor."""
    from src.processors.pools.unified_liquidity_processor import UnifiedLiquidityProcessor
    return UnifiedLiquidityProcessor(
        chain="ethereum",
        block_chunk_size=100000,
        enable_blacklist=False,
    )


@pytest.mark.asyncio
async def test_fast_validation():
    """
    Fast validation - fetch only last 10K blocks and validate.

    This tests that:
    1. Fetcher works correctly
    2. Processor decodes events correctly
    3. Snapshot calculation is accurate
    4. Comparison against live state is correct
    """

    logger.info("\n" + "="*80)
    logger.info("FAST VALIDATION TEST (10K blocks)")
    logger.info("="*80 + "\n")

    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    current_block = w3.eth.block_number

    # Fetch only recent 10K blocks
    start_block = current_block - 10000
    end_block = current_block

    logger.info(f"Current block: {current_block}")
    logger.info(f"Testing range: {start_block} to {end_block} (10K blocks)\n")

    # Fetch test data
    logger.info("Step 1: Fetching recent events...")
    fetcher = EthereumFetcher(rpc_url=RPC_URL)

    config = ConfigManager()
    data_dir = Path(config.base.DATA_DIR)
    test_output = data_dir / "ethereum" / "uniswap_v3_fast_test"

    # Clean old data
    import shutil
    if test_output.exists():
        shutil.rmtree(test_output)
    test_output.mkdir(parents=True, exist_ok=True)

    fetch_result = await fetcher.fetch_logs(
        start_block=start_block,
        end_block=end_block,
        contracts=[V3_TEST_POOL],
        events=[V3_MINT_EVENT, V3_BURN_EVENT],
        output_dir=str(test_output)
    )

    if not fetch_result.success:
        raise RuntimeError(f"Fetch failed: {fetch_result.error}")

    logger.info(f"✓ Fetched {fetch_result.fetched_blocks} blocks\n")

    # Process events
    # Check for existing snapshot (prerequisite)
    logger.info("Step 2: Checking for existing snapshot...")
    from src.core.storage.postgres_liquidity import load_liquidity_snapshot
    existing_snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)

    if not existing_snapshot:
        pytest.skip(
            "No existing snapshot found. Run test_unified_liquidity_processor.py::test_liquidity_snapshot_validation first "
            "to create baseline snapshot with full event history."
        )

    logger.info(f"✓ Found existing snapshot at block {existing_snapshot['snapshot_block']}")
    logger.info(f"  Ticks with liquidity: {existing_snapshot['total_ticks']}\\n")

    # Process recent events incrementally
    logger.info("Step 3: Processing recent events incrementally...")
    processor = get_processor()

    # Temporarily point processor to test data
    import src.processors.pools.unified_liquidity_processor as proc_module
    original_method = proc_module.UnifiedLiquidityProcessor._get_events_path

    def mock_get_events_path(self, protocol):
        return test_output

    proc_module.UnifiedLiquidityProcessor._get_events_path = mock_get_events_path

    try:
        # CRITICAL: Use force_rebuild=False to load existing snapshot state
        # This allows us to process only recent events incrementally
        result = await processor.process_liquidity_snapshots(
            protocol="uniswap_v3",
            start_block=0,
            force_rebuild=False,  # Load existing state!
            cleanup_parquet=False,
        )
    finally:
        proc_module.UnifiedLiquidityProcessor._get_events_path = original_method

    logger.info(f"✓ Processed {result['total_events_processed']} events")
    logger.info(f"✓ Snapshot block: {result['end_block']}\n")

    # Load updated snapshot
    logger.info("Step 4: Loading updated snapshot...")
    snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)

    if not snapshot:
        pytest.fail("No snapshot created")

    logger.info(f"✓ Snapshot loaded")
    logger.info(f"  Ticks with liquidity: {snapshot['total_ticks']}")
    logger.info(f"  Snapshot block: {snapshot['snapshot_block']}\n")

    # Fetch live state at snapshot block
    logger.info("Step 5: Fetching live state at snapshot block...")
    logger.info(f"  Block: {snapshot['snapshot_block']}\n")

    # Only check ticks that have liquidity in snapshot
    ticks_to_check = [int(tick) for tick in snapshot['tick_data'].keys()]

    logger.info(f"  Checking {len(ticks_to_check)} ticks with liquidity")

    v3_batcher = UniswapV3TickBatcher(w3)
    from eth_utils import to_checksum_address
    v3_pool_checksum = to_checksum_address(V3_TEST_POOL)

    batch_result = await v3_batcher.fetch_tick_data(
        pool_ticks={v3_pool_checksum: ticks_to_check},
        block_number=snapshot['snapshot_block']
    )

    if not batch_result.success:
        pytest.fail(f"Failed to fetch live ticks: {batch_result.error}")

    live_tick_data = batch_result.data.get(v3_pool_checksum, {})
    logger.info(f"✓ Fetched {len(live_tick_data)} live ticks\n")

    # Compare
    logger.info("Step 6: Comparing snapshot vs live...")
    mismatches = []

    for tick_str, snap_data in snapshot['tick_data'].items():
        tick = int(tick_str)
        live_data = live_tick_data.get(tick)

        snap_gross = snap_data.get('liquidity_gross', 0)
        snap_net = snap_data.get('liquidity_net', 0)
        live_gross = live_data.liquidity_gross if live_data else 0
        live_net = live_data.liquidity_net if live_data else 0

        if snap_gross != live_gross or snap_net != live_net:
            mismatches.append({
                'tick': tick,
                'snap_gross': snap_gross,
                'snap_net': snap_net,
                'live_gross': live_gross,
                'live_net': live_net,
            })

    logger.info(f"  Total ticks compared: {len(ticks_to_check)}")
    logger.info(f"  Matches: {len(ticks_to_check) - len(mismatches)}")
    logger.info(f"  Mismatches: {len(mismatches)}")

    if mismatches:
        logger.warning("\n⚠ MISMATCHES FOUND:")
        for i, m in enumerate(mismatches[:10]):
            logger.warning(f"  {i+1}. Tick {m['tick']}")
            logger.warning(f"     Snapshot: gross={m['snap_gross']}, net={m['snap_net']}")
            logger.warning(f"     Live:     gross={m['live_gross']}, net={m['live_net']}")
        if len(mismatches) > 10:
            logger.warning(f"  ... and {len(mismatches) - 10} more")

    match_pct = ((len(ticks_to_check) - len(mismatches)) / len(ticks_to_check) * 100) if ticks_to_check else 100
    logger.info(f"\n  Match percentage: {match_pct:.2f}%")

    # Assert perfection
    assert len(mismatches) == 0, f"Found {len(mismatches)} mismatches - snapshot doesn't match live state"

    logger.info("\n✓ VALIDATION PASSED - PERFECT MATCH!")
