"""
Complete V3 liquidity snapshot validation test.

This test validates our snapshot processor by:
1. Fetching ALL liquidity events from pool creation to current block
2. Building a complete snapshot from those events
3. Fetching the COMPLETE live state using the batch tick/bitmap fetchers
4. Comparing snapshot state to live state comprehensively

IMPORTANT: Only compares ticks that are valid for the pool's tick_spacing.
For example, a pool with tick_spacing=60 can only have ticks at -60, 0, 60, 120, etc.
"""

import asyncio
import logging
from pathlib import Path
from web3 import Web3

from src.processors.pools.unified_liquidity_processor import UnifiedLiquidityProcessor
from src.core.storage.postgres_liquidity import load_liquidity_snapshot
from src.core.storage.postgres_pools import get_database_engine
from src.fetchers.cryo_fetcher import CryoFetcher
from src.config import ConfigManager
from src.batchers.uniswap_v3_ticks import UniswapV3TickBatcher, UniswapV3BitmapBatcher
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# V3 event hashes
V3_MINT_EVENT = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
V3_BURN_EVENT = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

# RPC URL
RPC_URL = "http://100.104.193.35:8545"

# Tick range for V3 pools (full range)
MIN_TICK = -887272
MAX_TICK = 887272


async def test_snapshot_vs_live_state():
    """
    Complete test: Build snapshot from events and compare to live RPC state.
    """

    logger.info("="*80)
    logger.info("V3 SNAPSHOT VS LIVE STATE VALIDATION")
    logger.info("="*80)
    logger.info("")

    # Step 1: Get pool from database
    logger.info("Step 1: Getting V3 pool from database...")
    logger.info("-"*80)

    engine = get_database_engine()
    with engine.connect() as conn:
        # Get WBTC/USDC 0.3% - highly liquid, good test case
        result = conn.execute(text("""
            SELECT address, creation_block, fee, tick_spacing
            FROM network_1_dex_pools_cryo
            WHERE factory = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
            AND address = '0x99ac8ca7087fa4a2a1fb6357269965a2014abc35'
            LIMIT 1
        """)).fetchone()

        if not result:
            logger.error("Pool not found!")
            return

        pool_address = result.address
        creation_block = result.creation_block
        fee = result.fee
        tick_spacing = result.tick_spacing

    logger.info(f"✓ Pool: {pool_address}")
    logger.info(f"  Creation block: {creation_block:,}")
    logger.info(f"  Fee: {fee / 10000}%")
    logger.info(f"  Tick spacing: {tick_spacing}")
    logger.info("")

    # Step 2: Fetch ALL events from creation
    logger.info("Step 2: Fetching ALL liquidity events...")
    logger.info("-"*80)

    config = ConfigManager()
    data_dir = Path(config.base.DATA_DIR)
    output_dir = data_dir / "ethereum" / "uniswap_v3_modifyliquidity_events"

    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    current_block = w3.eth.block_number

    logger.info(f"Fetching blocks {creation_block:,} → {current_block:,}")
    logger.info(f"Range: {current_block - creation_block:,} blocks")
    logger.info("")

    fetcher = CryoFetcher(chain="ethereum", rpc_url=RPC_URL)
    result = await fetcher.fetch_logs(
        start_block=creation_block,
        end_block=current_block,
        contracts=[pool_address],
        events=[V3_MINT_EVENT, V3_BURN_EVENT],
        output_dir=str(output_dir)
    )

    if not result.success:
        logger.error(f"Failed to fetch events: {result.error}")
        return

    logger.info(f"✓ Events fetched")
    logger.info("")

    # Step 3: Build snapshot from events
    logger.info("Step 3: Building snapshot from events...")
    logger.info("-"*80)

    processor = UnifiedLiquidityProcessor(
        chain="ethereum",
        block_chunk_size=100000,
        enable_blacklist=False,
    )

    process_result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v3",
        start_block=creation_block,
        end_block=current_block,
        force_rebuild=True,
        cleanup_parquet=False,
    )

    logger.info(f"✓ Processed {process_result['total_events_processed']:,} events")
    logger.info(f"✓ Updated {process_result['pools_updated']} pools")
    logger.info("")

    # Step 4: Load snapshot
    logger.info("Step 4: Loading snapshot from database...")
    logger.info("-"*80)

    snapshot = load_liquidity_snapshot(pool_address, chain_id=1)

    if not snapshot:
        logger.error("❌ No snapshot found!")
        return

    snapshot_ticks = snapshot['tick_data']
    snapshot_bitmap = snapshot['tick_bitmap']

    logger.info(f"✓ Snapshot loaded")
    logger.info(f"  Snapshot block: {snapshot['snapshot_block']:,}")
    logger.info(f"  Ticks in snapshot: {len(snapshot_ticks):,}")
    logger.info(f"  Bitmap words: {len(snapshot_bitmap):,}")
    logger.info("")

    # Validate that all snapshot ticks are valid for tick_spacing
    invalid_ticks = [int(t) for t in snapshot_ticks.keys() if int(t) % tick_spacing != 0]
    if invalid_ticks:
        logger.error(f"❌ Found {len(invalid_ticks)} invalid ticks in snapshot (not divisible by {tick_spacing})")
        logger.error(f"   First 10: {invalid_ticks[:10]}")
        return

    logger.info(f"✓ All snapshot ticks are valid for tick_spacing={tick_spacing}")
    logger.info("")

    # Step 5: Fetch LIVE state using batchers
    logger.info("Step 5: Fetching LIVE state from RPC...")
    logger.info("-"*80)

    # First, get bitmap to find all initialized ticks
    bitmap_batcher = UniswapV3BitmapBatcher(w3)

    # Calculate which bitmap words we need to check (using tick_spacing)
    word_positions = bitmap_batcher.calculate_word_positions(
        MIN_TICK, MAX_TICK, tick_spacing
    )

    logger.info(f"Fetching {len(word_positions):,} bitmap words (tick_spacing={tick_spacing})...")

    bitmap_result = await bitmap_batcher.fetch_bitmap_data(
        {Web3.to_checksum_address(pool_address): word_positions}
    )

    if not bitmap_result.success:
        logger.error(f"Failed to fetch bitmap: {bitmap_result.error}")
        return

    live_bitmap = bitmap_result.data[Web3.to_checksum_address(pool_address)]
    logger.info(f"✓ Fetched {len(live_bitmap):,} bitmap words")

    # Find initialized ticks from bitmap (uses tick_spacing internally)
    initialized_ticks = bitmap_batcher.find_initialized_ticks(live_bitmap, tick_spacing)
    logger.info(f"✓ Found {len(initialized_ticks):,} initialized ticks from bitmap")

    # Verify all initialized ticks are valid for tick_spacing
    invalid_live_ticks = [t for t in initialized_ticks if t % tick_spacing != 0]
    if invalid_live_ticks:
        logger.error(f"❌ Found {len(invalid_live_ticks)} invalid ticks from bitmap!")
        logger.error(f"   First 10: {invalid_live_ticks[:10]}")
        return

    logger.info(f"✓ All live ticks are valid for tick_spacing={tick_spacing}")

    # Fetch tick data for all initialized ticks
    logger.info(f"Fetching tick data for {len(initialized_ticks):,} ticks...")

    tick_batcher = UniswapV3TickBatcher(w3)
    tick_result = await tick_batcher.fetch_tick_data(
        {Web3.to_checksum_address(pool_address): initialized_ticks}
    )

    if not tick_result.success:
        logger.error(f"Failed to fetch tick data: {tick_result.error}")
        return

    live_ticks = tick_result.data[Web3.to_checksum_address(pool_address)]
    live_block = tick_result.block_number

    logger.info(f"✓ Fetched tick data at block {live_block:,}")
    logger.info("")

    # Step 6: Compare snapshot to live state
    logger.info("Step 6: Comparing snapshot to live state...")
    logger.info("-"*80)

    # Compare tick counts
    snapshot_tick_count = len(snapshot_ticks)
    live_tick_count = len(live_ticks)

    logger.info(f"Tick counts:")
    logger.info(f"  Snapshot: {snapshot_tick_count:,} ticks")
    logger.info(f"  Live RPC: {live_tick_count:,} ticks")
    logger.info("")

    # Compare individual ticks
    matches = 0
    mismatches = 0
    snapshot_only = 0
    live_only = 0

    snapshot_tick_set = set(int(t) for t in snapshot_ticks.keys())
    live_tick_set = set(live_ticks.keys())

    # Check ticks in both
    for tick in snapshot_tick_set & live_tick_set:
        snap_data = snapshot_ticks[str(tick)]
        live_data = live_ticks[tick]

        snap_gross = int(snap_data['liquidityGross'])
        snap_net = int(snap_data['liquidityNet'])

        live_gross = live_data.liquidity_gross
        live_net = live_data.liquidity_net

        if snap_gross == live_gross and snap_net == live_net:
            matches += 1
        else:
            mismatches += 1
            if mismatches <= 5:  # Show first 5 mismatches
                logger.error(f"❌ Tick {tick} MISMATCH:")
                logger.error(f"   Snapshot: gross={snap_gross}, net={snap_net}")
                logger.error(f"   Live:     gross={live_gross}, net={live_net}")

    # Check ticks only in snapshot
    snapshot_only = len(snapshot_tick_set - live_tick_set)
    if snapshot_only > 0:
        logger.warning(f"⚠️  {snapshot_only} ticks only in snapshot (not in live state)")
        sample_snap_only = sorted(list(snapshot_tick_set - live_tick_set))[:5]
        for tick in sample_snap_only:
            snap_data = snapshot_ticks[str(tick)]
            logger.warning(f"   Tick {tick}: gross={snap_data['liquidityGross']}, net={snap_data['liquidityNet']}")

    # Check ticks only in live
    live_only = len(live_tick_set - snapshot_tick_set)
    if live_only > 0:
        logger.warning(f"⚠️  {live_only} ticks only in live state (not in snapshot)")
        sample_live_only = sorted(list(live_tick_set - snapshot_tick_set))[:5]
        for tick in sample_live_only:
            live_data = live_ticks[tick]
            logger.warning(f"   Tick {tick}: gross={live_data.liquidity_gross}, net={live_data.liquidity_net}")

    logger.info("")

    # Compare bitmaps
    logger.info("Comparing bitmaps...")
    bitmap_matches = 0
    bitmap_mismatches = 0

    snapshot_bitmap_ints = {int(k): int(v) for k, v in snapshot_bitmap.items()}

    for word_pos in set(snapshot_bitmap_ints.keys()) | set(live_bitmap.keys()):
        snap_val = snapshot_bitmap_ints.get(word_pos, 0)
        live_val = live_bitmap.get(word_pos, 0)

        if snap_val == live_val:
            bitmap_matches += 1
        else:
            bitmap_mismatches += 1
            if bitmap_mismatches <= 3:
                logger.error(f"❌ Bitmap word {word_pos} MISMATCH:")
                logger.error(f"   Snapshot: {bin(snap_val)}")
                logger.error(f"   Live:     {bin(live_val)}")

    logger.info("")

    # Summary
    logger.info("="*80)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*80)
    logger.info(f"Pool: {pool_address}")
    logger.info(f"Tick spacing: {tick_spacing}")
    logger.info(f"Snapshot block: {snapshot['snapshot_block']:,}")
    logger.info(f"Live block: {live_block:,}")
    logger.info(f"Block difference: {live_block - snapshot['snapshot_block']:,}")
    logger.info("")
    logger.info("Tick Comparison:")
    logger.info(f"  ✅ Matches: {matches:,}")
    logger.info(f"  ❌ Mismatches: {mismatches:,}")
    logger.info(f"  ⚠️  Snapshot only: {snapshot_only:,}")
    logger.info(f"  ⚠️  Live only: {live_only:,}")
    logger.info("")
    logger.info("Bitmap Comparison:")
    logger.info(f"  ✅ Matches: {bitmap_matches:,}")
    logger.info(f"  ❌ Mismatches: {bitmap_mismatches:,}")
    logger.info("")

    # Final verdict
    total_ticks = len(snapshot_tick_set | live_tick_set)
    match_rate = (matches / total_ticks * 100) if total_ticks > 0 else 0

    if mismatches == 0 and snapshot_only == 0 and live_only == 0:
        logger.info("✅ PERFECT MATCH - Snapshot is 100% accurate!")
    elif match_rate >= 99:
        logger.info(f"✅ EXCELLENT - {match_rate:.2f}% match rate")
        logger.info("   Small discrepancies likely due to block difference")
    elif match_rate >= 95:
        logger.info(f"⚠️  GOOD - {match_rate:.2f}% match rate")
        logger.info("   Some discrepancies found - review recommended")
    else:
        logger.info(f"❌ POOR - {match_rate:.2f}% match rate")
        logger.info("   Significant discrepancies - investigation needed")

    logger.info("="*80)


if __name__ == "__main__":
    asyncio.run(test_snapshot_vs_live_state())