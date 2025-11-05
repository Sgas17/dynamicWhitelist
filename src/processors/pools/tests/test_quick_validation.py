"""
Quick validation test - assumes data already fetched and processed.

Run test_unified_liquidity_processor.py::test_liquidity_snapshot_validation once
to fetch and process data, then use this for fast validation.
"""

import logging
from pathlib import Path

import pytest
from web3 import Web3

from src.batchers.uniswap_v3_ticks import UniswapV3TickBatcher
from src.core.storage.postgres_liquidity import load_liquidity_snapshot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test pool
V3_TEST_POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640".lower()
V3_TICK_SPACING = 10
RPC_URL = "http://100.104.193.35:8545"

# Tick range constants
MIN_TICK = -887272
MAX_TICK = 887272


@pytest.mark.asyncio
async def test_quick_validation():
    """Fast validation - just compare snapshot vs live at snapshot block."""

    logger.info("\n" + "=" * 80)
    logger.info("QUICK VALIDATION TEST")
    logger.info("=" * 80 + "\n")

    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL))

    # Load V3 snapshot
    logger.info("Loading V3 snapshot from database...")
    v3_snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)

    if not v3_snapshot:
        pytest.skip("No snapshot found - run full test first")

    logger.info(f"✓ V3 snapshot loaded: {V3_TEST_POOL}")
    logger.info(f"  Total ticks in snapshot: {v3_snapshot['total_ticks']}")
    logger.info(f"  Snapshot block: {v3_snapshot['snapshot_block']}\n")

    # Calculate ALL possible ticks
    all_valid_ticks = list(
        range(MIN_TICK - (MIN_TICK % V3_TICK_SPACING), MAX_TICK + 1, V3_TICK_SPACING)
    )

    logger.info(f"  Tick range: {MIN_TICK} to {MAX_TICK}")
    logger.info(f"  Tick spacing: {V3_TICK_SPACING}")
    logger.info(f"  Total possible ticks to check: {len(all_valid_ticks)}")
    logger.info(f"  Ticks with liquidity in snapshot: {v3_snapshot['total_ticks']}\n")

    # Fetch live on-chain state AT SNAPSHOT BLOCK
    logger.info("Fetching live V3 tick data from chain...")
    logger.info(
        f"  ⚠ CRITICAL: Fetching at snapshot block {v3_snapshot['snapshot_block']}\n"
    )

    v3_batcher = UniswapV3TickBatcher(w3)

    from eth_utils import to_checksum_address

    v3_pool_checksum = to_checksum_address(V3_TEST_POOL)

    # Chunk ticks
    TICK_CHUNK_SIZE = 500
    total_chunks = (len(all_valid_ticks) + TICK_CHUNK_SIZE - 1) // TICK_CHUNK_SIZE
    logger.info(
        f"  Fetching {len(all_valid_ticks)} ticks in {total_chunks} chunks of {TICK_CHUNK_SIZE}"
    )

    live_tick_data = {}
    fetch_block_number = v3_snapshot["snapshot_block"]  # USE SNAPSHOT BLOCK!
    failed_chunks = 0

    for chunk_idx in range(0, len(all_valid_ticks), TICK_CHUNK_SIZE):
        tick_chunk = all_valid_ticks[chunk_idx : chunk_idx + TICK_CHUNK_SIZE]
        chunk_num = (chunk_idx // TICK_CHUNK_SIZE) + 1

        if chunk_num % 50 == 0:
            logger.info(f"  Processing chunk {chunk_num}/{total_chunks}...")

        batch_result = await v3_batcher.fetch_tick_data(
            pool_ticks={v3_pool_checksum: tick_chunk},
            block_number=fetch_block_number,  # Same block for all chunks!
        )

        if not batch_result.success:
            logger.warning(
                f"  ⚠ Chunk {chunk_num}/{total_chunks} failed: {batch_result.error}"
            )
            failed_chunks += 1
            continue

        # Aggregate results
        chunk_data = batch_result.data.get(v3_pool_checksum, {})
        live_tick_data.update(chunk_data)

    if failed_chunks > 0:
        logger.warning(f"  ⚠ {failed_chunks}/{total_chunks} chunks failed")

    logger.info(f"✓ Fetched live tick data at block {fetch_block_number}")
    logger.info(f"  Live ticks returned: {len(live_tick_data)}")
    logger.info(
        f"  Successfully fetched {total_chunks - failed_chunks}/{total_chunks} chunks\n"
    )

    # Compare snapshot vs live
    logger.info("Comparing V3 snapshot vs live state...")
    mismatches = []
    matches = 0

    for tick in all_valid_ticks:
        snapshot_tick_data = v3_snapshot["tick_data"].get(str(tick))
        live_tick_info = live_tick_data.get(tick)

        # Get liquidity values
        snap_gross = (
            snapshot_tick_data.get("liquidity_gross", 0) if snapshot_tick_data else 0
        )
        snap_net = (
            snapshot_tick_data.get("liquidity_net", 0) if snapshot_tick_data else 0
        )
        live_gross = live_tick_info.liquidity_gross if live_tick_info else 0
        live_net = live_tick_info.liquidity_net if live_tick_info else 0

        # Compare
        if snap_gross != live_gross or snap_net != live_net:
            mismatches.append(
                {
                    "tick": tick,
                    "issue": "liquidity_mismatch",
                    "snapshot_gross": snap_gross,
                    "snapshot_net": snap_net,
                    "live_gross": live_gross,
                    "live_net": live_net,
                }
            )
        else:
            matches += 1

    # Report results
    total_compared = len(all_valid_ticks)
    logger.info(f"  Total ticks compared: {total_compared}")
    logger.info(f"  Matches: {matches}")
    logger.info(f"  Mismatches: {len(mismatches)}")

    if mismatches:
        logger.warning("\n⚠ V3 MISMATCHES FOUND:")
        for i, mismatch in enumerate(mismatches[:10]):
            logger.warning(f"  {i + 1}. Tick {mismatch['tick']}: {mismatch['issue']}")
            if mismatch["issue"] == "liquidity_mismatch":
                logger.warning(
                    f"     Snapshot: gross={mismatch['snapshot_gross']}, net={mismatch['snapshot_net']}"
                )
                logger.warning(
                    f"     Live:     gross={mismatch['live_gross']}, net={mismatch['live_net']}"
                )
        if len(mismatches) > 10:
            logger.warning(f"  ... and {len(mismatches) - 10} more mismatches")

    match_percentage = (matches / total_compared * 100) if total_compared > 0 else 0
    logger.info(f"\n  Match percentage: {match_percentage:.2f}%")

    # Assert perfect or near-perfect match
    assert match_percentage >= 99.9, (
        f"Validation failed: only {match_percentage:.2f}% match (expected >= 99.9%)"
    )

    logger.info("\n✓ VALIDATION PASSED")
