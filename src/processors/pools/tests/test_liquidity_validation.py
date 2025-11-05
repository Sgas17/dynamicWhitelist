"""
Validation tests for liquidity snapshots against on-chain state.

This test suite validates that calculated liquidity snapshots match the actual
on-chain state by:
1. Processing all historical events for a pool
2. Fetching current on-chain state using batchers
3. Comparing calculated vs on-chain tick data and bitmaps
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import pytest
import pytest_asyncio
from eth_typing import ChecksumAddress
from sqlalchemy import text
from web3 import Web3

from src.batchers.uniswap_v3_ticks import UniswapV3BitmapBatcher, UniswapV3TickBatcher
from src.batchers.uniswap_v4_ticks import UniswapV4TickBatcher
from src.core.storage.postgres_liquidity import load_liquidity_snapshot
from src.core.storage.postgres_pools import get_database_engine

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# RPC configuration
RPC_URL = "http://100.104.193.35:8545"

# Test pools - choose pools with moderate activity for validation
# WETH/USDC 0.05% - Well-known, high volume pool
WETH_USDC_V3 = "0x88e6A0c2dDd26FEEb64F039a2c41296FcB3f5640"

# WETH/USDT 0.05%
WETH_USDT_V3 = "0x11b815efB8f581194ae79006d24E0d814B7697F6"


@pytest.fixture
def web3():
    """Web3 instance for RPC calls."""
    return Web3(Web3.HTTPProvider(RPC_URL))


@pytest.fixture
def v3_tick_batcher(web3):
    """UniswapV3 tick data batcher."""
    return UniswapV3TickBatcher(web3=web3)


@pytest.fixture
def v3_bitmap_batcher(web3):
    """UniswapV3 bitmap batcher."""
    return UniswapV3BitmapBatcher(web3=web3)


def get_processor():
    """Import and create processor at runtime."""
    from src.processors.pools.unified_liquidity_processor import (
        UnifiedLiquidityProcessor,
    )

    return UnifiedLiquidityProcessor(
        chain="ethereum",
        block_chunk_size=100000,
        enable_blacklist=False,
    )


def get_pool_info(pool_address: str) -> Dict:
    """Get pool info from database."""
    engine = get_database_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT address, factory, asset0, asset1, fee, tick_spacing, creation_block
                FROM network_1_dex_pools_cryo
                WHERE address = :pool
            """),
            {"pool": pool_address.lower()},
        )
        row = result.fetchone()
        if not row:
            raise ValueError(f"Pool {pool_address} not found in database")

        return {
            "address": row.address,
            "factory": row.factory,
            "asset0": row.asset0,
            "asset1": row.asset1,
            "fee": row.fee,
            "tick_spacing": row.tick_spacing,
            "creation_block": row.creation_block,
        }


class ValidationResult:
    """Result of liquidity snapshot validation."""

    def __init__(self):
        self.is_valid = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.tick_count_calculated = 0
        self.tick_count_onchain = 0
        self.tick_match_count = 0
        self.liquidity_mismatches: List[Dict] = []
        self.missing_ticks: List[int] = []
        self.extra_ticks: List[int] = []
        self.bitmap_mismatches: List[
            Tuple[int, int, int]
        ] = []  # (word, calculated, onchain)

    @property
    def tick_match_rate(self) -> float:
        """Percentage of ticks that match."""
        if self.tick_count_onchain == 0:
            return 0.0
        return self.tick_match_count / self.tick_count_onchain

    def add_error(self, error: str):
        """Add error and mark as invalid."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str):
        """Add warning (doesn't invalidate)."""
        self.warnings.append(warning)

    def summary(self) -> str:
        """Get validation summary."""
        lines = [
            "=" * 80,
            "VALIDATION SUMMARY",
            "=" * 80,
            f"Status: {'✅ VALID' if self.is_valid else '❌ INVALID'}",
            f"",
            f"Tick Statistics:",
            f"  Calculated ticks: {self.tick_count_calculated}",
            f"  On-chain ticks:   {self.tick_count_onchain}",
            f"  Matching ticks:   {self.tick_match_count}",
            f"  Match rate:       {self.tick_match_rate * 100:.2f}%",
            f"",
        ]

        if self.missing_ticks:
            lines.append(
                f"Missing ticks (in on-chain but not calculated): {len(self.missing_ticks)}"
            )
            lines.append(f"  Examples: {self.missing_ticks[:5]}")
            lines.append("")

        if self.extra_ticks:
            lines.append(
                f"Extra ticks (in calculated but not on-chain): {len(self.extra_ticks)}"
            )
            lines.append(f"  Examples: {self.extra_ticks[:5]}")
            lines.append("")

        if self.liquidity_mismatches:
            lines.append(f"Liquidity mismatches: {len(self.liquidity_mismatches)}")
            for mismatch in self.liquidity_mismatches[:5]:
                lines.append(
                    f"  Tick {mismatch['tick']}: "
                    f"calculated_net={mismatch['calculated_net']}, "
                    f"onchain_net={mismatch['onchain_net']}"
                )
            lines.append("")

        if self.bitmap_mismatches:
            lines.append(f"Bitmap mismatches: {len(self.bitmap_mismatches)}")
            for word, calc, onchain in self.bitmap_mismatches[:5]:
                lines.append(f"  Word {word}: calculated={calc}, onchain={onchain}")
            lines.append("")

        if self.errors:
            lines.append("ERRORS:")
            for error in self.errors:
                lines.append(f"  ❌ {error}")
            lines.append("")

        if self.warnings:
            lines.append("WARNINGS:")
            for warning in self.warnings:
                lines.append(f"  ⚠️  {warning}")
            lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)


async def validate_v3_snapshot(
    pool_address: str,
    snapshot: Dict,
    tick_batcher: UniswapV3TickBatcher,
    bitmap_batcher: UniswapV3BitmapBatcher,
) -> ValidationResult:
    """
    Validate a V3 liquidity snapshot against on-chain state.

    Args:
        pool_address: Pool address
        snapshot: Calculated snapshot from database
        tick_batcher: Batcher for fetching tick data
        bitmap_batcher: Batcher for fetching bitmap data

    Returns:
        ValidationResult with comparison details
    """
    result = ValidationResult()

    logger.info(f"Validating snapshot for pool {pool_address}")
    logger.info(f"Snapshot has {len(snapshot['tick_data'])} initialized ticks")

    # Convert pool address to checksum format
    pool_checksum = Web3.to_checksum_address(pool_address)

    # Get list of ticks from snapshot
    calculated_ticks = list(snapshot["tick_data"].keys())
    result.tick_count_calculated = len(calculated_ticks)

    if not calculated_ticks:
        result.add_warning("No ticks in calculated snapshot")
        return result

    # Fetch on-chain tick data
    logger.info(f"Fetching on-chain tick data for {len(calculated_ticks)} ticks...")
    tick_result = await tick_batcher.fetch_tick_data({pool_checksum: calculated_ticks})

    if not tick_result.success:
        result.add_error(f"Failed to fetch on-chain tick data: {tick_result.error}")
        return result

    onchain_ticks = tick_result.data.get(pool_checksum, {})
    result.tick_count_onchain = len(onchain_ticks)

    logger.info(f"Fetched {len(onchain_ticks)} ticks from on-chain")

    # Compare tick data
    for tick in calculated_ticks:
        calculated_data = snapshot["tick_data"][tick]
        onchain_data = onchain_ticks.get(tick)

        if onchain_data is None:
            result.extra_ticks.append(tick)
            result.add_error(f"Tick {tick} exists in snapshot but not on-chain")
            continue

        # Compare liquidityNet (most important for accuracy)
        calculated_net = calculated_data.get("liquidityNet", 0)
        onchain_net = onchain_data.liquidity_net

        if calculated_net != onchain_net:
            result.liquidity_mismatches.append(
                {
                    "tick": tick,
                    "calculated_net": calculated_net,
                    "onchain_net": onchain_net,
                    "difference": abs(calculated_net - onchain_net),
                }
            )
            result.add_error(
                f"Tick {tick} liquidityNet mismatch: "
                f"calculated={calculated_net}, onchain={onchain_net}"
            )
        else:
            result.tick_match_count += 1

        # Compare liquidityGross
        calculated_gross = calculated_data.get("liquidityGross", 0)
        onchain_gross = onchain_data.liquidity_gross

        if calculated_gross != onchain_gross:
            result.add_warning(
                f"Tick {tick} liquidityGross mismatch: "
                f"calculated={calculated_gross}, onchain={onchain_gross}"
            )

    # Check for missing ticks (on-chain but not in snapshot)
    for tick in onchain_ticks:
        if tick not in calculated_ticks:
            result.missing_ticks.append(tick)
            result.add_error(f"Tick {tick} exists on-chain but not in snapshot")

    # Fetch and compare bitmaps
    logger.info("Validating tick bitmaps...")
    calculated_bitmap = snapshot.get("tick_bitmap", {})

    if calculated_bitmap:
        # Get word positions from calculated bitmap
        word_positions = [int(word_pos) for word_pos in calculated_bitmap.keys()]

        bitmap_result = await bitmap_batcher.fetch_bitmap_data(
            {pool_checksum: word_positions}
        )

        if bitmap_result.success:
            onchain_bitmap = bitmap_result.data.get(pool_checksum, {})

            for word_pos in word_positions:
                calculated_value = int(calculated_bitmap.get(str(word_pos), 0))
                onchain_value = onchain_bitmap.get(word_pos, 0)

                if calculated_value != onchain_value:
                    result.bitmap_mismatches.append(
                        (word_pos, calculated_value, onchain_value)
                    )
                    result.add_error(
                        f"Bitmap word {word_pos} mismatch: "
                        f"calculated={calculated_value}, onchain={onchain_value}"
                    )
        else:
            result.add_warning(f"Failed to fetch bitmap data: {bitmap_result.error}")

    return result


@pytest.mark.asyncio
async def test_validate_small_pool_snapshot(v3_tick_batcher, v3_bitmap_batcher):
    """
    Validate liquidity snapshot for a small test pool.

    This test processes a limited block range for a pool and validates
    the snapshot against on-chain state.
    """
    logger.info("=" * 80)
    logger.info("TEST: Validate Small Pool Snapshot")
    logger.info("=" * 80)

    # Use a pool that should have some activity in first 100k blocks
    pool_address = WETH_USDC_V3
    pool_info = get_pool_info(pool_address)

    logger.info(f"Pool: {pool_address}")
    logger.info(f"Creation block: {pool_info['creation_block']}")

    # Process events (use existing snapshot if available)
    processor = get_processor()

    # Load snapshot from database
    snapshot = load_liquidity_snapshot(pool_address, chain_id=1)

    if not snapshot:
        logger.info("No snapshot found, processing events first...")
        result = await processor.process_liquidity_snapshots(
            protocol="uniswap_v3",
            start_block=pool_info["creation_block"],
            end_block=pool_info["creation_block"] + 100000,  # First 100k blocks
            force_rebuild=True,
            cleanup_parquet=False,
        )

        logger.info(f"Processed {result.get('total_events_processed', 0)} events")

        # Load the created snapshot
        snapshot = load_liquidity_snapshot(pool_address, chain_id=1)

    if not snapshot:
        pytest.skip("Could not create or load snapshot for validation")

    # Validate snapshot
    validation_result = await validate_v3_snapshot(
        pool_address=pool_address,
        snapshot=snapshot,
        tick_batcher=v3_tick_batcher,
        bitmap_batcher=v3_bitmap_batcher,
    )

    # Print detailed summary
    logger.info(validation_result.summary())

    # Assert validation passed
    assert validation_result.is_valid, (
        f"Validation failed:\n{validation_result.summary()}"
    )
    assert validation_result.tick_match_rate >= 0.95, (
        f"Tick match rate {validation_result.tick_match_rate:.2%} below 95% threshold"
    )


@pytest.mark.asyncio
@pytest.mark.slow
async def test_validate_full_pool_history(v3_tick_batcher, v3_bitmap_batcher):
    """
    Validate liquidity snapshot for a pool's complete history.

    This test processes ALL events from pool creation to current block
    and validates the final snapshot matches on-chain state 100%.
    """
    logger.info("=" * 80)
    logger.info("TEST: Validate Full Pool History")
    logger.info("=" * 80)

    pool_address = WETH_USDT_V3  # Use different pool than small test
    pool_info = get_pool_info(pool_address)

    logger.info(f"Pool: {pool_address}")
    logger.info(f"Processing from block {pool_info['creation_block']} to latest...")

    # Process ALL events for this pool
    processor = get_processor()
    result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v3",
        start_block=pool_info["creation_block"],
        end_block=None,  # Process to latest
        force_rebuild=True,
        cleanup_parquet=False,
    )

    logger.info(f"Processed {result.get('total_events_processed', 0)} events")
    logger.info(f"Updated {result.get('pools_updated', 0)} pools")

    # Load snapshot
    snapshot = load_liquidity_snapshot(pool_address, chain_id=1)

    assert snapshot is not None, "Failed to create snapshot"

    # Validate snapshot
    validation_result = await validate_v3_snapshot(
        pool_address=pool_address,
        snapshot=snapshot,
        tick_batcher=v3_tick_batcher,
        bitmap_batcher=v3_bitmap_batcher,
    )

    # Print detailed summary
    logger.info(validation_result.summary())

    # For full history, we expect 100% accuracy
    assert validation_result.is_valid, (
        f"Validation failed:\n{validation_result.summary()}"
    )
    assert validation_result.tick_match_rate == 1.0, (
        f"Full history validation must be 100% accurate, got {validation_result.tick_match_rate:.2%}"
    )
    assert len(validation_result.missing_ticks) == 0, "Should have no missing ticks"
    assert len(validation_result.extra_ticks) == 0, "Should have no extra ticks"
    assert len(validation_result.liquidity_mismatches) == 0, (
        "Should have no liquidity mismatches"
    )
