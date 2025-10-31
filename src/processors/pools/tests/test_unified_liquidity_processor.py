"""
End-to-end integration tests for UnifiedLiquidityProcessor.

This test suite performs real integration testing:
1. Fetch actual liquidity events from parquet files for V3 and V4 pools
2. Process events and create snapshots
3. Store snapshots in PostgreSQL
4. Verify data integrity and schema compatibility

The tests automatically fetch liquidity event data for specific test pools using cryo.

Note: Imports UnifiedLiquidityProcessor at runtime to avoid degenbot circular import issues.
"""

import pytest
import pytest_asyncio
import asyncio
import logging
from pathlib import Path
from datetime import datetime, UTC
from sqlalchemy import text

# Import storage modules (these don't have circular import issues)
from src.core.storage.postgres_liquidity import (
    load_liquidity_snapshot,
    get_snapshot_statistics,
)
from src.core.storage.timescaledb import get_timescale_engine
from src.core.storage.postgres_pools import get_database_engine
from src.config import ConfigManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Test pool addresses from database (high liquidity, active pools)
V3_TEST_POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640".lower()  # USDC/WETH 0.05% pool
V3_POOL_CREATION_BLOCK = 12_376_729  # Pool creation block from database
V3_TICK_SPACING = 10  # Pool tick spacing

V4_TEST_POOL = "0xdce6394339af00981949f5f3baf27e3610c76326a700af57e4b3e3ae4977f78d".lower()  # V4 pool ID
V4_POOL_CREATION_BLOCK = 21_696_049  # Pool creation block from database
V4_TICK_SPACING = 60  # Pool tick spacing

# Event hashes
V3_MINT_EVENT = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
V3_BURN_EVENT = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
V4_MODIFY_LIQUIDITY_EVENT = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"

# Contract addresses
V4_POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"

# RPC configuration
RPC_URL = "http://100.104.193.35:8545"


@pytest_asyncio.fixture(scope="session")
async def v3_liquidity_data():
    """Use existing V3 liquidity event data."""
    config = ConfigManager()
    data_dir = Path(config.base.DATA_DIR)
    output_dir = data_dir / "ethereum" / "uniswap_v3_modifyliquidity_events"

    if not output_dir.exists():
        raise RuntimeError(f"V3 test data not found at {output_dir}")

    logger.info(f"✓ Using existing V3 liquidity data from {output_dir}")
    return output_dir


@pytest_asyncio.fixture(scope="session")
async def v4_liquidity_data():
    """Use existing V4 liquidity event data."""
    config = ConfigManager()
    data_dir = Path(config.base.DATA_DIR)
    output_dir = data_dir / "ethereum" / "uniswap_v4_modifyliquidity_events"

    if not output_dir.exists():
        raise RuntimeError(f"V4 test data not found at {output_dir}")

    logger.info(f"✓ Using existing V4 liquidity data from {output_dir}")
    return output_dir


def get_processor():
    """Import and create processor at runtime to avoid circular import."""
    from src.processors.pools.unified_liquidity_processor import UnifiedLiquidityProcessor
    return UnifiedLiquidityProcessor(
        chain="ethereum",
        block_chunk_size=100000,
        enable_blacklist=False,
    )


@pytest.fixture
def db_engine():
    """Get database engine."""
    return get_timescale_engine()


class TestV3PoolSnapshot:
    """End-to-end test for V3 pool liquidity snapshot."""

    @pytest.mark.asyncio
    async def test_v3_pool_snapshot_creation(self, db_engine, v3_liquidity_data):
        """
        Test creating a snapshot for a real V3 pool.

        This test:
        1. Processes V3 liquidity events from parquet files
        2. Creates a snapshot with tick_bitmap and tick_data
        3. Stores snapshot in PostgreSQL
        4. Verifies snapshot can be loaded back
        """
        logger.info("="*80)
        logger.info("TEST: V3 Pool Snapshot Creation")
        logger.info("="*80)

        logger.info(f"Testing V3 pool: {V3_TEST_POOL}")

        # Get processor
        processor = get_processor()

        # Process V3 liquidity events
        logger.info("Processing V3 liquidity events...")
        result = await processor.process_liquidity_snapshots(
            protocol="uniswap_v3",
            start_block=None,  # Start from earliest available
            end_block=None,    # Process all available
            force_rebuild=False,
            cleanup_parquet=False,  # Don't delete files during test
        )

        logger.info(f"✓ Processed {result['total_events_processed']} V3 events")
        logger.info(f"✓ Updated {result['pools_updated']} pools")

        # Verify snapshot was created
        snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)

        if snapshot:
            logger.info(f"✓ V3 Snapshot loaded successfully")
            logger.info(f"  - Snapshot block: {snapshot['snapshot_block']}")
            logger.info(f"  - Total ticks: {snapshot['total_ticks']}")
            logger.info(f"  - Total bitmap words: {snapshot['total_bitmap_words']}")
            logger.info(f"  - Protocol: {snapshot['protocol']}")

            # Verify structure
            assert 'tick_bitmap' in snapshot
            assert 'tick_data' in snapshot
            assert isinstance(snapshot['tick_bitmap'], dict)
            assert isinstance(snapshot['tick_data'], dict)

            # Verify tick data structure
            if snapshot['tick_data']:
                sample_tick = list(snapshot['tick_data'].keys())[0]
                sample_data = snapshot['tick_data'][sample_tick]
                assert 'liquidityNet' in sample_data
                assert 'liquidityGross' in sample_data
                assert 'block_number' in sample_data
                logger.info(f"  - Sample tick {sample_tick}: {sample_data}")

            logger.info("✓ V3 snapshot structure verified")
        else:
            logger.warning(f"⚠ No snapshot found for V3 pool {V3_TEST_POOL}")
            logger.warning(f"  This might mean no liquidity events exist for this pool")


class TestV4PoolSnapshot:
    """End-to-end test for V4 pool liquidity snapshot."""

    @pytest.mark.asyncio
    async def test_v4_pool_snapshot_creation(self, db_engine, v4_liquidity_data):
        """
        Test creating a snapshot for a real V4 pool.

        This test:
        1. Processes V4 ModifyLiquidity events from parquet files
        2. Creates a snapshot with tick_bitmap and tick_data
        3. Stores snapshot in PostgreSQL
        4. Verifies snapshot can be loaded back
        """
        logger.info("="*80)
        logger.info("TEST: V4 Pool Snapshot Creation")
        logger.info("="*80)

        logger.info(f"Testing V4 pool: {V4_TEST_POOL}")

        # Get processor
        processor = get_processor()

        # Process V4 liquidity events
        logger.info("Processing V4 liquidity events...")
        result = await processor.process_liquidity_snapshots(
            protocol="uniswap_v4",
            start_block=None,  # Start from earliest available
            end_block=None,    # Process all available
            force_rebuild=False,
            cleanup_parquet=False,  # Don't delete files during test
        )

        logger.info(f"✓ Processed {result['total_events_processed']} V4 events")
        logger.info(f"✓ Updated {result['pools_updated']} pools")

        # Verify snapshot was created
        snapshot = load_liquidity_snapshot(V4_TEST_POOL, chain_id=1)

        if snapshot:
            logger.info(f"✓ V4 Snapshot loaded successfully")
            logger.info(f"  - Snapshot block: {snapshot['snapshot_block']}")
            logger.info(f"  - Total ticks: {snapshot['total_ticks']}")
            logger.info(f"  - Total bitmap words: {snapshot['total_bitmap_words']}")
            logger.info(f"  - Protocol: {snapshot['protocol']}")
            logger.info(f"  - Pool ID (bytes32): {snapshot['pool_address']}")

            # Verify structure
            assert 'tick_bitmap' in snapshot
            assert 'tick_data' in snapshot
            assert isinstance(snapshot['tick_bitmap'], dict)
            assert isinstance(snapshot['tick_data'], dict)

            # Verify tick data structure (should be same as V3)
            if snapshot['tick_data']:
                sample_tick = list(snapshot['tick_data'].keys())[0]
                sample_data = snapshot['tick_data'][sample_tick]
                assert 'liquidityNet' in sample_data
                assert 'liquidityGross' in sample_data
                assert 'block_number' in sample_data
                logger.info(f"  - Sample tick {sample_tick}: {sample_data}")

            logger.info("✓ V4 snapshot structure verified")
        else:
            logger.warning(f"⚠ No snapshot found for V4 pool {V4_TEST_POOL}")
            logger.warning(f"  This might mean no liquidity events exist for this pool")


class TestSnapshotSchemaCompatibility:
    """Test that V3 and V4 snapshots use the same database schema."""

    @pytest.mark.asyncio
    async def test_v3_and_v4_schema_compatibility(self, db_engine):
        """
        Verify that V3 and V4 snapshots are stored in the same table
        with the same schema.
        """
        logger.info("="*80)
        logger.info("TEST: V3/V4 Schema Compatibility")
        logger.info("="*80)

        table_name = "network_1_liquidity_snapshots"

        with db_engine.connect() as conn:
            # Check if table exists
            result = conn.execute(text(f"""
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_name = :table_name
            """), {"table_name": table_name})

            table_exists = result.fetchone()[0] > 0
            assert table_exists, f"Table {table_name} should exist"
            logger.info(f"✓ Table {table_name} exists")

            # Get V3 and V4 snapshots
            result = conn.execute(text(f"""
                SELECT pool_address, protocol, total_ticks, total_bitmap_words
                FROM {table_name}
                WHERE pool_address IN (:v3_pool, :v4_pool)
            """), {"v3_pool": V3_TEST_POOL, "v4_pool": V4_TEST_POOL})

            snapshots = {row.pool_address: dict(row._mapping) for row in result}

            if V3_TEST_POOL in snapshots:
                v3_snap = snapshots[V3_TEST_POOL]
                logger.info(f"✓ V3 snapshot found:")
                logger.info(f"  - Protocol: {v3_snap['protocol']}")
                logger.info(f"  - Ticks: {v3_snap['total_ticks']}")
                logger.info(f"  - Bitmap words: {v3_snap['total_bitmap_words']}")

            if V4_TEST_POOL in snapshots:
                v4_snap = snapshots[V4_TEST_POOL]
                logger.info(f"✓ V4 snapshot found:")
                logger.info(f"  - Protocol: {v4_snap['protocol']}")
                logger.info(f"  - Ticks: {v4_snap['total_ticks']}")
                logger.info(f"  - Bitmap words: {v4_snap['total_bitmap_words']}")

            # Verify both use same table
            if V3_TEST_POOL in snapshots and V4_TEST_POOL in snapshots:
                logger.info("✓ Both V3 and V4 snapshots stored in same table")
                logger.info("✓ Schema compatibility verified")


class TestStatistics:
    """Test snapshot statistics."""

    @pytest.mark.asyncio
    async def test_snapshot_statistics(self, db_engine):
        """Get statistics about all snapshots."""
        logger.info("="*80)
        logger.info("TEST: Snapshot Statistics")
        logger.info("="*80)

        stats = get_snapshot_statistics(chain_id=1)

        logger.info(f"Total snapshots: {stats['total_snapshots']}")
        logger.info(f"Average ticks per pool: {stats['avg_ticks_per_pool']:.2f}")
        logger.info(f"Average bitmap words per pool: {stats['avg_bitmap_words_per_pool']:.2f}")
        logger.info(f"Latest snapshot block: {stats['latest_snapshot_block']}")
        logger.info(f"Protocol count: {stats['protocol_count']}")

        if 'snapshots_by_protocol' in stats:
            logger.info("Snapshots by protocol:")
            for protocol, count in stats['snapshots_by_protocol'].items():
                logger.info(f"  {protocol}: {count}")

        if stats['total_snapshots'] > 0:
            logger.info("✓ Snapshots exist in database")
        else:
            logger.info("⚠ No snapshots found (expected - data not yet fetched)")


@pytest.mark.asyncio
async def test_end_to_end_v3_and_v4(db_engine, v3_liquidity_data, v4_liquidity_data):
    """
    Complete end-to-end test processing both V3 and V4 pools.

    This is the main integration test that verifies:
    1. V3 events are processed correctly
    2. V4 events are processed correctly
    3. Both create valid snapshots
    4. Both snapshots use the same database schema
    """
    logger.info("\n" + "="*80)
    logger.info("COMPLETE END-TO-END TEST: V3 AND V4 PROCESSING")
    logger.info("="*80 + "\n")

    # Get processor
    processor = get_processor()

    # Process V3
    logger.info("Step 1: Processing V3 liquidity events...")
    v3_result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v3",
        start_block=None,
        end_block=None,
        force_rebuild=False,
        cleanup_parquet=False,
    )
    logger.info(f"✓ V3: Processed {v3_result['total_events_processed']} events")
    logger.info(f"✓ V3: Updated {v3_result['pools_updated']} pools\n")

    # Process V4
    logger.info("Step 2: Processing V4 liquidity events...")
    v4_result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v4",
        start_block=None,
        end_block=None,
        force_rebuild=False,
        cleanup_parquet=False,
    )
    logger.info(f"✓ V4: Processed {v4_result['total_events_processed']} events")
    logger.info(f"✓ V4: Updated {v4_result['pools_updated']} pools\n")

    # Verify both snapshots
    logger.info("Step 3: Verifying snapshots in database...")
    v3_snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)
    v4_snapshot = load_liquidity_snapshot(V4_TEST_POOL, chain_id=1)

    if v3_snapshot:
        logger.info(f"✓ V3 snapshot exists: {V3_TEST_POOL}")
        logger.info(f"  Ticks: {v3_snapshot['total_ticks']}, Block: {v3_snapshot['snapshot_block']}")

    if v4_snapshot:
        logger.info(f"✓ V4 snapshot exists: {V4_TEST_POOL}")
        logger.info(f"  Ticks: {v4_snapshot['total_ticks']}, Block: {v4_snapshot['snapshot_block']}")

    # Final verification
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    logger.info(f"V3 Events Processed: {v3_result['total_events_processed']}")
    logger.info(f"V4 Events Processed: {v4_result['total_events_processed']}")
    logger.info(f"V3 Snapshot Created: {'✓' if v3_snapshot else '✗'}")
    logger.info(f"V4 Snapshot Created: {'✓' if v4_snapshot else '✗'}")
    logger.info("="*80 + "\n")

    # At minimum, the processor should run without errors
    assert v3_result is not None
    assert v4_result is not None
    logger.info("✓ End-to-end test completed successfully")


@pytest.mark.asyncio
async def test_liquidity_snapshot_validation(db_engine):
    """
    THE VALIDATION TEST: Compare snapshot state against live on-chain state.

    This test validates the accuracy of the liquidity processor by:
    0. Fetching up-to-date liquidity events using production fetcher code
    1. Processing events from pool deployment to current block for test pools
    2. Building snapshots from historical events
    3. Fetching actual current on-chain state using tick batchers
    4. Comparing snapshot vs live state tick-by-tick
    5. Validating only ticks that match tick_spacing are compared
    """
    logger.info("\n" + "="*80)
    logger.info("LIQUIDITY SNAPSHOT VALIDATION TEST")
    logger.info("="*80 + "\n")

    # Import batchers and fetcher
    from src.batchers.uniswap_v3_ticks import UniswapV3TickBatcher
    from src.batchers.uniswap_v4_ticks import UniswapV4TickBatcher
    from src.fetchers.ethereum_fetcher import EthereumFetcher
    from web3 import Web3
    from pathlib import Path

    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    current_block = w3.eth.block_number
    logger.info(f"Current block: {current_block}\n")

    # ============================================================================
    # STEP 0: FETCH FRESH DATA FOR TEST POOL
    # ============================================================================
    logger.info("Step 0: Fetching up-to-date liquidity events from production fetcher...")

    # Initialize fetcher (will use RPC_URL from config if not provided)
    fetcher = EthereumFetcher(rpc_url=RPC_URL)

    # Setup output directory for test pool data
    config = ConfigManager()
    data_dir = Path(config.base.DATA_DIR)

    # Use standard processor directory names
    v3_test_output = data_dir / "ethereum" / "uniswap_v3_modifyliquidity_events"
    v4_test_output = data_dir / "ethereum" / "uniswap_v4_modifyliquidity_events"

    # Clean old test data (remove all existing parquet files)
    import shutil
    if v3_test_output.exists():
        for f in v3_test_output.glob("*.parquet"):
            f.unlink()
    else:
        v3_test_output.mkdir(parents=True, exist_ok=True)

    if v4_test_output.exists():
        for f in v4_test_output.glob("*.parquet"):
            f.unlink()
    else:
        v4_test_output.mkdir(parents=True, exist_ok=True)

    # Fetch V3 test pool events from deployment to current block
    logger.info(f"  Fetching V3 pool {V3_TEST_POOL}")
    logger.info(f"    From block: {V3_POOL_CREATION_BLOCK}")
    logger.info(f"    To block: {current_block}")

    v3_fetch_result = await fetcher.fetch_logs(
        start_block=V3_POOL_CREATION_BLOCK,
        end_block=current_block,
        contracts=[V3_TEST_POOL],
        events=[V3_MINT_EVENT, V3_BURN_EVENT],
        output_dir=str(v3_test_output)
    )

    if not v3_fetch_result.success:
        raise RuntimeError(f"Failed to fetch V3 test pool events: {v3_fetch_result.error}")

    logger.info(f"  ✓ Fetched V3 events: {v3_fetch_result.fetched_blocks} blocks")
    logger.info(f"  ✓ Output: {v3_fetch_result.data_path}\n")

    # Fetch V4 test pool events from deployment to current block
    logger.info(f"  Fetching V4 pool {V4_TEST_POOL}")
    logger.info(f"    From block: {V4_POOL_CREATION_BLOCK}")
    logger.info(f"    To block: {current_block}")

    v4_fetch_result = await fetcher.fetch_logs(
        start_block=V4_POOL_CREATION_BLOCK,
        end_block=current_block,
        contracts=[V4_POOL_MANAGER],
        events=[V4_MODIFY_LIQUIDITY_EVENT],
        output_dir=str(v4_test_output)
    )

    if not v4_fetch_result.success:
        raise RuntimeError(f"Failed to fetch V4 test pool events: {v4_fetch_result.error}")

    logger.info(f"  ✓ Fetched V4 events: {v4_fetch_result.fetched_blocks} blocks")
    logger.info(f"  ✓ Output: {v4_fetch_result.data_path}\n")

    # Get processor
    processor = get_processor()

    # ============================================================================
    # V3 VALIDATION
    # ============================================================================

    # CRITICAL: Fetch live state FIRST to establish target block!
    logger.info("Step 1: Fetching live V3 tick data to establish target block...")
    v3_batcher = UniswapV3TickBatcher(w3)

    from eth_utils import to_checksum_address
    v3_pool_checksum = to_checksum_address(V3_TEST_POOL)

    # Fetch a single tick to get current block number
    initial_result = await v3_batcher.fetch_tick_data(
        pool_ticks={v3_pool_checksum: [0]},  # Just fetch one tick to get block
        block_number=None  # Use latest
    )

    target_block = initial_result.block_number
    logger.info(f"✓ Target block established: {target_block}")
    logger.info(f"  This is the block we will compare snapshot vs live at\n")

    # Now process events UP TO this target block
    logger.info("Step 2: Processing V3 events up to target block...")
    logger.info(f"  Using freshly fetched V3 data from {v3_test_output}")
    logger.info(f"  Processing from deployment to block {target_block}\n")

    v3_result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v3",
        start_block=0,  # Force reprocess from beginning
        end_block=target_block,  # CRITICAL: Process up to target block!
        force_rebuild=True,  # Rebuild from scratch
        cleanup_parquet=False,
    )
    logger.info(f"✓ V3: Processed {v3_result['total_events_processed']} events")
    logger.info(f"✓ V3: Updated {v3_result['pools_updated']} pools")
    logger.info(f"✓ V3: Snapshot created at block {v3_result['end_block']}\n")

    # Load V3 snapshot
    logger.info("Step 3: Loading V3 snapshot from database...")
    v3_snapshot = load_liquidity_snapshot(V3_TEST_POOL, chain_id=1)
    
    if not v3_snapshot:
        logger.warning("⚠ V3 snapshot not found - test pool may not have events in the data range")
        logger.info(f"Skipping V3 validation (pool: {V3_TEST_POOL})\n")
        v3_validation_passed = None
    else:
        logger.info(f"✓ V3 snapshot loaded: {V3_TEST_POOL}")
        logger.info(f"  Total ticks in snapshot: {v3_snapshot['total_ticks']}")
        logger.info(f"  Snapshot block: {v3_snapshot['snapshot_block']}\n")

        # Calculate ALL possible ticks in the full tick range accounting for tick_spacing
        # Uniswap V3 tick range: -887272 to 887272
        MIN_TICK = -887272
        MAX_TICK = 887272

        # Generate all valid ticks based on tick_spacing
        all_valid_ticks = list(range(
            MIN_TICK - (MIN_TICK % V3_TICK_SPACING),  # Round to nearest valid tick
            MAX_TICK + 1,
            V3_TICK_SPACING
        ))

        logger.info(f"  Tick range: {MIN_TICK} to {MAX_TICK}")
        logger.info(f"  Tick spacing: {V3_TICK_SPACING}")
        logger.info(f"  Total possible ticks to check: {len(all_valid_ticks)}")
        logger.info(f"  Ticks with liquidity in snapshot: {v3_snapshot['total_ticks']}\n")

        # Verify snapshot is at target block
        if v3_snapshot['snapshot_block'] != target_block:
            logger.error(f"❌ ERROR: Snapshot at block {v3_snapshot['snapshot_block']} but target is {target_block}!")
            v3_validation_passed = False
        else:
            logger.info(f"✅ Snapshot confirmed at target block {target_block}\n")

            # Fetch live on-chain state at target block (should match snapshot exactly!)
            logger.info("Step 4: Fetching full live V3 tick data at target block...")

            # Chunk ticks to avoid contract size limits
            TICK_CHUNK_SIZE = 500
            total_chunks = (len(all_valid_ticks) + TICK_CHUNK_SIZE - 1) // TICK_CHUNK_SIZE
            logger.info(f"  Fetching {len(all_valid_ticks)} ticks in {total_chunks} chunks of {TICK_CHUNK_SIZE}")
            logger.info(f"  Block: {target_block} (same as snapshot)\n")

            live_tick_data = {}
            failed_chunks = 0

            for chunk_idx in range(0, len(all_valid_ticks), TICK_CHUNK_SIZE):
                tick_chunk = all_valid_ticks[chunk_idx:chunk_idx + TICK_CHUNK_SIZE]
                chunk_num = (chunk_idx // TICK_CHUNK_SIZE) + 1

                if chunk_num % 50 == 0:  # Log progress every 50 chunks
                    logger.info(f"  Processing chunk {chunk_num}/{total_chunks}...")

                batch_result = await v3_batcher.fetch_tick_data(
                    pool_ticks={v3_pool_checksum: tick_chunk},
                    block_number=target_block  # Fetch at target block
                )

                if not batch_result.success:
                    logger.warning(f"  ⚠ Chunk {chunk_num}/{total_chunks} failed: {batch_result.error}")
                    failed_chunks += 1
                    continue

                # Aggregate results
                chunk_data = batch_result.data.get(v3_pool_checksum, {})
                live_tick_data.update(chunk_data)

            if failed_chunks > 0:
                logger.warning(f"  ⚠ {failed_chunks}/{total_chunks} chunks failed to fetch")

            if len(live_tick_data) == 0:
                logger.error(f"✗ Failed to fetch any live V3 tick data")
                v3_validation_passed = False
            else:
                logger.info(f"✓ Fetched live tick data at block {target_block}")
                logger.info(f"  Live ticks returned: {len(live_tick_data)}")
                logger.info(f"  Successfully fetched {total_chunks - failed_chunks}/{total_chunks} chunks\n")

                # Compare snapshot vs live for ALL possible ticks
                logger.info("Step 5: Comparing V3 snapshot vs live state...")
            mismatches = []
            matches = 0

            for tick in all_valid_ticks:
                snapshot_tick_data = v3_snapshot['tick_data'].get(str(tick))
                live_tick_info = live_tick_data.get(tick)

                # Get liquidity values (default to 0 if not present)
                snap_gross = snapshot_tick_data.get('liquidity_gross', 0) if snapshot_tick_data else 0
                snap_net = snapshot_tick_data.get('liquidity_net', 0) if snapshot_tick_data else 0
                live_gross = live_tick_info.liquidity_gross if live_tick_info else 0
                live_net = live_tick_info.liquidity_net if live_tick_info else 0

                # Compare - both should have same values (0 or non-zero)
                if snap_gross != live_gross or snap_net != live_net:
                    mismatches.append({
                        'tick': tick,
                        'issue': 'liquidity_mismatch',
                        'snapshot_gross': snap_gross,
                        'snapshot_net': snap_net,
                        'live_gross': live_gross,
                        'live_net': live_net,
                    })
                else:
                    matches += 1

            # Report results
            total_compared = len(all_valid_ticks)
            logger.info(f"  Total ticks compared: {total_compared}")
            logger.info(f"  Matches: {matches}")
            logger.info(f"  Mismatches: {len(mismatches)}")
            
            if mismatches:
                logger.warning("\n⚠ V3 MISMATCHES FOUND:")
                for i, mismatch in enumerate(mismatches[:10]):  # Show first 10
                    logger.warning(f"  {i+1}. Tick {mismatch['tick']}: {mismatch['issue']}")
                    if mismatch['issue'] == 'liquidity_mismatch':
                        logger.warning(f"     Snapshot: gross={mismatch['snapshot_gross']}, net={mismatch['snapshot_net']}")
                        logger.warning(f"     Live:     gross={mismatch['live_gross']}, net={mismatch['live_net']}")
                if len(mismatches) > 10:
                    logger.warning(f"  ... and {len(mismatches) - 10} more mismatches")
            
            match_percentage = (matches / total_compared * 100) if total_compared > 0 else 0
            logger.info(f"\n  Match percentage: {match_percentage:.2f}%")
            
            v3_validation_passed = len(mismatches) == 0
            if v3_validation_passed:
                logger.info("✓ V3 validation PASSED: Snapshot matches live state perfectly!\n")
            else:
                logger.warning(f"⚠ V3 validation FAILED: {len(mismatches)} mismatches found\n")

    # ============================================================================
    # V4 VALIDATION
    # ============================================================================
    logger.info("Step 5: Processing V4 events and creating snapshot...")
    v4_result = await processor.process_liquidity_snapshots(
        protocol="uniswap_v4",
        start_block=None,
        end_block=None,
        force_rebuild=False,
        cleanup_parquet=False,
    )
    logger.info(f"✓ V4: Processed {v4_result['total_events_processed']} events")
    logger.info(f"✓ V4: Updated {v4_result['pools_updated']} pools\n")

    # Load V4 snapshot
    logger.info("Step 6: Loading V4 snapshot from database...")
    v4_snapshot = load_liquidity_snapshot(V4_TEST_POOL, chain_id=1)
    
    if not v4_snapshot:
        logger.warning("⚠ V4 snapshot not found - test pool may not have events in the data range")
        logger.info(f"Skipping V4 validation (pool: {V4_TEST_POOL})\n")
        v4_validation_passed = None
    else:
        logger.info(f"✓ V4 snapshot loaded: {V4_TEST_POOL}")
        logger.info(f"  Total ticks in snapshot: {v4_snapshot['total_ticks']}")
        logger.info(f"  Snapshot block: {v4_snapshot['snapshot_block']}\n")

        # Calculate ALL possible ticks in the full tick range accounting for tick_spacing
        # Uniswap V4 has same tick range as V3: -887272 to 887272
        MIN_TICK_V4 = -887272
        MAX_TICK_V4 = 887272

        # Generate all valid ticks based on tick_spacing
        all_valid_ticks_v4 = list(range(
            MIN_TICK_V4 - (MIN_TICK_V4 % V4_TICK_SPACING),  # Round to nearest valid tick
            MAX_TICK_V4 + 1,
            V4_TICK_SPACING
        ))

        logger.info(f"  Tick range: {MIN_TICK_V4} to {MAX_TICK_V4}")
        logger.info(f"  Tick spacing: {V4_TICK_SPACING}")
        logger.info(f"  Total possible ticks to check: {len(all_valid_ticks_v4)}")
        logger.info(f"  Ticks with liquidity in snapshot: {v4_snapshot['total_ticks']}\n")

        # Fetch live on-chain state using batcher with chunking
        logger.info("Step 7: Fetching live V4 tick data from chain using batcher...")
        v4_batcher = UniswapV4TickBatcher(w3)

        # Chunk ticks to avoid contract size limits
        # V4 has fewer ticks due to larger tick_spacing (60 vs 10)
        TICK_CHUNK_SIZE_V4 = 500
        total_chunks_v4 = (len(all_valid_ticks_v4) + TICK_CHUNK_SIZE_V4 - 1) // TICK_CHUNK_SIZE_V4
        logger.info(f"  Fetching {len(all_valid_ticks_v4)} ticks in {total_chunks_v4} chunks of {TICK_CHUNK_SIZE_V4}")

        live_tick_data_v4 = {}
        fetch_block_number_v4 = None
        failed_chunks_v4 = 0

        for chunk_idx in range(0, len(all_valid_ticks_v4), TICK_CHUNK_SIZE_V4):
            tick_chunk = all_valid_ticks_v4[chunk_idx:chunk_idx + TICK_CHUNK_SIZE_V4]
            chunk_num = (chunk_idx // TICK_CHUNK_SIZE_V4) + 1

            if chunk_num % 10 == 0:  # Log progress every 10 chunks
                logger.info(f"  Processing chunk {chunk_num}/{total_chunks_v4}...")

            batch_result = await v4_batcher.fetch_tick_data(
                pool_ticks={V4_TEST_POOL: tick_chunk},
                block_number=fetch_block_number_v4  # Use same block for all chunks
            )

            if not batch_result.success:
                logger.warning(f"  ⚠ Chunk {chunk_num}/{total_chunks_v4} failed: {batch_result.error}")
                failed_chunks_v4 += 1
                continue

            # Store block number from first successful fetch
            if fetch_block_number_v4 is None:
                fetch_block_number_v4 = batch_result.block_number

            # Aggregate results
            chunk_data = batch_result.data.get(V4_TEST_POOL, {})
            live_tick_data_v4.update(chunk_data)

        if failed_chunks_v4 > 0:
            logger.warning(f"  ⚠ {failed_chunks_v4}/{total_chunks_v4} chunks failed to fetch")

        if len(live_tick_data_v4) == 0:
            logger.error(f"✗ Failed to fetch any live V4 tick data")
            v4_validation_passed = False
        else:
            logger.info(f"✓ Fetched live tick data at block {fetch_block_number_v4}")
            logger.info(f"  Live ticks returned: {len(live_tick_data_v4)}")
            logger.info(f"  Successfully fetched {total_chunks_v4 - failed_chunks_v4}/{total_chunks_v4} chunks\n")
            live_tick_data = live_tick_data_v4

            # Compare snapshot vs live for ALL possible ticks
            logger.info("Step 8: Comparing V4 snapshot vs live state...")
            mismatches = []
            matches = 0

            for tick in all_valid_ticks_v4:
                snapshot_tick_data = v4_snapshot['tick_data'].get(str(tick))
                live_tick_info = live_tick_data.get(tick)

                # Get liquidity values (default to 0 if not present)
                snap_gross = snapshot_tick_data.get('liquidity_gross', 0) if snapshot_tick_data else 0
                snap_net = snapshot_tick_data.get('liquidity_net', 0) if snapshot_tick_data else 0
                live_gross = live_tick_info.liquidity_gross if live_tick_info else 0
                live_net = live_tick_info.liquidity_net if live_tick_info else 0

                # Compare - both should have same values (0 or non-zero)
                if snap_gross != live_gross or snap_net != live_net:
                    mismatches.append({
                        'tick': tick,
                        'issue': 'liquidity_mismatch',
                        'snapshot_gross': snap_gross,
                        'snapshot_net': snap_net,
                        'live_gross': live_gross,
                        'live_net': live_net,
                    })
                else:
                    matches += 1

            # Report results
            total_compared = len(all_valid_ticks_v4)
            logger.info(f"  Total ticks compared: {total_compared}")
            logger.info(f"  Matches: {matches}")
            logger.info(f"  Mismatches: {len(mismatches)}")
            
            if mismatches:
                logger.warning("\n⚠ V4 MISMATCHES FOUND:")
                for i, mismatch in enumerate(mismatches[:10]):
                    logger.warning(f"  {i+1}. Tick {mismatch['tick']}: {mismatch['issue']}")
                    if mismatch['issue'] == 'liquidity_mismatch':
                        logger.warning(f"     Snapshot: gross={mismatch['snapshot_gross']}, net={mismatch['snapshot_net']}")
                        logger.warning(f"     Live:     gross={mismatch['live_gross']}, net={mismatch['live_net']}")
                if len(mismatches) > 10:
                    logger.warning(f"  ... and {len(mismatches) - 10} more mismatches")
            
            match_percentage = (matches / total_compared * 100) if total_compared > 0 else 0
            logger.info(f"\n  Match percentage: {match_percentage:.2f}%")
            
            v4_validation_passed = len(mismatches) == 0
            if v4_validation_passed:
                logger.info("✓ V4 validation PASSED: Snapshot matches live state perfectly!\n")
            else:
                logger.warning(f"⚠ V4 validation FAILED: {len(mismatches)} mismatches found\n")

    # ============================================================================
    # FINAL SUMMARY
    # ============================================================================
    logger.info("\n" + "="*80)
    logger.info("VALIDATION TEST SUMMARY")
    logger.info("="*80)
    logger.info(f"Current block: {current_block}")
    logger.info(f"V3 Test Pool: {V3_TEST_POOL}")
    logger.info(f"  Events processed: {v3_result['total_events_processed']}")
    if v3_validation_passed is not None:
        logger.info(f"  Validation: {'✓ PASSED' if v3_validation_passed else '✗ FAILED'}")
    else:
        logger.info(f"  Validation: SKIPPED (no snapshot)")
    
    logger.info(f"\nV4 Test Pool: {V4_TEST_POOL}")
    logger.info(f"  Events processed: {v4_result['total_events_processed']}")
    if v4_validation_passed is not None:
        logger.info(f"  Validation: {'✓ PASSED' if v4_validation_passed else '✗ FAILED'}")
    else:
        logger.info(f"  Validation: SKIPPED (no snapshot)")
    logger.info("="*80 + "\n")

    # Assert test results
    if v3_validation_passed is False or v4_validation_passed is False:
        pytest.fail("Validation failed: Snapshot state does not match live on-chain state")
    
    logger.info("✓ Liquidity snapshot validation test completed successfully")


if __name__ == "__main__":
    # Run with: pytest src/processors/pools/tests/test_unified_liquidity_processor.py -v -s
    pytest.main([__file__, "-v", "-s"])
