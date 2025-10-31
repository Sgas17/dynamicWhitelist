"""
Test script for liquidity storage layers.

This script tests both PostgreSQL (snapshots) and TimescaleDB (updates) storage.
"""

import sys
import os
import logging
from datetime import datetime, UTC

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))

from src.core.storage.timescaledb import get_timescale_engine
from src.core.storage.postgres_liquidity import (
    setup_liquidity_snapshots_table,
    store_liquidity_snapshot,
    load_liquidity_snapshot,
    get_snapshot_statistics,
)
from src.core.storage.timescaledb_liquidity import (
    setup_liquidity_updates_table,
    store_liquidity_update,
    store_liquidity_updates_batch,
    get_updates_since_block,
    get_update_statistics,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_snapshot_storage():
    """Test PostgreSQL snapshot storage."""
    logger.info("=" * 60)
    logger.info("Testing PostgreSQL Snapshot Storage")
    logger.info("=" * 60)

    engine = get_timescale_engine()
    chain_id = 1

    # Setup table
    logger.info("1. Setting up liquidity_snapshots table...")
    success = setup_liquidity_snapshots_table(engine, chain_id)
    assert success, "Table setup failed"
    logger.info("✓ Table setup successful")

    # Create test snapshot
    test_pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    test_snapshot = {
        "snapshot_block": 12369621,
        "snapshot_timestamp": datetime.now(UTC),
        "tick_bitmap": {
            -50: {"bitmap": 18446744073709551615, "block_number": 12369621},
            0: {"bitmap": 255, "block_number": 12369621},
        },
        "tick_data": {
            -887220: {
                "liquidityNet": 1234567890,
                "liquidityGross": 1234567890,
                "block_number": 12369621
            },
            -887210: {
                "liquidityNet": -9876543210,
                "liquidityGross": 9876543210,
                "block_number": 12369625
            },
        },
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "asset0": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "asset1": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "fee": 500,
        "tick_spacing": 10,
        "protocol": "uniswap_v3",
    }

    # Store snapshot
    logger.info("2. Storing test snapshot...")
    success = store_liquidity_snapshot(test_pool_address, test_snapshot, chain_id)
    assert success, "Snapshot storage failed"
    logger.info("✓ Snapshot stored successfully")

    # Load snapshot
    logger.info("3. Loading snapshot...")
    loaded_snapshot = load_liquidity_snapshot(test_pool_address, chain_id)
    assert loaded_snapshot is not None, "Snapshot load failed"
    logger.info("✓ Snapshot loaded successfully")
    logger.info(f"  Pool: {loaded_snapshot['pool_address']}")
    logger.info(f"  Block: {loaded_snapshot['snapshot_block']}")
    logger.info(f"  Total ticks: {loaded_snapshot['total_ticks']}")
    logger.info(f"  Total bitmap words: {loaded_snapshot['total_bitmap_words']}")
    logger.info(f"  Protocol: {loaded_snapshot['protocol']}")

    # Verify tick_data structure
    logger.info("4. Verifying tick_data structure...")
    for tick, data in loaded_snapshot["tick_data"].items():
        assert set(data.keys()) == {"liquidityNet", "liquidityGross", "block_number"}, \
            f"Invalid tick_data structure for tick {tick}"
    logger.info("✓ Tick_data structure validated (3 fields only)")

    # Get statistics
    logger.info("5. Getting snapshot statistics...")
    stats = get_snapshot_statistics(chain_id)
    logger.info("✓ Snapshot statistics:")
    logger.info(f"  Total snapshots: {stats['total_snapshots']}")
    logger.info(f"  Latest block: {stats['latest_snapshot_block']}")
    logger.info(f"  Avg ticks per pool: {stats['avg_ticks_per_pool']:.2f}")

    logger.info("\n✓ All PostgreSQL snapshot tests passed!\n")


def test_update_storage():
    """Test TimescaleDB update storage."""
    logger.info("=" * 60)
    logger.info("Testing TimescaleDB Update Storage")
    logger.info("=" * 60)

    engine = get_timescale_engine()
    chain_id = 1

    # Setup table
    logger.info("1. Setting up liquidity_updates hypertable...")
    success = setup_liquidity_updates_table(engine, chain_id)
    assert success, "Hypertable setup failed"
    logger.info("✓ Hypertable setup successful")

    # Create test update
    test_pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    test_update = {
        "event_time": datetime.now(UTC),
        "block_number": 12369622,
        "transaction_index": 5,
        "log_index": 12,
        "pool_address": test_pool_address,
        "event_type": "Mint",
        "tick_lower": -887220,
        "tick_upper": -887210,
        "liquidity_delta": 1000000000,
        "transaction_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "sender_address": "0x1234567890123456789012345678901234567890",
        "amount0": 1000000,
        "amount1": 5000000,
    }

    # Store single update
    logger.info("2. Storing single test update...")
    success = store_liquidity_update(test_update, chain_id)
    assert success, "Update storage failed"
    logger.info("✓ Update stored successfully")

    # Create batch of test updates
    logger.info("3. Storing batch of test updates...")
    batch_updates = []
    for i in range(10):
        update = {
            "event_time": datetime.now(UTC),
            "block_number": 12369623 + i,
            "transaction_index": i,
            "log_index": i * 2,
            "pool_address": test_pool_address,
            "event_type": "Mint" if i % 2 == 0 else "Burn",
            "tick_lower": -887220,
            "tick_upper": -887210,
            "liquidity_delta": 1000000 * (1 if i % 2 == 0 else -1),
            "transaction_hash": f"0x{'0' * 63}{i}",
            "sender_address": "0x1234567890123456789012345678901234567890",
            "amount0": 1000 * i,
            "amount1": 5000 * i,
        }
        batch_updates.append(update)

    stored_count = store_liquidity_updates_batch(batch_updates, chain_id)
    assert stored_count == len(batch_updates), \
        f"Batch storage incomplete ({stored_count}/{len(batch_updates)})"
    logger.info(f"✓ Batch stored successfully ({stored_count} updates)")

    # Query updates
    logger.info("4. Querying updates since block...")
    updates = get_updates_since_block(test_pool_address, after_block=12369621, chain_id=chain_id)
    assert updates, "No updates retrieved"
    logger.info(f"✓ Retrieved {len(updates)} updates")
    logger.info(f"  First update: Block {updates[0]['block_number']}, Type: {updates[0]['event_type']}")
    logger.info(f"  Last update: Block {updates[-1]['block_number']}, Type: {updates[-1]['event_type']}")

    # Get statistics
    logger.info("5. Getting update statistics...")
    stats = get_update_statistics(chain_id)
    logger.info("✓ Update statistics:")
    logger.info(f"  Total updates: {stats['total_updates']}")
    logger.info(f"  Unique pools: {stats['unique_pools']}")
    logger.info(f"  Mint count: {stats['mint_count']}")
    logger.info(f"  Burn count: {stats['burn_count']}")
    logger.info(f"  Latest block: {stats['latest_block']}")

    logger.info("\n✓ All TimescaleDB update tests passed!\n")


def main():
    """Run all tests."""
    logger.info("\n" + "=" * 60)
    logger.info("LIQUIDITY STORAGE LAYER TESTS")
    logger.info("=" * 60 + "\n")

    try:
        # Test snapshot storage
        snapshot_success = test_snapshot_storage()

        # Test update storage
        update_success = test_update_storage()

        # Summary
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"PostgreSQL Snapshots: {'✓ PASS' if snapshot_success else '✗ FAIL'}")
        logger.info(f"TimescaleDB Updates: {'✓ PASS' if update_success else '✗ FAIL'}")

        if snapshot_success and update_success:
            logger.info("\n✓ ALL TESTS PASSED!")
            return 0
        else:
            logger.error("\n✗ SOME TESTS FAILED")
            return 1

    except Exception as e:
        logger.error(f"Test execution failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
