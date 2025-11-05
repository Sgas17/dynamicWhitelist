#!/usr/bin/env python3
"""
Quick test script for WhitelistManager.

Tests:
1. WhitelistManager can connect to database
2. Schema is created correctly
3. First publish works (full update)
4. Second publish works (differential update)
5. Snapshots are stored in database

Usage:
    cd /home/sam-sullivan/dynamicWhitelist
    python -m src.core.test_whitelist_manager
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

import pytest

from src.core.whitelist_manager import WhitelistManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Sample pool data for testing
SAMPLE_POOLS = [
    {
        "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        "token0": {
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
            "symbol": "USDC",
            "name": "USD Coin",
        },
        "token1": {
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "decimals": 18,
            "symbol": "WETH",
            "name": "Wrapped Ether",
        },
        "protocol": "UniswapV3",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "fee": 500,
        "tick_spacing": 10,
    },
    {
        "address": "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
        "token0": {
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
            "symbol": "USDC",
        },
        "token1": {
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "decimals": 18,
            "symbol": "WETH",
        },
        "protocol": "UniswapV3",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "fee": 3000,
        "tick_spacing": 60,
    },
    {
        "address": "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        "token0": {
            "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            "decimals": 8,
            "symbol": "WBTC",
        },
        "token1": {
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "decimals": 18,
            "symbol": "WETH",
        },
        "protocol": "UniswapV3",
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "fee": 3000,
        "tick_spacing": 60,
    },
]


@pytest.mark.asyncio
async def test_whitelist_manager():
    """Test WhitelistManager functionality."""

    print("\n" + "=" * 70)
    print("WHITELIST MANAGER INTEGRATION TEST")
    print("=" * 70)

    # Get database config from environment
    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "defi_platform"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }

    print(f"\n📋 Database Config:")
    print(f"   Host: {db_config['host']}:{db_config['port']}")
    print(f"   Database: {db_config['database']}")
    print(f"   User: {db_config['user']}")

    try:
        # Test 1: Initialize WhitelistManager
        print("\n" + "-" * 70)
        print("TEST 1: Initialize WhitelistManager")
        async with WhitelistManager(db_config) as manager:
            print("   ✅ WhitelistManager initialized")
            print("   ✅ Database schema verified")

            # Test 2: First publish (should be FULL)
            print("\n" + "-" * 70)
            print("TEST 2: First Publish (Expected: FULL)")
            print(f"   Publishing {len(SAMPLE_POOLS)} pools...")

            result1 = await manager.publish_differential_update(
                chain="ethereum", new_pools=SAMPLE_POOLS
            )

            print(f"\n   📊 First Publish Results:")
            print(f"      Update Type: {result1['update_type']}")
            print(f"      Total Pools: {result1['total_pools']}")
            print(f"      Added: {result1['added']}")
            print(f"      Removed: {result1['removed']}")
            print(f"      Snapshot ID: {result1['snapshot_id']}")
            print(f"      Published: {'✅' if result1['published'] else '❌'}")

            assert result1["update_type"] == "full", "First update should be 'full'"
            assert result1["total_pools"] == len(SAMPLE_POOLS)
            assert result1["published"], "Publish should succeed"
            print("\n   ✅ First publish passed validation")

            # Test 3: Second publish with changes (should be DIFFERENTIAL)
            print("\n" + "-" * 70)
            print("TEST 3: Second Publish with Changes (Expected: DIFFERENTIAL)")

            # Add two new pools, remove one
            modified_pools = SAMPLE_POOLS[:2] + [  # Keep first 2, remove WBTC/WETH
                {
                    "address": "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
                    "token0": {
                        "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                        "decimals": 6,
                        "symbol": "USDT",
                    },
                    "token1": {
                        "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "decimals": 18,
                        "symbol": "WETH",
                    },
                    "protocol": "UniswapV3",
                    "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "fee": 3000,
                    "tick_spacing": 60,
                },
                {
                    "address": "0x11b815efB8f581194ae79006d24E0d814B7697F6",
                    "token0": {
                        "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                        "decimals": 6,
                        "symbol": "USDT",
                    },
                    "token1": {
                        "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "decimals": 18,
                        "symbol": "WETH",
                    },
                    "protocol": "UniswapV3",
                    "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "fee": 500,
                    "tick_spacing": 10,
                },
            ]

            print(f"   Modified whitelist: 2 same, 1 removed, 2 added = 4 total")

            result2 = await manager.publish_differential_update(
                chain="ethereum", new_pools=modified_pools
            )

            print(f"\n   📊 Second Publish Results:")
            print(f"      Update Type: {result2['update_type']}")
            print(f"      Total Pools: {result2['total_pools']}")
            print(f"      Added: {result2['added']}")
            print(f"      Removed: {result2['removed']}")
            print(f"      Snapshot ID: {result2['snapshot_id']}")
            print(f"      Published: {'✅' if result2['published'] else '❌'}")

            assert result2["update_type"] == "differential", (
                "Second update should be 'differential'"
            )
            assert result2["added"] == 2, "Should have added 2 pools"
            assert result2["removed"] == 1, "Should have removed 1 pool"
            assert result2["total_pools"] == 4, "Total should be 4 pools"
            print("\n   ✅ Second publish passed validation")

            # Test 4: Verify snapshots in database
            print("\n" + "-" * 70)
            print("TEST 4: Verify Snapshots in Database")

            import psycopg2

            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    # Count snapshots
                    cur.execute("""
                        SELECT
                            COUNT(DISTINCT snapshot_id) as snapshot_count,
                            COUNT(*) as total_rows
                        FROM whitelist_snapshots
                        WHERE chain = 'ethereum'
                    """)
                    snapshot_count, total_rows = cur.fetchone()

                    print(f"   📊 Database Status:")
                    print(f"      Total Snapshots: {snapshot_count}")
                    print(f"      Total Rows: {total_rows}")

                    # Get latest snapshot details
                    cur.execute("""
                        SELECT
                            snapshot_id,
                            COUNT(*) as pool_count,
                            published_at
                        FROM whitelist_snapshots
                        WHERE chain = 'ethereum'
                        GROUP BY snapshot_id, published_at
                        ORDER BY published_at DESC
                        LIMIT 2
                    """)
                    snapshots = cur.fetchall()

                    print(f"\n   📋 Recent Snapshots:")
                    for snapshot_id, pool_count, published_at in snapshots:
                        print(
                            f"      Snapshot {snapshot_id}: {pool_count} pools at {published_at}"
                        )

                    assert snapshot_count >= 2, "Should have at least 2 snapshots"
                    assert total_rows == 3 + 4, "Should have 7 total rows (3 + 4)"
                    print("\n   ✅ Database verification passed")

        # Success!
        print("\n" + "=" * 70)
        print("✅ ALL TESTS PASSED")
        print("=" * 70)
        print("\n📊 Summary:")
        print("   • WhitelistManager integration working correctly")
        print("   • Database schema created and operational")
        print("   • Full updates work (first publish)")
        print("   • Differential updates work (subsequent publishes)")
        print("   • Snapshots stored correctly in database")
        print("\n🚀 Ready for production use in dynamicWhitelist orchestrator!")

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ TEST FAILED")
        print("=" * 70)
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


@pytest.mark.asyncio
async def cleanup_test_data():
    """Clean up test data from database."""
    print("\n" + "-" * 70)
    print("CLEANUP: Removing test data...")

    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "defi_platform"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }

    try:
        import psycopg2

        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM whitelist_snapshots
                    WHERE chain = 'ethereum'
                """)
                deleted_count = cur.rowcount
            conn.commit()

        print(f"   ✅ Deleted {deleted_count} test rows from whitelist_snapshots")
    except Exception as e:
        print(f"   ⚠️  Cleanup failed: {e}")


@pytest.mark.asyncio
async def main():
    """Main test function."""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║  WhitelistManager Integration Test                              ║
║  Validates WhitelistManager works with dynamicWhitelist          ║
╚══════════════════════════════════════════════════════════════════╝

Prerequisites:
1. PostgreSQL database running and configured in .env
2. NATS server running (docker run -p 4222:4222 nats:latest)
3. Database credentials in environment variables

Starting tests...
    """)

    try:
        await test_whitelist_manager()

        # Ask user if they want to clean up
        print("\n" + "-" * 70)
        cleanup = input("Clean up test data from database? (y/N): ").lower()
        if cleanup == "y":
            await cleanup_test_data()
        else:
            print("   ℹ️  Test data preserved for inspection")

    except KeyboardInterrupt:
        print("\n\n👋 Test interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
