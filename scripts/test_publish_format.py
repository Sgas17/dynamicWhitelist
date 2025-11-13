#!/usr/bin/env python3
"""
Test script to publish a sample pool and verify the format.
Uses actual config from the project.
"""
import asyncio
import json
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_config
from src.core.whitelist_manager import WhitelistManager


async def test_publish_format():
    """Test publishing a sample pool in the correct format."""
    config = get_config()

    # Get database config from environment
    db_config = {
        "host": config.database.POSTGRES_HOST,
        "port": config.database.POSTGRES_PORT,
        "user": config.database.POSTGRES_USER,
        "password": config.database.POSTGRES_PASSWORD,
        "database": config.database.POSTGRES_DB,
    }

    print(f"📊 Using database: {db_config['host']}:{db_config['port']}/{db_config['database']}")

    # Sample pools in dynamicWhitelist format
    sample_pools = [
        # V3 pool
        {
            "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "token0": {
                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "decimals": 6,
                "symbol": "USDC",
            },
            "token1": {
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "decimals": 18,
                "symbol": "WETH",
            },
            "protocol": "v3",
            "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
            "fee": 500,
            "tick_spacing": 10,
        },
        # V4 pool
        {
            "address": "0x000000000004444c5dc75cb358380d2e3de08a90",  # Pool manager
            "pool_id": "0xfe0750fd50bc21d9833fdcdd97992fdb60ecbb3040c833250a56aa3b7ede0aa0",
            "token0": {
                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "decimals": 6,
                "symbol": "USDC",
            },
            "token1": {
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "decimals": 18,
                "symbol": "WETH",
            },
            "protocol": "v4",
            "factory": "0x000000000004444c5dc75cb358380d2e3de08a90",
            "fee": 800000,
            "tick_spacing": 60,
        },
    ]

    print(f"\n📦 Sample pools to publish: {len(sample_pools)}")

    try:
        async with WhitelistManager(db_config) as wl:
            # Test transformation
            print("\n🔄 Testing transformation:")
            for i, pool in enumerate(sample_pools):
                transformed = wl._transform_pool_for_arena(pool)
                print(f"\n  Pool {i+1} ({pool['protocol']}):")
                print(f"    Input:  {json.dumps(pool, indent=6)[:200]}...")
                print(f"    Output: {json.dumps(transformed, indent=6)}")

            # Publish with force_full to trigger message
            print("\n📤 Publishing to NATS...")
            result = await wl.publish_differential_update(
                chain="ethereum",
                new_pools=sample_pools,
                force_full=True
            )

            print(f"\n✅ Published successfully!")
            print(f"   Snapshot ID: {result['snapshot_id']}")
            print(f"   Total pools: {result['total_pools']}")
            print(f"   Update type: {result['update_type']}")

            return 0

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(test_publish_format()))
