"""
Filter Uniswap V2/V3/V4 pools from database to find those with trusted tokens.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage


async def get_v2_pools_with_trusted_tokens(
    storage: PostgresStorage,
    trusted_tokens: Dict[str, str]
) -> List[Dict]:
    """
    Query V2 pools from database and filter by trusted tokens.

    Args:
        storage: Database storage instance
        trusted_tokens: Dict mapping token name to address

    Returns:
        List of V2 pools containing at least one trusted token
    """
    # Normalize trusted addresses to lowercase
    trusted_addresses = tuple(addr.lower() for addr in trusted_tokens.values())

    query = """
    SELECT
        address as pool_address,
        asset0 as token0,
        asset1 as token1,
        factory,
        creation_block as block_number
    FROM network_1__dex_pools
    WHERE protocol_name = 'uniswap_v2'
    AND (LOWER(asset0) = ANY($1) OR LOWER(asset1) = ANY($1))
    ORDER BY creation_block DESC
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, trusted_addresses)

    # Add token names for clarity
    token_name_map = {addr.lower(): name for name, addr in trusted_tokens.items()}

    pools = []
    for row in results:
        pool_dict = dict(row)

        # Add which tokens are trusted
        trusted_in_pool = []
        if pool_dict['token0'].lower() in token_name_map:
            trusted_in_pool.append(f"token0={token_name_map[pool_dict['token0'].lower()]}")
        if pool_dict['token1'].lower() in token_name_map:
            trusted_in_pool.append(f"token1={token_name_map[pool_dict['token1'].lower()]}")

        pool_dict['trusted_tokens'] = ", ".join(trusted_in_pool)
        pools.append(pool_dict)

    return pools


async def get_v3_pools_with_trusted_tokens(
    storage: PostgresStorage,
    trusted_tokens: Dict[str, str]
) -> List[Dict]:
    """
    Query V3 pools from database and filter by trusted tokens.

    Args:
        storage: Database storage instance
        trusted_tokens: Dict mapping token name to address

    Returns:
        List of pools containing at least one trusted token
    """
    # Normalize trusted addresses to lowercase
    trusted_addresses = tuple(addr.lower() for addr in trusted_tokens.values())

    query = """
    SELECT
        address as pool_address,
        asset0 as token0,
        asset1 as token1,
        fee,
        tick_spacing,
        factory,
        creation_block as block_number
    FROM network_1__dex_pools
    WHERE protocol_name = 'uniswap_v3'
    AND (LOWER(asset0) = ANY($1) OR LOWER(asset1) = ANY($1))
    ORDER BY creation_block DESC
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, trusted_addresses)

    # Add token names for clarity
    token_name_map = {addr.lower(): name for name, addr in trusted_tokens.items()}

    pools = []
    for row in results:
        pool_dict = dict(row)

        # Add which tokens are trusted
        trusted_in_pool = []
        if pool_dict['token0'].lower() in token_name_map:
            trusted_in_pool.append(f"token0={token_name_map[pool_dict['token0'].lower()]}")
        if pool_dict['token1'].lower() in token_name_map:
            trusted_in_pool.append(f"token1={token_name_map[pool_dict['token1'].lower()]}")

        pool_dict['trusted_tokens'] = ", ".join(trusted_in_pool)
        pools.append(pool_dict)

    return pools


async def get_v4_pools_with_trusted_tokens(
    storage: PostgresStorage,
    trusted_tokens: Dict[str, str]
) -> List[Dict]:
    """
    Query V4 pools from database and filter by trusted tokens.

    Args:
        storage: Database storage instance
        trusted_tokens: Dict mapping token name to address

    Returns:
        List of pools containing at least one trusted token
    """
    # Normalize trusted addresses to lowercase
    trusted_addresses = tuple(addr.lower() for addr in trusted_tokens.values())

    query = """
    SELECT
        address as pool_id,
        asset0 as currency0,
        asset1 as currency1,
        fee,
        tick_spacing,
        factory,
        creation_block as block_number
    FROM network_1__dex_pools
    WHERE protocol_name = 'uniswap_v4'
    AND (LOWER(asset0) = ANY($1) OR LOWER(asset1) = ANY($1))
    ORDER BY creation_block DESC
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, trusted_addresses)

    # Add token names for clarity
    token_name_map = {addr.lower(): name for name, addr in trusted_tokens.items()}

    pools = []
    for row in results:
        pool_dict = dict(row)

        # Add which tokens are trusted
        trusted_in_pool = []
        if pool_dict['currency0'].lower() in token_name_map:
            trusted_in_pool.append(f"currency0={token_name_map[pool_dict['currency0'].lower()]}")
        if pool_dict['currency1'].lower() in token_name_map:
            trusted_in_pool.append(f"currency1={token_name_map[pool_dict['currency1'].lower()]}")

        pool_dict['trusted_tokens'] = ", ".join(trusted_in_pool)
        pools.append(pool_dict)

    return pools


async def main():
    """Main function to query and filter pools from database."""

    # Initialize config and storage
    config_manager = ConfigManager()
    chain_config = config_manager.chains

    # Get trusted tokens for ethereum
    chain_name = "ethereum"
    trusted_tokens = chain_config.get_trusted_tokens_for_chain()[chain_name]

    print(f"🔍 Filtering pools with trusted tokens:")
    for name, addr in trusted_tokens.items():
        print(f"   {name}: {addr}")

    # Initialize database storage
    db_config = {
        'host': config_manager.database.POSTGRES_HOST,
        'port': config_manager.database.POSTGRES_PORT,
        'user': config_manager.database.POSTGRES_USER,
        'password': config_manager.database.POSTGRES_PASSWORD,
        'database': config_manager.database.POSTGRES_DB,
        'pool_size': 10,
        'pool_timeout': 10
    }
    storage = PostgresStorage(config=db_config)
    await storage.connect()

    try:
        # Query V2 pools
        print(f"\n📊 Querying Uniswap V2 pools from database...")
        v2_pools = await get_v2_pools_with_trusted_tokens(storage, trusted_tokens)

        print(f"   Found {len(v2_pools)} V2 pools with trusted tokens")

        if v2_pools:
            print(f"\n📋 Sample V2 pools:")
            for i, pool in enumerate(v2_pools[:5]):
                print(f"   {i+1}. Pool: {pool['pool_address'][:10]}...")
                print(f"      {pool['trusted_tokens']}")

        # Query V3 pools
        print(f"\n📊 Querying Uniswap V3 pools from database...")
        v3_pools = await get_v3_pools_with_trusted_tokens(storage, trusted_tokens)

        print(f"   Found {len(v3_pools)} V3 pools with trusted tokens")

        if v3_pools:
            print(f"\n📋 Sample V3 pools:")
            for i, pool in enumerate(v3_pools[:5]):
                print(f"   {i+1}. Pool: {pool['pool_address'][:10]}...")
                print(f"      {pool['trusted_tokens']}")
                print(f"      Fee: {pool['fee']} bps")

        # Query V4 pools
        print(f"\n📊 Querying Uniswap V4 pools from database...")
        v4_pools = await get_v4_pools_with_trusted_tokens(storage, trusted_tokens)

        print(f"   Found {len(v4_pools)} V4 pools with trusted tokens")

        if v4_pools:
            print(f"\n📋 Sample V4 pools:")
            for i, pool in enumerate(v4_pools[:5]):
                print(f"   {i+1}. Pool ID: {pool['pool_id'][:10]}...")
                print(f"      {pool['trusted_tokens']}")
                print(f"      Fee: {pool['fee']} bps")

        # Summary
        print(f"\n📈 Summary:")
        print(f"   Total V2 pools with trusted tokens: {len(v2_pools)}")
        print(f"   Total V3 pools with trusted tokens: {len(v3_pools)}")
        print(f"   Total V4 pools with trusted tokens: {len(v4_pools)}")
        print(f"   Total pools: {len(v2_pools) + len(v3_pools) + len(v4_pools)}")

        # Save to files for later use
        import json

        if v2_pools:
            with open('/home/sam-sullivan/dynamicWhitelist/data/v2_trusted_pools.json', 'w') as f:
                json.dump(v2_pools, f, indent=2, default=str)
            print(f"\n💾 Saved V2 pools to data/v2_trusted_pools.json")

        if v3_pools:
            with open('/home/sam-sullivan/dynamicWhitelist/data/v3_trusted_pools.json', 'w') as f:
                json.dump(v3_pools, f, indent=2, default=str)
            print(f"💾 Saved V3 pools to data/v3_trusted_pools.json")

        if v4_pools:
            with open('/home/sam-sullivan/dynamicWhitelist/data/v4_trusted_pools.json', 'w') as f:
                json.dump(v4_pools, f, indent=2, default=str)
            print(f"💾 Saved V4 pools to data/v4_trusted_pools.json")

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
