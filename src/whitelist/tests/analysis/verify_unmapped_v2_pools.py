#!/usr/bin/env python3
"""
Verify that unmapped tokens truly lack BOTH:
1. Exchange prices (Hyperliquid/Lighter)
2. V2 pools with trusted tokens for price discovery
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, Set

from src.config import get_config
from src.core.storage.postgres import PostgresStorage


async def get_v2_pools_for_tokens(
    storage: PostgresStorage,
    token_symbols: Set[str],
    trusted_addresses: Set[str],
    chain_id: int = 1,
) -> Dict[str, list]:
    """
    Get V2 pools where token pairs with trusted tokens.
    Returns dict of {token_symbol: [list of trusted tokens it pairs with]}
    """
    # Query both tables for V2 pools
    table1 = f"network_{chain_id}__dex_pools"
    table2 = f"network_{chain_id}_dex_pools_cryo"

    # Get token addresses from token registry
    token_addresses = {}
    async with storage.pool.acquire() as conn:
        for symbol in token_symbols:
            result = await conn.fetchrow(
                """
                SELECT LOWER(address) as address
                FROM token_registry
                WHERE UPPER(symbol) = $1
                LIMIT 1
                """,
                symbol.upper(),
            )
            if result:
                token_addresses[symbol] = result["address"]

    if not token_addresses:
        print("⚠️  No token addresses found in registry for these symbols")
        return {}

    # Prepare query parameters
    token_addrs = list(token_addresses.values())
    trusted_list = list(trusted_addresses)

    query = f"""
    SELECT DISTINCT
        LOWER(asset0) as token0,
        LOWER(asset1) as token1
    FROM (
        SELECT asset0, asset1
        FROM {table1}
        WHERE (
            (LOWER(asset0) = ANY($1) AND LOWER(asset1) = ANY($2))
            OR
            (LOWER(asset1) = ANY($1) AND LOWER(asset0) = ANY($2))
        )
        UNION
        SELECT asset0, asset1
        FROM {table2}
        WHERE (
            (LOWER(asset0) = ANY($1) AND LOWER(asset1) = ANY($2))
            OR
            (LOWER(asset1) = ANY($1) AND LOWER(asset0) = ANY($2))
        )
    ) AS combined_pools
    """

    async with storage.pool.acquire() as conn:
        results = await conn.fetch(query, token_addrs, trusted_list)

    # Map back to symbols
    addr_to_symbol = {addr: sym for sym, addr in token_addresses.items()}
    trusted_to_symbol = {}

    # Get trusted token symbols
    async with storage.pool.acquire() as conn:
        for trusted_addr in trusted_addresses:
            result = await conn.fetchrow(
                """
                SELECT symbol
                FROM token_registry
                WHERE LOWER(address) = $1
                LIMIT 1
                """,
                trusted_addr,
            )
            if result:
                trusted_to_symbol[trusted_addr] = result["symbol"]

    # Build result mapping
    token_pools = {symbol: [] for symbol in token_symbols}

    for row in results:
        token0 = row["token0"]
        token1 = row["token1"]

        # Determine which is the unmapped token and which is trusted
        if token0 in addr_to_symbol and token1 in trusted_to_symbol:
            symbol = addr_to_symbol[token0]
            trusted = trusted_to_symbol[token1]
            if trusted not in token_pools[symbol]:
                token_pools[symbol].append(trusted)

        elif token1 in addr_to_symbol and token0 in trusted_to_symbol:
            symbol = addr_to_symbol[token1]
            trusted = trusted_to_symbol[token0]
            if trusted not in token_pools[symbol]:
                token_pools[symbol].append(trusted)

    return token_pools


async def verify_unmapped_tokens():
    """Verify tokens that can't be priced through ANY mechanism."""
    # Load configuration
    config = get_config()

    # Create database config dict
    db_config = {
        "host": config.database.POSTGRES_HOST,
        "port": config.database.POSTGRES_PORT,
        "user": config.database.POSTGRES_USER,
        "password": config.database.POSTGRES_PASSWORD,
        "database": config.database.POSTGRES_DB,
        "pool_size": 10,
        "pool_timeout": 10,
    }

    storage = PostgresStorage(config=db_config)
    await storage.connect()

    # Load trusted tokens for ethereum
    all_trusted_tokens = config.chains.get_trusted_tokens_for_chain()
    trusted_tokens = all_trusted_tokens.get("ethereum", {})
    trusted_addresses = set(addr.lower() for addr in trusted_tokens.values())

    print(f"🔍 Loaded {len(trusted_addresses)} trusted token addresses")
    print(f"   Trusted tokens: {list(trusted_tokens.keys())}")
    print()

    # Load unmapped tokens from overlap analysis
    overlap_path = Path(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/overlap_analysis.json"
    )
    with open(overlap_path, "r") as f:
        overlap_data = json.load(f)

    both_unmapped = set(overlap_data["both_unmapped"])

    print("=" * 80)
    print(
        f"VERIFYING {len(both_unmapped)} TOKENS UNMAPPED IN BOTH EXCHANGES"
    )
    print("=" * 80)
    print()

    # Check for V2 pools with trusted tokens
    print("🔍 Checking for V2 pools with trusted tokens...")
    v2_pools = await get_v2_pools_for_tokens(
        storage, both_unmapped, trusted_addresses
    )

    # Categorize results
    tokens_with_v2 = {
        symbol: pools for symbol, pools in v2_pools.items() if pools
    }
    tokens_without_v2 = {
        symbol: pools for symbol, pools in v2_pools.items() if not pools
    }
    tokens_not_in_registry = both_unmapped - set(v2_pools.keys())

    print()
    print("=" * 80)
    print("📊 RESULTS")
    print("=" * 80)
    print()

    print(f"✅ Tokens WITH V2 pools + trusted: {len(tokens_with_v2)}")
    if tokens_with_v2:
        for symbol, trusted_list in sorted(tokens_with_v2.items()):
            print(f"   • {symbol} → pairs with: {', '.join(trusted_list)}")
    print()

    print(
        f"❌ Tokens WITHOUT V2 pools + trusted: {len(tokens_without_v2)}"
    )
    if tokens_without_v2:
        for symbol in sorted(tokens_without_v2.keys()):
            print(f"   • {symbol}")
    print()

    print(
        f"⚠️  Tokens not in registry: {len(tokens_not_in_registry)}"
    )
    if tokens_not_in_registry:
        for symbol in sorted(tokens_not_in_registry):
            print(f"   • {symbol}")
    print()

    print("=" * 80)
    print("🔍 CONCLUSION")
    print("=" * 80)
    print()

    truly_unpriceable = len(tokens_without_v2) + len(tokens_not_in_registry)
    print(
        f"Tokens that are TRULY unpriceable (no exchange price, no V2 pool): {truly_unpriceable}"
    )
    print(
        f"Tokens that CAN be priced via V2 pools: {len(tokens_with_v2)}"
    )
    print()

    if tokens_with_v2:
        print(
            "✅ These tokens can be priced through V2 pool reserves + trusted token prices"
        )
    print()

    # Save results
    output = {
        "verified_at": "2025-10-14",
        "summary": {
            "total_checked": len(both_unmapped),
            "with_v2_pools": len(tokens_with_v2),
            "without_v2_pools": len(tokens_without_v2),
            "not_in_registry": len(tokens_not_in_registry),
            "truly_unpriceable": truly_unpriceable,
        },
        "tokens_with_v2_pools": tokens_with_v2,
        "tokens_without_v2_pools": list(tokens_without_v2.keys()),
        "tokens_not_in_registry": list(tokens_not_in_registry),
    }

    output_path = Path(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/verification_results.json"
    )
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"💾 Results saved to: {output_path}")

    await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(verify_unmapped_tokens())
