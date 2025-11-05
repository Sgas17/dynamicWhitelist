#!/usr/bin/env python3
"""
Test script for TokenWhitelistBuilder.

Tests the complete whitelist building process and validates output format.
"""

import asyncio
import json
import sys
from pathlib import Path
from pprint import pprint

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.whitelist.builder import TokenWhitelistBuilder


async def test_whitelist_builder():
    """Test the whitelist builder with all data sources."""
    print("=" * 80)
    print("🧪 Testing TokenWhitelistBuilder")
    print("=" * 80)

    # Initialize config and storage
    config = ConfigManager()

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

    try:
        builder = TokenWhitelistBuilder(storage)

        # Build whitelist with all sources and liquidity filter
        print("\n" + "=" * 80)
        print("Building whitelist...")
        print("=" * 80)

        # Get RPC URL from config
        rpc_url = config.chains.get_rpc_url("ethereum")

        result = await builder.build_whitelist(
            top_transfers=100,
            chain="ethereum",
            min_liquidity_usd=10000,  # $10k minimum liquidity
            rpc_url=rpc_url,
        )

        # Validate result structure
        print("\n" + "=" * 80)
        print("✅ Validating Result Structure")
        print("=" * 80)

        # Check required keys
        required_keys = [
            "all_tokens",
            "all_pairs",
            "decimals",
            "symbols",
            "v2_pairs",
            "v3_pairs",
            "v4_pairs",
            "whitelist",
        ]

        for key in required_keys:
            assert key in result, f"Missing required key: {key}"
            print(f"  ✓ {key}: {len(result[key])} items")

        # Validate all_tokens format
        print("\n" + "=" * 80)
        print("📋 Sample all_tokens entries:")
        print("=" * 80)
        sample_tokens = list(result["all_tokens"].items())[:3]
        for addr, token_data in sample_tokens:
            print(f"\n{addr}:")
            print(f"  address: {token_data['address']}")
            print(f"  symbol: {token_data['symbol']}")
            print(f"  decimals: {token_data['decimals']}")

            # Validate structure
            assert "address" in token_data
            assert "symbol" in token_data
            assert "decimals" in token_data
            assert token_data["address"] == addr

        # Validate all_pairs format
        print("\n" + "=" * 80)
        print("🔗 Sample all_pairs entries:")
        print("=" * 80)
        sample_pairs = list(result["all_pairs"].items())[:3]
        for pool_addr, pair_data in sample_pairs:
            print(f"\n{pool_addr}:")
            print(
                f"  token0: {pair_data['token0']['symbol']} ({pair_data['token0']['address'][:10]}...)"
            )
            print(
                f"  token1: {pair_data['token1']['symbol']} ({pair_data['token1']['address'][:10]}...)"
            )
            print(f"  exchange: {pair_data['exchange']}")
            print(f"  fee: {pair_data['fee']}")
            print(f"  tickSpacing: {pair_data['tickSpacing']}")
            print(f"  stable: {pair_data['stable']}")

            # Validate structure
            assert "token0" in pair_data
            assert "token1" in pair_data
            assert "exchange" in pair_data
            assert "address" in pair_data["token0"]
            assert "symbol" in pair_data["token0"]
            assert "decimals" in pair_data["token0"]
            assert "address" in pair_data["token1"]
            assert "symbol" in pair_data["token1"]
            assert "decimals" in pair_data["token1"]

        # Validate decimals mapping
        print("\n" + "=" * 80)
        print("🔢 Sample decimals mapping:")
        print("=" * 80)
        sample_decimals = list(result["decimals"].items())[:5]
        for addr, decimals in sample_decimals:
            symbol = result["symbols"].get(addr, "?")
            print(f"  {symbol:8s} {addr[:10]}... -> {decimals} decimals")

            # Validate consistency
            if addr in result["all_tokens"]:
                assert result["all_tokens"][addr]["decimals"] == decimals

        # Validate symbols mapping
        print("\n" + "=" * 80)
        print("🏷️  Sample symbols mapping:")
        print("=" * 80)
        sample_symbols = list(result["symbols"].items())[:5]
        for addr, symbol in sample_symbols:
            print(f"  {addr[:10]}... -> {symbol}")

            # Validate consistency
            if addr in result["all_tokens"]:
                assert result["all_tokens"][addr]["symbol"] == symbol

        # Validate filtered pairs
        print("\n" + "=" * 80)
        print("🔍 Filtered Pairs Validation:")
        print("=" * 80)

        # V2 pairs
        print(f"\n  V2 Pairs: {len(result['v2_pairs'])}")
        if result["v2_pairs"]:
            sample_v2 = list(result["v2_pairs"].items())[:2]
            for addr, pair in sample_v2:
                print(
                    f"    {addr[:10]}... - {pair['exchange']} - {pair['token0']['symbol']}/{pair['token1']['symbol']}"
                )
                assert "V2" in pair["exchange"]

        # V3 pairs
        print(f"\n  V3 Pairs: {len(result['v3_pairs'])}")
        if result["v3_pairs"]:
            sample_v3 = list(result["v3_pairs"].items())[:2]
            for addr, pair in sample_v3:
                print(
                    f"    {addr[:10]}... - {pair['exchange']} - {pair['token0']['symbol']}/{pair['token1']['symbol']}"
                )
                assert "V3" in pair["exchange"]

        # V4 pairs
        print(f"\n  V4 Pairs: {len(result['v4_pairs'])}")
        if result["v4_pairs"]:
            sample_v4 = list(result["v4_pairs"].items())[:2]
            for addr, pair in sample_v4:
                print(
                    f"    {addr[:10]}... - {pair['exchange']} - {pair['token0']['symbol']}/{pair['token1']['symbol']}"
                )
                assert "V4" in pair["exchange"]

        # Validate whitelist
        print("\n" + "=" * 80)
        print("✨ Whitelist Summary:")
        print("=" * 80)
        print(f"  Total whitelisted tokens: {len(result['whitelist'])}")
        print(f"  Sample tokens:")
        for token_addr in result["whitelist"][:10]:
            symbol = result["symbols"].get(token_addr, "?")
            print(f"    {symbol:8s} {token_addr}")

        # Check token sources breakdown
        print("\n" + "=" * 80)
        print("📊 Token Sources Breakdown:")
        print("=" * 80)
        if "breakdown" in result:
            for source, count in result["breakdown"].items():
                print(f"  {source}: {count}")

        # Show multi-source tokens
        if "token_sources" in result:
            multi_source = {
                token: sources
                for token, sources in result["token_sources"].items()
                if len(sources) > 1
            }
            print(f"\n  Multi-source tokens: {len(multi_source)}")
            for token, sources in sorted(
                multi_source.items(), key=lambda x: -len(x[1])
            )[:5]:
                symbol = result["symbols"].get(token, "?")
                print(f"    {symbol:8s} {token[:10]}... - {', '.join(sources)}")

        # Save sample output
        output_path = project_root / "data" / "test_whitelist_output.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save a smaller sample for inspection
        sample_output = {
            "total_tokens": len(result["whitelist"]),
            "total_pairs": len(result["all_pairs"]),
            "breakdown": result.get("breakdown", {}),
            "sample_tokens": {
                k: result["all_tokens"][k]
                for k in list(result["all_tokens"].keys())[:5]
            },
            "sample_pairs": {
                k: result["all_pairs"][k] for k in list(result["all_pairs"].keys())[:5]
            },
            "sample_decimals": {
                k: result["decimals"][k] for k in list(result["decimals"].keys())[:5]
            },
            "sample_symbols": {
                k: result["symbols"][k] for k in list(result["symbols"].keys())[:5]
            },
            "v2_pairs_count": len(result["v2_pairs"]),
            "v3_pairs_count": len(result["v3_pairs"]),
            "v4_pairs_count": len(result["v4_pairs"]),
        }

        with open(output_path, "w") as f:
            json.dump(sample_output, f, indent=2)

        print(f"\n💾 Saved sample output to {output_path}")

        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)

        return result

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    result = asyncio.run(test_whitelist_builder())
