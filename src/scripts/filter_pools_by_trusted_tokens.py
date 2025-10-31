"""
Filter Uniswap V2/V3/V4 pools to find those containing at least one trusted token.
"""

import polars as pl
from pathlib import Path
from typing import List, Dict, Set
from src.config import ConfigManager


def load_v3_pools(data_dir: Path) -> pl.DataFrame:
    """Load all V3 PoolCreated events from parquet files."""
    v3_pool_dir = data_dir / "uniswap_v3_poolcreated_events"

    if not v3_pool_dir.exists():
        print(f"❌ V3 pool directory not found: {v3_pool_dir}")
        return pl.DataFrame()

    parquet_files = list(v3_pool_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"❌ No V3 parquet files found in {v3_pool_dir}")
        return pl.DataFrame()

    print(f"📂 Loading {len(parquet_files)} V3 pool files...")

    dfs = []
    for file in parquet_files:
        try:
            df = pl.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Failed to read {file.name}: {e}")

    if not dfs:
        return pl.DataFrame()

    return pl.concat(dfs)


def load_v4_pools(data_dir: Path) -> pl.DataFrame:
    """Load all V4 Initialize events from parquet files."""
    v4_pool_dir = data_dir / "uniswap_v4_initialize_events"

    if not v4_pool_dir.exists():
        print(f"❌ V4 pool directory not found: {v4_pool_dir}")
        return pl.DataFrame()

    parquet_files = list(v4_pool_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"❌ No V4 parquet files found in {v4_pool_dir}")
        return pl.DataFrame()

    print(f"📂 Loading {len(parquet_files)} V4 pool files...")

    dfs = []
    for file in parquet_files:
        try:
            df = pl.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Failed to read {file.name}: {e}")

    if not dfs:
        return pl.DataFrame()

    return pl.concat(dfs)


def filter_v3_pools_by_trusted_tokens(
    pools_df: pl.DataFrame,
    trusted_tokens: Dict[str, str]
) -> pl.DataFrame:
    """
    Filter V3 pools to those containing at least one trusted token.

    Args:
        pools_df: DataFrame with V3 PoolCreated events (token0, token1, pool)
        trusted_tokens: Dict mapping token name to address

    Returns:
        Filtered DataFrame with trusted pools
    """
    if pools_df.is_empty():
        return pools_df

    # Normalize trusted token addresses to lowercase
    trusted_addresses = {addr.lower() for addr in trusted_tokens.values()}

    # Filter pools where token0 OR token1 is in trusted tokens
    filtered = pools_df.filter(
        (pl.col("token0").str.to_lowercase().is_in(trusted_addresses)) |
        (pl.col("token1").str.to_lowercase().is_in(trusted_addresses))
    )

    # Add column indicating which token(s) are trusted
    token_name_map = {addr.lower(): name for name, addr in trusted_tokens.items()}

    def get_trusted_tokens_in_pool(row):
        trusted = []
        token0_lower = row["token0"].lower() if isinstance(row["token0"], str) else ""
        token1_lower = row["token1"].lower() if isinstance(row["token1"], str) else ""

        if token0_lower in token_name_map:
            trusted.append(f"token0={token_name_map[token0_lower]}")
        if token1_lower in token_name_map:
            trusted.append(f"token1={token_name_map[token1_lower]}")

        return ", ".join(trusted)

    return filtered


def filter_v4_pools_by_trusted_tokens(
    pools_df: pl.DataFrame,
    trusted_tokens: Dict[str, str]
) -> pl.DataFrame:
    """
    Filter V4 pools to those containing at least one trusted token.

    Args:
        pools_df: DataFrame with V4 Initialize events (currency0, currency1, id)
        trusted_tokens: Dict mapping token name to address

    Returns:
        Filtered DataFrame with trusted pools
    """
    if pools_df.is_empty():
        return pools_df

    # Normalize trusted token addresses to lowercase
    trusted_addresses = {addr.lower() for addr in trusted_tokens.values()}

    # Filter pools where currency0 OR currency1 is in trusted tokens
    filtered = pools_df.filter(
        (pl.col("currency0").str.to_lowercase().is_in(trusted_addresses)) |
        (pl.col("currency1").str.to_lowercase().is_in(trusted_addresses))
    )

    return filtered


def main():
    """Main function to filter pools by trusted tokens."""

    # Initialize config
    config_manager = ConfigManager()
    chain_config = config_manager.chains

    # Get trusted tokens for ethereum
    chain_name = "ethereum"
    trusted_tokens = chain_config.get_trusted_tokens_for_chain()[chain_name]

    print(f"🔍 Filtering pools with trusted tokens: {list(trusted_tokens.keys())}")
    print(f"Trusted token addresses: {[addr[:10]+'...' for addr in trusted_tokens.values()]}")

    # Data directory
    data_dir = Path("/home/sam-sullivan/dynamicWhitelist/data/ethereum")

    # Load V3 pools
    print(f"\n📊 Loading Uniswap V3 pools...")
    v3_pools = load_v3_pools(data_dir)

    if not v3_pools.is_empty():
        print(f"   Total V3 pools: {len(v3_pools)}")

        # Filter by trusted tokens
        v3_filtered = filter_v3_pools_by_trusted_tokens(v3_pools, trusted_tokens)
        print(f"   V3 pools with trusted tokens: {len(v3_filtered)}")
        print(f"   Percentage: {len(v3_filtered) / len(v3_pools) * 100:.1f}%")

        # Show sample
        if len(v3_filtered) > 0:
            print(f"\n📋 Sample V3 pools with trusted tokens:")
            print(v3_filtered.select(["pool", "token0", "token1", "fee"]).head(5))

            # Save filtered pools
            output_file = data_dir / "v3_pools_with_trusted_tokens.parquet"
            v3_filtered.write_parquet(output_file)
            print(f"\n💾 Saved filtered V3 pools to: {output_file}")

    # Load V4 pools
    print(f"\n📊 Loading Uniswap V4 pools...")
    v4_pools = load_v4_pools(data_dir)

    if not v4_pools.is_empty():
        print(f"   Total V4 pools: {len(v4_pools)}")

        # Filter by trusted tokens
        v4_filtered = filter_v4_pools_by_trusted_tokens(v4_pools, trusted_tokens)
        print(f"   V4 pools with trusted tokens: {len(v4_filtered)}")
        print(f"   Percentage: {len(v4_filtered) / len(v4_pools) * 100:.1f}%")

        # Show sample
        if len(v4_filtered) > 0:
            print(f"\n📋 Sample V4 pools with trusted tokens:")
            print(v4_filtered.select(["id", "currency0", "currency1", "fee"]).head(5))

            # Save filtered pools
            output_file = data_dir / "v4_pools_with_trusted_tokens.parquet"
            v4_filtered.write_parquet(output_file)
            print(f"\n💾 Saved filtered V4 pools to: {output_file}")

    # Summary
    print(f"\n📈 Summary:")
    if not v3_pools.is_empty():
        print(f"   V3: {len(v3_filtered)} / {len(v3_pools)} pools")
    if not v4_pools.is_empty():
        print(f"   V4: {len(v4_filtered)} / {len(v4_pools)} pools")


if __name__ == "__main__":
    main()
