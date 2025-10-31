"""
Reth Database Snapshot Loader

Uses the Rust scrape_rethdb_data library to quickly load current liquidity state
directly from the reth database instead of processing historical events.

This provides a MUCH faster way to create initial snapshots.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class RethSnapshotLoader:
    """
    Load liquidity snapshots directly from reth database using Rust library.

    This bypasses event processing and reads the current on-chain state directly,
    which is orders of magnitude faster for initial snapshot creation.
    """

    def __init__(self, reth_db_path: str):
        """
        Initialize the snapshot loader.

        Args:
            reth_db_path: Path to the reth database directory
                         (e.g., "/home/user/.local/share/reth/mainnet/db")
        """
        self.reth_db_path = Path(reth_db_path)

        if not self.reth_db_path.exists():
            raise ValueError(f"Reth database path does not exist: {reth_db_path}")

        # Try to import the Rust library
        try:
            import scrape_rethdb_data
            self.rust_lib = scrape_rethdb_data
            logger.info(f"Successfully loaded scrape_rethdb_data Rust library")
        except ImportError as e:
            raise ImportError(
                "scrape_rethdb_data Rust library not installed. "
                "Run: uv run maturin develop --release --manifest-path ~/scrape_rethdb_data/Cargo.toml --features=python"
            ) from e

    def load_v3_pool_snapshot(
        self,
        pool_address: str,
        tick_spacing: int,
    ) -> Tuple[Dict[int, Dict[str, int]], int]:
        """
        Load V3 pool liquidity snapshot from reth database.

        Args:
            pool_address: Uniswap V3 pool address (checksummed)
            tick_spacing: Tick spacing for the pool (1, 10, 60, 200)

        Returns:
            Tuple of (tick_data_dict, block_number)
            - tick_data_dict: {tick_index: {"liquidity_gross": int, "liquidity_net": int}}
            - block_number: Block number of the snapshot
        """
        logger.info(f"Loading V3 pool snapshot from reth DB: {pool_address}")

        # Prepare pool input for Rust
        pools = [{
            "address": pool_address,
            "protocol": "v3",
            "tick_spacing": tick_spacing,
        }]

        # Call Rust function to collect data
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            None  # No V4 pool IDs needed for V3
        )

        # Parse JSON result
        results = json.loads(result_json)

        if not results:
            raise ValueError(f"No data returned for pool {pool_address}")

        pool_data = results[0]

        # Extract tick data
        tick_data = {}
        if "ticks" in pool_data and pool_data["ticks"]:
            for tick in pool_data["ticks"]:
                tick_index = tick["tick"]  # Rust library uses "tick" not "tick_index"
                tick_data[tick_index] = {
                    "liquidity_gross": tick["liquidity_gross"],
                    "liquidity_net": tick["liquidity_net"],
                }

        # Get block number from slot0 or reserves
        block_number = pool_data.get("block_number", 0)

        logger.info(
            f"✓ Loaded {len(tick_data)} ticks from reth DB "
            f"at block {block_number}"
        )

        return tick_data, block_number

    def load_v4_pool_snapshot(
        self,
        pool_address: str,
        pool_id: str,
        tick_spacing: int,
    ) -> Tuple[Dict[int, Dict[str, int]], Dict[int, str], int]:
        """
        Load V4 pool liquidity snapshot from reth database.

        Args:
            pool_address: Uniswap V4 PoolManager address
            pool_id: V4 pool ID (hex string)
            tick_spacing: Tick spacing for the pool

        Returns:
            Tuple of (tick_data_dict, bitmap_dict, block_number)
            - tick_data_dict: {tick_index: {"liquidity_gross": int, "liquidity_net": int}}
            - bitmap_dict: {word_pos: "0xhex_bitmap_value"}
            - block_number: Block number of the snapshot
        """
        logger.info(f"Loading V4 pool snapshot from reth DB: {pool_id}")

        # Prepare pool input for Rust
        pools = [{
            "address": pool_address,
            "protocol": "v4",
            "tick_spacing": tick_spacing,
        }]

        # Call Rust function with V4 pool ID
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            [pool_id]  # V4 requires pool ID
        )

        # Parse JSON result
        results = json.loads(result_json)

        if not results:
            raise ValueError(f"No data returned for pool {pool_id}")

        pool_data = results[0]

        # Extract tick data
        tick_data = {}
        if "ticks" in pool_data and pool_data["ticks"]:
            for tick in pool_data["ticks"]:
                tick_index = tick["tick"]  # Rust library uses "tick" not "tick_index"
                tick_data[tick_index] = {
                    "liquidity_gross": tick["liquidity_gross"],
                    "liquidity_net": tick["liquidity_net"],
                }

        # Extract bitmap data
        bitmap_data = {}
        if "bitmaps" in pool_data and pool_data["bitmaps"]:
            for bitmap in pool_data["bitmaps"]:
                word_pos = bitmap["word_pos"]
                bitmap_value = bitmap["bitmap"]
                bitmap_data[word_pos] = bitmap_value

        # Get block number
        block_number = pool_data.get("block_number", 0)

        logger.info(
            f"✓ Loaded {len(tick_data)} ticks from reth DB "
            f"at block {block_number}"
        )

        return tick_data, bitmap_data, block_number

    def load_v2_pool_snapshot(
        self,
        pool_address: str,
    ) -> Tuple[Dict[str, int], int]:
        """
        Load V2 pool reserves from reth database.

        Args:
            pool_address: Uniswap V2 pool address (checksummed)

        Returns:
            Tuple of (reserves_dict, block_number)
            - reserves_dict: {"reserve0": int, "reserve1": int, "block_timestamp_last": int}
            - block_number: Block number of the snapshot
        """
        logger.info(f"Loading V2 pool reserves from reth DB: {pool_address}")

        # Prepare pool input for Rust
        pools = [{
            "address": pool_address,
            "protocol": "v2",
            "tick_spacing": None,
        }]

        # Call Rust function
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            None
        )

        # Parse JSON result
        results = json.loads(result_json)

        if not results:
            raise ValueError(f"No data returned for pool {pool_address}")

        pool_data = results[0]

        # Extract reserves
        reserves = pool_data.get("reserves", {})
        block_number = pool_data.get("block_number", 0)

        logger.info(
            f"✓ Loaded V2 reserves from reth DB at block {block_number}: "
            f"reserve0={reserves.get('reserve0', 0)}, "
            f"reserve1={reserves.get('reserve1', 0)}"
        )

        return reserves, block_number

    def batch_load_v3_pools(
        self,
        pool_configs: List[Dict[str, any]],
    ) -> List[Tuple[str, Dict[int, Dict[str, int]], Dict[int, str], int]]:
        """
        Batch load multiple V3 pools efficiently.

        Args:
            pool_configs: List of dicts with keys: address, tick_spacing

        Returns:
            List of tuples: (pool_address, tick_data_dict, bitmap_dict, block_number)
            - tick_data_dict: {tick_index: {"liquidity_gross": int, "liquidity_net": int}}
            - bitmap_dict: {word_pos: "0xhex_bitmap_value"}
        """
        logger.info(f"Batch loading {len(pool_configs)} V3 pools from reth DB")

        # Prepare pools input
        pools = [
            {
                "address": config["address"],
                "protocol": "v3",
                "tick_spacing": config["tick_spacing"],
            }
            for config in pool_configs
        ]

        # Call Rust function once for all pools
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            None
        )

        # Parse results
        results = json.loads(result_json)

        # Process each pool
        output = []
        for pool_data, config in zip(results, pool_configs):
            # Extract tick data
            tick_data = {}
            if "ticks" in pool_data and pool_data["ticks"]:
                for tick in pool_data["ticks"]:
                    tick_index = tick["tick"]  # Rust library uses "tick" not "tick_index"
                    tick_data[tick_index] = {
                        "liquidity_gross": tick["liquidity_gross"],
                        "liquidity_net": tick["liquidity_net"],
                    }

            # Extract bitmap data
            bitmap_data = {}
            if "bitmaps" in pool_data and pool_data["bitmaps"]:
                for bitmap in pool_data["bitmaps"]:
                    word_pos = bitmap["word_pos"]
                    bitmap_value = bitmap["bitmap"]
                    bitmap_data[word_pos] = bitmap_value

            block_number = pool_data.get("block_number", 0)
            output.append((config["address"], tick_data, bitmap_data, block_number))

        logger.info(f"✓ Batch loaded {len(output)} V3 pools from reth DB")

        return output


    def batch_load_v3_states(
        self,
        pool_configs: List[Dict],
    ) -> List[Dict]:
        """
        Batch load V3 pool states (slot0 + liquidity only) for filtering.
        Much faster than batch_load_v3_pools as it skips tick data.

        Args:
            pool_configs: List of dicts with keys: address, tick_spacing

        Returns:
            List of dicts with keys: address, slot0, liquidity, tick, sqrtPriceX96
        """
        logger.info(f"Batch loading {len(pool_configs)} V3 pool states from reth DB (slot0_only)")

        # Prepare pools input with slot0_only flag
        pools = [
            {
                "address": config["address"],
                "protocol": "v3",
                "tick_spacing": config["tick_spacing"],
                "slot0_only": True,  # Only fetch slot0 + liquidity
            }
            for config in pool_configs
        ]

        # Call Rust function
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            None
        )

        # Parse results
        results = json.loads(result_json)

        # Process each pool
        output = []
        for pool_data in results:
            slot0 = pool_data.get("slot0", {})
            liquidity = pool_data.get("liquidity")

            # sqrt_price_x96 comes as hex string from Rust, convert to int
            sqrt_price_hex = slot0.get("sqrt_price_x96", "0x0")
            if isinstance(sqrt_price_hex, str) and sqrt_price_hex.startswith("0x"):
                sqrt_price = int(sqrt_price_hex, 16)
            else:
                sqrt_price = int(sqrt_price_hex)

            output.append({
                "address": pool_data["address"],
                "slot0": slot0,
                "liquidity": str(liquidity) if liquidity is not None else "0",
                "tick": slot0.get("tick", 0),
                "sqrtPriceX96": str(sqrt_price),
            })

        logger.info(f"✓ Batch loaded {len(output)} V3 pool states from reth DB")

        return output

    def batch_load_v4_states(
        self,
        pool_configs: List[Dict],
    ) -> List[Dict]:
        """
        Batch load V4 pool states (slot0 + liquidity only) for filtering.
        Much faster than batch_load_v4_pools as it skips tick data.

        Args:
            pool_configs: List of dicts with keys: pool_id, tick_spacing, pool_manager

        Returns:
            List of dicts with keys: pool_id, slot0, liquidity, tick, sqrtPriceX96
        """
        logger.info(f"Batch loading {len(pool_configs)} V4 pool states from reth DB (slot0_only)")

        # Prepare pools input with slot0_only flag
        pools = []
        pool_ids = []

        for i, config in enumerate(pool_configs):
            pool_manager = config["pool_manager"]
            pool_id = config["pool_id"]

            # Debug: Log first few to check format
            if i < 3:
                logger.info(f"   Pool {i}: manager={pool_manager}, id={pool_id}")

            pools.append({
                "address": pool_manager,
                "protocol": "v4",
                "tick_spacing": config["tick_spacing"],
                "slot0_only": True,  # Only fetch slot0 + liquidity
            })
            pool_ids.append(pool_id)

        # Call Rust function
        result_json = self.rust_lib.collect_pools(
            str(self.reth_db_path),
            pools,
            pool_ids  # V4 requires pool IDs
        )

        # Parse results
        results = json.loads(result_json)

        # Process each pool
        output = []
        for pool_data in results:
            slot0 = pool_data.get("slot0", {})
            liquidity = pool_data.get("liquidity")
            pool_id = pool_data.get("pool_id")

            # sqrt_price_x96 comes as hex string from Rust, convert to int
            sqrt_price_hex = slot0.get("sqrt_price_x96", "0x0")
            if isinstance(sqrt_price_hex, str) and sqrt_price_hex.startswith("0x"):
                sqrt_price = int(sqrt_price_hex, 16)
            else:
                sqrt_price = int(sqrt_price_hex)

            output.append({
                "pool_id": pool_id,
                "slot0": slot0,
                "liquidity": str(liquidity) if liquidity is not None else "0",
                "tick": slot0.get("tick", 0),
                "sqrtPriceX96": str(sqrt_price),
            })

        logger.info(f"✓ Batch loaded {len(output)} V4 pool states from reth DB")

        return output

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Initialize loader
    loader = RethSnapshotLoader("/path/to/reth/db")

    # Load V3 pool snapshot
    pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC-WETH
    tick_data, block_number = loader.load_v3_pool_snapshot(
        pool_address=pool_address,
        tick_spacing=10,
    )

    print(f"Loaded {len(tick_data)} ticks at block {block_number}")

    # Show first few ticks
    for tick_index in sorted(tick_data.keys())[:5]:
        data = tick_data[tick_index]
        print(f"  Tick {tick_index}: gross={data['liquidity_gross']}, net={data['liquidity_net']}")
