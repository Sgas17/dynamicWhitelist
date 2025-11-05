"""
Unified Liquidity Snapshot Processor

This processor handles liquidity events (Mint/Burn) from parquet files,
processes them using degenbot's tick map logic, and stores:
1. Snapshots to PostgreSQL (complete state at specific block)
2. Individual events to TimescaleDB (time-series data)

The processor supports incremental updates and can generate fresh snapshots
from scratch or update existing ones.
"""

import logging
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set
from weakref import WeakSet

import eth_abi.abi as eth_abi
import polars as pl
from degenbot.types import BoundedCache
from degenbot.uniswap.v3_liquidity_pool import UniswapV3Pool
from degenbot.uniswap.v3_types import (
    UniswapV3BitmapAtWord,
    UniswapV3LiquidityAtTick,
    UniswapV3PoolLiquidityMappingUpdate,
    UniswapV3PoolState,
)
from degenbot.uniswap.v4_liquidity_pool import UniswapV4Pool
from degenbot.uniswap.v4_types import (
    UniswapV4BitmapAtWord,
    UniswapV4LiquidityAtTick,
    UniswapV4PoolLiquidityMappingUpdate,
    UniswapV4PoolState,
)
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes

from src.core.storage.postgres_liquidity import (
    get_snapshot_statistics,
    load_liquidity_snapshot,
    setup_liquidity_snapshots_table,
    store_liquidity_snapshot,
)
from src.core.storage.postgres_pools import get_pools_by_factory
from src.core.storage.timescaledb import get_timescale_engine
from src.core.storage.timescaledb_liquidity import (
    setup_liquidity_updates_table,
    store_liquidity_updates_batch,
)
from src.processors.base import BaseProcessor

logger = logging.getLogger(__name__)


class MockV3LiquidityPool(UniswapV3Pool):
    """
    Mock pool for processing liquidity events.

    This class uses degenbot's tick map update logic without
    needing a full pool instance.
    """

    def __init__(self):
        self._initial_state_block = 0
        self._state_cache = BoundedCache(max_items=8)
        self._subscribers = WeakSet()
        self._state_lock = Lock()
        self._swap_step_cache_lock = Lock()
        self._swap_step_cache = {}
        self._update_block = 0  # Track the latest block processed (private attribute)
        self.sparse_liquidity_map = False
        self.name = "V3 POOL"

    @property
    def update_block(self) -> int:
        """Return the latest block processed."""
        return self._update_block


class MockV4LiquidityPool(UniswapV4Pool):
    """
    Mock pool for processing V4 liquidity events.

    This class uses degenbot's V4 tick map update logic without
    needing a full pool instance.
    """

    def __init__(self):
        self._initial_state_block = 0
        self._state_cache = BoundedCache(max_items=8)
        self._subscribers = WeakSet()
        self._state_lock = Lock()
        self._swap_step_cache_lock = Lock()
        self._swap_step_cache = {}
        self._update_block = 0  # Track the latest block processed (private attribute)
        self.sparse_liquidity_map = False
        self.name = "V4 POOL"

    @property
    def update_block(self) -> int:
        """Return the latest block processed."""
        return self._update_block


class UnifiedLiquidityProcessor(BaseProcessor):
    """
    Unified processor for liquidity events across all protocols.

    Features:
    - Process Mint/Burn events from parquet files
    - Generate complete snapshots of tick_bitmap and tick_data
    - Store snapshots to PostgreSQL
    - Store individual events to TimescaleDB
    - Support incremental updates (only process new events)
    - Cleanup old parquet files after processing
    """

    def __init__(
        self,
        chain: str = "ethereum",
        protocol: str = "unified_liquidity",
        block_chunk_size: int = 100000,
        enable_blacklist: bool = False,
    ):
        """
        Initialize the liquidity processor.

        Args:
            chain: Chain name (e.g., "ethereum")
            protocol: Protocol identifier (default: "unified_liquidity")
            block_chunk_size: Process events in chunks of this many blocks
            enable_blacklist: Whether to filter blacklisted pools
        """
        super().__init__(chain=chain, protocol=protocol)

        # Set chain_id using ConfigManager (available after super().__init__)
        self.chain_id = self.config.chains.get_chain_id(chain)
        self.block_chunk_size = block_chunk_size
        self.enable_blacklist = enable_blacklist
        self.engine = get_timescale_engine()

        # Setup tables
        self._setup_tables()

        # Initialize degenbot helpers for tick map updates (V3 and V4)
        self.v3_lp_helper = MockV3LiquidityPool()
        self.v4_lp_helper = MockV4LiquidityPool()

        logger.info(
            f"UnifiedLiquidityProcessor initialized for {chain} "
            f"(chain_id: {self.chain_id})"
        )

    def _setup_tables(self):
        """Setup required database tables."""
        setup_liquidity_snapshots_table(self.engine, self.chain_id)
        setup_liquidity_updates_table(self.engine, self.chain_id)

    def validate_config(self) -> bool:
        """Validate processor configuration."""
        try:
            # Check that we can connect to database
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")

            # Check that we have protocol configs
            protocols = ["uniswap_v3", "uniswap_v4"]
            for protocol in protocols:
                try:
                    self.config.protocols.get_event_hash(f"{protocol}_mint")
                    self.config.protocols.get_event_hash(f"{protocol}_burn")
                except Exception:
                    logger.warning(f"Missing event hashes for {protocol}")

            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

    async def process(self, **kwargs) -> Dict[str, Any]:
        """
        Main process method (required by BaseProcessor).

        Delegates to process_liquidity_snapshots().
        """
        return await self.process_liquidity_snapshots(**kwargs)

    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single liquidity event.

        Args:
            event: Event dict with decoded data

        Returns:
            Processed event dict or None if invalid
        """
        # This is handled in the batch processing logic
        # Individual event processing is done in _decode_liquidity_events
        return event

    def _get_events_path(self, protocol: str) -> Path:
        """Get path to liquidity events for protocol."""
        data_dir = Path(self.config.base.DATA_DIR)
        chain_path = data_dir / self.chain

        # Protocol-specific event paths
        if protocol == "uniswap_v3":
            return chain_path / f"{protocol}_modifyliquidity_events"
        elif protocol == "uniswap_v4":
            return chain_path / f"{protocol}_modifyliquidity_events"
        else:
            raise ValueError(f"Unknown protocol: {protocol}")

    def _get_last_snapshot_block(self, protocol: str) -> int:
        """
        Get the last snapshot block from database for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Last block number with snapshot, or 0 if none
        """
        stats = get_snapshot_statistics(self.chain_id)
        if stats.get("total_snapshots", 0) == 0:
            return 0

        # Get latest snapshot block for this protocol
        # For now, return the overall latest block
        return stats.get("latest_snapshot_block", 0)

    def _load_pools_from_database(self, protocol: str) -> Dict[str, dict]:
        """
        Load pool data from database for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Dict mapping pool_address to pool data
        """
        # Get factory addresses for protocol
        factories = self.config.protocols.get_factory_addresses(protocol, self.chain)

        pools = {}
        for factory in factories:
            factory_pools = get_pools_by_factory(
                factory_address=factory.lower(),  # Normalize to lowercase for database query
                chain_id=self.chain_id,
                active_only=self.enable_blacklist,  # If blacklist enabled, only get active pools
            )

            for pool in factory_pools:
                pools[pool["address"]] = pool

        logger.info(f"Loaded {len(pools)} pools for {protocol} from database")
        return pools

    def _get_pools_with_events(
        self, events_path: Path, start_block: int = 0, end_block: Optional[int] = None
    ) -> Set[str]:
        """
        Get set of pool addresses that have liquidity events in block range.

        Args:
            events_path: Path to parquet files
            start_block: Start block (inclusive)
            end_block: End block (inclusive), None for all

        Returns:
            Set of pool addresses with events (as hex strings)
        """
        logger.info("Finding pools with liquidity events...")

        events = (
            pl.scan_parquet(events_path / "*.parquet")
            .select("address", "block_number")
            .filter(pl.col("block_number") >= start_block)
        )

        if end_block is not None:
            events = events.filter(pl.col("block_number") <= end_block)

        # Get unique pool addresses (they come as binary bytes from parquet)
        binary_addresses = events.collect().get_column("address").unique().to_list()

        # Convert binary addresses to lowercase hex strings for comparison with database
        pools_with_events = set()
        for addr in binary_addresses:
            if isinstance(addr, bytes):
                # Convert bytes to lowercase hex string with 0x prefix
                hex_addr = ("0x" + addr.hex()).lower()
                pools_with_events.add(hex_addr)
            else:
                # Already a string, convert to lowercase
                pools_with_events.add(addr.lower())

        logger.info(f"Found {len(pools_with_events)} pools with liquidity events")
        return pools_with_events

    def _decode_liquidity_events(
        self, events_df: pl.DataFrame, protocol: str
    ) -> List[Dict[str, Any]]:
        """
        Decode liquidity events from parquet dataframe.

        Args:
            events_df: Polars dataframe with raw event data
            protocol: Protocol name (for event hash lookup)

        Returns:
            List of decoded event dicts
        """
        mint_topic = HexBytes(self.config.protocols.get_event_hash(f"{protocol}_mint"))
        burn_topic = HexBytes(self.config.protocols.get_event_hash(f"{protocol}_burn"))

        decoded_events = []

        # Check if this is V4 (uses ModifyLiquidity event)
        is_v4 = protocol == "uniswap_v4"

        for row in events_df.iter_rows(named=True):
            try:
                log_hash = row["topic0"]
                event_data = row["data"]

                if is_v4:
                    # V4 ModifyLiquidity event structure:
                    # - topic0: event hash
                    # - topic1: PoolId (indexed)
                    # - topic2: sender address (indexed) - not needed for tick map updates
                    # - data: (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)
                    tick_lower, tick_upper, liquidity_delta, salt = eth_abi.decode(
                        types=["int24", "int24", "int256", "bytes32"],
                        data=event_data,
                    )
                    # Determine if this is a mint or burn based on liquidity_delta sign
                    event_type = "Mint" if liquidity_delta > 0 else "Burn"
                    sender = None  # Not needed for tick map updates
                    amount0 = 0  # V4 doesn't emit amounts in the event
                    amount1 = 0
                else:
                    # V3 style: tick_lower and tick_upper in topics
                    topic2 = row["topic2"]
                    topic3 = row["topic3"]

                    # Decode tick_lower and tick_upper from topics
                    tick_lower = eth_abi.decode(types=["int24"], data=topic2)[0]
                    tick_upper = eth_abi.decode(types=["int24"], data=topic3)[0]

                    # Decode liquidity delta and amounts from data
                    if log_hash == mint_topic:
                        # Mint: (address sender, uint128 amount, uint256 amount0, uint256 amount1)
                        sender, liquidity_delta, amount0, amount1 = eth_abi.decode(
                            types=["address", "int128", "uint256", "uint256"],
                            data=event_data,
                        )
                        event_type = "Mint"
                    elif log_hash == burn_topic:
                        # Burn: (uint128 amount, uint256 amount0, uint256 amount1)
                        liquidity_delta, amount0, amount1 = eth_abi.decode(
                            types=["int128", "uint256", "uint256"],
                            data=event_data,
                        )
                        liquidity_delta = -liquidity_delta  # Negative for burns
                        sender = None
                        event_type = "Burn"
                    else:
                        logger.warning(f"Unknown event hash: {log_hash}")
                        continue

                # Handle transaction_hash conversion from binary to hex
                tx_hash = row.get("transaction_hash")
                if isinstance(tx_hash, bytes):
                    tx_hash_str = "0x" + tx_hash.hex()
                elif tx_hash:
                    tx_hash_str = (
                        tx_hash if tx_hash.startswith("0x") else "0x" + tx_hash
                    )
                else:
                    tx_hash_str = "0x" + "0" * 64

                # Handle pool_address conversion from binary to lowercase hex
                pool_addr = row["address"]
                if isinstance(pool_addr, bytes):
                    pool_addr_str = ("0x" + pool_addr.hex()).lower()
                elif isinstance(pool_addr, str):
                    pool_addr_str = pool_addr.lower()
                else:
                    pool_addr_str = str(pool_addr).lower()

                decoded_events.append(
                    {
                        "pool_address": pool_addr_str,
                        "block_number": row["block_number"],
                        "transaction_index": row["transaction_index"],
                        "log_index": row.get("log_index", 0),
                        "transaction_hash": tx_hash_str,
                        "event_type": event_type,
                        "tick_lower": tick_lower,
                        "tick_upper": tick_upper,
                        "liquidity_delta": int(liquidity_delta),
                        "sender_address": to_checksum_address(sender)
                        if sender
                        else None,
                        "amount0": int(amount0),
                        "amount1": int(amount1),
                        "event_time": datetime.fromtimestamp(
                            row.get("timestamp", 0), UTC
                        )
                        if row.get("timestamp")
                        else datetime.now(UTC),
                    }
                )
            except Exception as e:
                logger.error(f"Error decoding event: {e}")
                continue

        logger.info(f"Decoded {len(decoded_events)} liquidity events")
        return decoded_events

    def _update_tick_maps(
        self,
        tick_bitmap: dict,
        tick_data: dict,
        tick_lower: int,
        tick_upper: int,
        liquidity_delta: int,
        tick_spacing: int,
        block_number: int,
        protocol: str = "uniswap_v3",
    ):
        """
        Update tick_bitmap and tick_data using degenbot logic.

        Args:
            tick_bitmap: Current tick bitmap state
            tick_data: Current tick data state (simplified)
            tick_lower: Lower tick
            tick_upper: Upper tick
            liquidity_delta: Liquidity change (positive for mint, negative for burn)
            tick_spacing: Pool tick spacing
            block_number: Block number of this update
            protocol: Protocol identifier (e.g., "uniswap_v3", "uniswap_v4")
        """
        # Determine if this is V4 based on protocol
        is_v4 = "v4" in protocol.lower()

        # Select appropriate helper and types
        if is_v4:
            lp_helper = self.v4_lp_helper
            PoolState = UniswapV4PoolState
            BitmapAtWord = UniswapV4BitmapAtWord
            LiquidityAtTick = UniswapV4LiquidityAtTick
            PoolLiquidityMappingUpdate = UniswapV4PoolLiquidityMappingUpdate
        else:
            lp_helper = self.v3_lp_helper
            PoolState = UniswapV3PoolState
            BitmapAtWord = UniswapV3BitmapAtWord
            LiquidityAtTick = UniswapV3LiquidityAtTick
            PoolLiquidityMappingUpdate = UniswapV3PoolLiquidityMappingUpdate

        # Configure helper pool
        lp_helper.tick_spacing = tick_spacing
        lp_helper._state_cache.clear()

        # Set current state from provided data
        state_kwargs = {
            "block": None,
            "liquidity": 2**256 - 1,
            "sqrt_price_x96": 0,
            "tick": 0,
            "tick_bitmap": {int(k): BitmapAtWord(**v) for k, v in tick_bitmap.items()},
            "tick_data": {int(k): LiquidityAtTick(**v) for k, v in tick_data.items()},
        }

        # V4 uses 'id' field instead of 'address'
        if is_v4:
            state_kwargs["id"] = HexBytes("0x" + "00" * 32)
        else:
            state_kwargs["address"] = "0x0000000000000000000000000000000000000000"

        lp_helper._state = PoolState(**state_kwargs)

        # Debug logging before update
        logger.debug(
            f"  Applying update: block={block_number}, liquidity_delta={liquidity_delta}, "
            f"tick_lower={tick_lower}, tick_upper={tick_upper}"
        )

        # Check current state at ticks
        current_lower = tick_data.get(tick_lower, {})
        current_upper = tick_data.get(tick_upper, {})
        logger.debug(
            f"  Before update - Lower tick {tick_lower}: gross={current_lower.get('liquidity_gross', 0)}, net={current_lower.get('liquidity_net', 0)}"
        )
        logger.debug(
            f"  Before update - Upper tick {tick_upper}: gross={current_upper.get('liquidity_gross', 0)}, net={current_upper.get('liquidity_net', 0)}"
        )

        # Apply liquidity update
        try:
            lp_helper.update_liquidity_map(
                update=PoolLiquidityMappingUpdate(
                    block_number=block_number,
                    liquidity=liquidity_delta,
                    tick_lower=tick_lower,
                    tick_upper=tick_upper,
                )
            )
        except Exception as e:
            logger.error(f"  ❌ ERROR applying update: {e}")
            logger.error(
                f"  Event details: block={block_number}, liquidity_delta={liquidity_delta}, "
                f"tick_lower={tick_lower}, tick_upper={tick_upper}"
            )
            logger.error(f"  Current lower tick {tick_lower}: {current_lower}")
            logger.error(f"  Current upper tick {tick_upper}: {current_upper}")
            raise

        # Extract updated state (full degenbot format)
        updated_bitmap = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_bitmap.items()
        }
        updated_tick_data_full = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_data.items()
        }

        # Convert to simplified format (use snake_case - Python standard)
        updated_tick_data_simplified = {
            k: {
                "liquidity_net": v["liquidity_net"],
                "liquidity_gross": v["liquidity_gross"],
                "block_number": v.get("block", block_number),
            }
            for k, v in updated_tick_data_full.items()
        }

        # Update in-place
        tick_bitmap.clear()
        tick_bitmap.update(updated_bitmap)
        tick_data.clear()
        tick_data.update(updated_tick_data_simplified)

    async def process_liquidity_snapshots(
        self,
        protocol: str = "uniswap_v3",
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force_rebuild: bool = False,
        cleanup_parquet: bool = True,
    ) -> Dict[str, Any]:
        """
        Process liquidity events and generate snapshots.

        Args:
            protocol: Protocol to process
            start_block: Start from this block (None = use last snapshot block)
            end_block: End at this block (None = process all available)
            force_rebuild: Rebuild from scratch ignoring existing snapshots
            cleanup_parquet: Clean up old parquet files after processing

        Returns:
            Result dict with statistics
        """
        logger.info(f"Processing liquidity snapshots for {protocol}...")

        # Get events path
        events_path = self._get_events_path(protocol)
        if not events_path.exists():
            logger.error(f"Events path does not exist: {events_path}")
            return {"error": "Events path not found"}

        # Determine start block
        if force_rebuild:
            start_block = 0
        elif start_block is None:
            start_block = self._get_last_snapshot_block(protocol) + 1

        logger.info(f"Processing events from block {start_block}")

        # Load pool data from database
        pool_data = self._load_pools_from_database(protocol)
        if not pool_data:
            logger.error(f"No pools found for {protocol}")
            return {"error": "No pools found"}

        # Get pools with events in this range
        pools_with_events = self._get_pools_with_events(
            events_path, start_block=start_block, end_block=end_block
        )

        # Filter to only pools we track
        tracked_pools_with_events = pools_with_events & set(pool_data.keys())
        logger.info(
            f"Processing {len(tracked_pools_with_events)} tracked pools with events"
        )

        # Load existing snapshots or initialize empty
        liquidity_snapshots = {}
        for pool_address in tracked_pools_with_events:
            if force_rebuild:
                liquidity_snapshots[pool_address] = {
                    "tick_bitmap": {},
                    "tick_data": {},
                }
            else:
                snapshot = load_liquidity_snapshot(pool_address, self.chain_id)
                if snapshot:
                    liquidity_snapshots[pool_address] = {
                        "tick_bitmap": snapshot["tick_bitmap"],
                        "tick_data": snapshot["tick_data"],
                    }
                else:
                    liquidity_snapshots[pool_address] = {
                        "tick_bitmap": {},
                        "tick_data": {},
                    }

        # Get last event block
        events_df = pl.scan_parquet(events_path / "*.parquet").collect()

        # Convert binary addresses to lowercase hex strings for filtering (to match database format)
        events_df = events_df.with_columns(
            pl.col("address")
            .map_elements(
                lambda x: ("0x" + x.hex()).lower()
                if isinstance(x, bytes)
                else x.lower(),
                return_dtype=pl.String,
            )
            .alias("address")
        )

        last_event_block = events_df.select(pl.max("block_number")).item()

        if end_block:
            last_event_block = min(last_event_block, end_block)

        logger.info(f"Data contains events up to block {last_event_block}")

        # Process events in chunks
        total_events_processed = 0
        pools_updated = set()
        all_decoded_events = []

        for chunk_start in range(
            start_block, last_event_block + 1, self.block_chunk_size
        ):
            chunk_end = min(chunk_start + self.block_chunk_size, last_event_block + 1)

            # Get events for this chunk
            chunk_events = (
                events_df.filter(pl.col("block_number") >= chunk_start)
                .filter(pl.col("block_number") < chunk_end)
                .filter(pl.col("address").is_in(list(tracked_pools_with_events)))
            )

            if chunk_events.is_empty():
                continue

            logger.info(
                f"Processing chunk {chunk_start}-{chunk_end} "
                f"({len(chunk_events)} events)"
            )

            # Decode events
            decoded_events = self._decode_liquidity_events(chunk_events, protocol)
            all_decoded_events.extend(decoded_events)
            total_events_processed += len(decoded_events)

            # Process events by pool
            for pool_address in set(e["pool_address"] for e in decoded_events):
                # Debug: Check if address format matches
                if pool_address not in pool_data:
                    logger.error(
                        f"Pool address {pool_address} not found in pool_data keys"
                    )
                    logger.error(f"Sample pool_data keys: {list(pool_data.keys())[:5]}")
                    logger.error(
                        f"Looking for address format: {type(pool_address)}, value: {pool_address}"
                    )
                    continue
                pool = pool_data[pool_address]
                pool_events = [
                    e for e in decoded_events if e["pool_address"] == pool_address
                ]

                # Get current snapshot state
                tick_bitmap = liquidity_snapshots[pool_address]["tick_bitmap"]
                tick_data = liquidity_snapshots[pool_address]["tick_data"]

                # Apply each event
                for event in sorted(
                    pool_events,
                    key=lambda x: (x["block_number"], x["transaction_index"]),
                ):
                    self._update_tick_maps(
                        tick_bitmap=tick_bitmap,
                        tick_data=tick_data,
                        tick_lower=event["tick_lower"],
                        tick_upper=event["tick_upper"],
                        liquidity_delta=event["liquidity_delta"],
                        tick_spacing=pool["tick_spacing"],
                        block_number=event["block_number"],
                        protocol=protocol,
                    )

                pools_updated.add(pool_address)

            # Store snapshots for this chunk
            for pool_address in pools_updated:
                pool = pool_data[pool_address]
                snapshot_data = {
                    "snapshot_block": chunk_end - 1,
                    "snapshot_timestamp": datetime.now(UTC),
                    "tick_bitmap": liquidity_snapshots[pool_address]["tick_bitmap"],
                    "tick_data": liquidity_snapshots[pool_address]["tick_data"],
                    "factory": pool["factory"],
                    "asset0": pool["asset0"],
                    "asset1": pool["asset1"],
                    "fee": pool["fee"],
                    "tick_spacing": pool["tick_spacing"],
                    "protocol": protocol,
                    "last_event_block": chunk_end - 1,
                }

                store_liquidity_snapshot(pool_address, snapshot_data, self.chain_id)

        # Store all events to TimescaleDB
        if all_decoded_events:
            logger.info(f"Storing {len(all_decoded_events)} events to TimescaleDB...")
            stored_count = store_liquidity_updates_batch(
                all_decoded_events, chain_id=self.chain_id, batch_size=1000
            )
            logger.info(f"Stored {stored_count} events to TimescaleDB")

        # Cleanup parquet files if requested
        if cleanup_parquet:
            self._cleanup_old_parquet_files(events_path, keep_latest=True)

        result = {
            "protocol": protocol,
            "start_block": start_block,
            "end_block": last_event_block,
            "total_events_processed": total_events_processed,
            "pools_updated": len(pools_updated),
            "snapshots_stored": len(pools_updated),
            "events_stored": stored_count if all_decoded_events else 0,
        }

        logger.info(f"Liquidity snapshot processing complete: {result}")
        return result

    def _cleanup_old_parquet_files(self, parquet_path: Path, keep_latest: bool = True):
        """Clean up old parquet files after processing."""
        parquet_files = sorted(
            parquet_path.glob("*.parquet"), key=lambda p: p.stat().st_mtime
        )

        if keep_latest and len(parquet_files) > 1:
            files_to_delete = parquet_files[:-1]  # Keep latest
        elif not keep_latest:
            files_to_delete = parquet_files
        else:
            files_to_delete = []

        for file_path in files_to_delete:
            try:
                file_path.unlink()
                logger.debug(f"Deleted old parquet file: {file_path.name}")
            except Exception as e:
                logger.error(f"Error deleting {file_path.name}: {e}")

        if files_to_delete:
            logger.info(f"Cleaned up {len(files_to_delete)} old parquet files")

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored snapshots and events."""
        snapshot_stats = get_snapshot_statistics(self.chain_id)

        return {
            "chain": self.chain,
            "chain_id": self.chain_id,
            "snapshots": snapshot_stats,
        }
