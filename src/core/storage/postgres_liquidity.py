"""
PostgreSQL Storage Layer for Liquidity Snapshots

This module provides CRUD operations for liquidity snapshot data stored in PostgreSQL.
Snapshots contain tick_bitmap and tick_data (simplified: liquidityNet, liquidityGross, block_number)
for each pool at a specific block height.

Schema:
    network_{chain_id}_liquidity_snapshots:
        - pool_address (TEXT, PRIMARY KEY)
        - snapshot_block (BIGINT)
        - tick_bitmap (JSONB): {word: {bitmap, block_number}}
        - tick_data (JSONB): {tick: {liquidityNet, liquidityGross, block_number}}
        - Pool metadata + statistics
"""

import logging
import json
from datetime import datetime, UTC
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.core.storage.timescaledb import get_timescale_engine

logger = logging.getLogger(__name__)


def get_table_name(chain_id: int) -> str:
    """Get liquidity snapshots table name for chain."""
    return f"network_{chain_id}_liquidity_snapshots"


def setup_liquidity_snapshots_table(engine: Engine, chain_id: int) -> bool:
    """
    Create liquidity snapshots table for the specified chain.

    Args:
        engine: SQLAlchemy engine
        chain_id: Chain ID (e.g., 1 for Ethereum mainnet)

    Returns:
        True if table was created or already exists, False on error
    """
    table_name = get_table_name(chain_id)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Primary key
        pool_address TEXT PRIMARY KEY,

        -- Snapshot metadata
        snapshot_block BIGINT NOT NULL,
        snapshot_timestamp TIMESTAMPTZ NOT NULL,
        snapshot_created_at TIMESTAMPTZ DEFAULT NOW(),

        -- Liquidity state
        tick_bitmap JSONB NOT NULL,        -- {{word: {{bitmap, block_number}}}}
        tick_data JSONB NOT NULL,          -- {{tick: {{liquidityNet, liquidityGross, block_number}}}}

        -- Pool metadata (denormalized for convenience)
        factory TEXT NOT NULL,
        asset0 TEXT NOT NULL,
        asset1 TEXT NOT NULL,
        fee INTEGER NOT NULL,
        tick_spacing INTEGER NOT NULL,
        protocol TEXT NOT NULL,            -- "uniswap_v3", "uniswap_v4", etc.

        -- Statistics
        total_ticks INTEGER DEFAULT 0,     -- Number of initialized ticks
        total_bitmap_words INTEGER DEFAULT 0,  -- Number of bitmap words

        -- Update tracking
        last_event_block BIGINT,           -- Last processed event block
        last_updated TIMESTAMPTZ DEFAULT NOW(),
        update_count INTEGER DEFAULT 0     -- Number of updates since creation
    );
    """

    # Create indexes
    create_indexes_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_block
        ON {table_name}(snapshot_block);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_factory
        ON {table_name}(factory);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_updated
        ON {table_name}(last_updated);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_protocol
        ON {table_name}(protocol);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_tick_data_gin
        ON {table_name} USING GIN (tick_data);
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info(f"Table {table_name} created successfully")

            # Create indexes
            for index_sql in create_indexes_sql.split(";"):
                if index_sql.strip():
                    conn.execute(text(index_sql))
            conn.commit()
            logger.info(f"Indexes created for {table_name}")

            return True
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        return False


def store_liquidity_snapshot(
    pool_address: str,
    snapshot_data: dict,
    chain_id: int = 1
) -> bool:
    """
    Store or update a liquidity snapshot for a pool.

    Args:
        pool_address: Pool address (checksummed)
        snapshot_data: Snapshot dict with all required fields
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        True if stored successfully, False otherwise
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    # Validate required fields
    required_fields = [
        "snapshot_block", "tick_bitmap", "tick_data",
        "factory", "asset0", "asset1", "fee", "tick_spacing", "protocol"
    ]
    for field in required_fields:
        if field not in snapshot_data:
            logger.error(f"Missing required field: {field}")
            return False

    # Validate tick_data structure (simplified: liquidity_net, liquidity_gross, block_number)
    for tick, data in snapshot_data["tick_data"].items():
        if not isinstance(data, dict):
            logger.error(f"Invalid tick_data format for tick {tick}")
            return False
        if set(data.keys()) != {"liquidity_net", "liquidity_gross", "block_number"}:
            logger.error(
                f"Invalid tick_data fields for tick {tick}: {list(data.keys())}. "
                "Expected: liquidity_net, liquidity_gross, block_number"
            )
            return False

    # Calculate statistics
    total_ticks = len(snapshot_data["tick_data"])
    total_bitmap_words = len(snapshot_data["tick_bitmap"])

    insert_sql = f"""
    INSERT INTO {table_name} (
        pool_address, snapshot_block, snapshot_timestamp,
        tick_bitmap, tick_data,
        factory, asset0, asset1, fee, tick_spacing, protocol,
        total_ticks, total_bitmap_words,
        last_event_block, update_count
    ) VALUES (
        :pool_address, :snapshot_block, :snapshot_timestamp,
        :tick_bitmap, :tick_data,
        :factory, :asset0, :asset1, :fee, :tick_spacing, :protocol,
        :total_ticks, :total_bitmap_words,
        :last_event_block, 0
    )
    ON CONFLICT (pool_address) DO UPDATE SET
        snapshot_block = EXCLUDED.snapshot_block,
        snapshot_timestamp = EXCLUDED.snapshot_timestamp,
        tick_bitmap = EXCLUDED.tick_bitmap,
        tick_data = EXCLUDED.tick_data,
        total_ticks = EXCLUDED.total_ticks,
        total_bitmap_words = EXCLUDED.total_bitmap_words,
        last_event_block = EXCLUDED.last_event_block,
        last_updated = NOW(),
        update_count = {table_name}.update_count + 1;
    """

    try:
        with engine.connect() as conn:
            conn.execute(
                text(insert_sql),
                {
                    "pool_address": pool_address,
                    "snapshot_block": snapshot_data["snapshot_block"],
                    "snapshot_timestamp": snapshot_data.get(
                        "snapshot_timestamp",
                        datetime.now(UTC)
                    ),
                    "tick_bitmap": json.dumps(snapshot_data["tick_bitmap"]),
                    "tick_data": json.dumps(snapshot_data["tick_data"]),
                    "factory": snapshot_data["factory"],
                    "asset0": snapshot_data["asset0"],
                    "asset1": snapshot_data["asset1"],
                    "fee": snapshot_data["fee"],
                    "tick_spacing": snapshot_data["tick_spacing"],
                    "protocol": snapshot_data["protocol"],
                    "total_ticks": total_ticks,
                    "total_bitmap_words": total_bitmap_words,
                    "last_event_block": snapshot_data.get("last_event_block", snapshot_data["snapshot_block"]),
                },
            )
            conn.commit()
            logger.debug(f"Stored snapshot for pool {pool_address} at block {snapshot_data['snapshot_block']}")
            return True
    except Exception as e:
        logger.error(f"Error storing snapshot for pool {pool_address}: {e}")
        return False


def load_liquidity_snapshot(
    pool_address: str,
    chain_id: int = 1
) -> Optional[dict]:
    """
    Load liquidity snapshot for a specific pool.

    Args:
        pool_address: Pool address (checksummed)
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Snapshot dict or None if not found
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT
        pool_address, snapshot_block, snapshot_timestamp,
        tick_bitmap, tick_data,
        factory, asset0, asset1, fee, tick_spacing, protocol,
        total_ticks, total_bitmap_words,
        last_event_block, last_updated, update_count
    FROM {table_name}
    WHERE pool_address = :pool_address;
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(select_sql),
                {"pool_address": pool_address}
            )
            row = result.fetchone()

            if not row:
                return None

            # Convert integers in tick_bitmap and tick_data keys from strings
            tick_bitmap = {int(k): v for k, v in row.tick_bitmap.items()}
            tick_data = {int(k): v for k, v in row.tick_data.items()}

            return {
                "pool_address": row.pool_address,
                "snapshot_block": row.snapshot_block,
                "snapshot_timestamp": row.snapshot_timestamp,
                "tick_bitmap": tick_bitmap,
                "tick_data": tick_data,
                "factory": row.factory,
                "asset0": row.asset0,
                "asset1": row.asset1,
                "fee": row.fee,
                "tick_spacing": row.tick_spacing,
                "protocol": row.protocol,
                "total_ticks": row.total_ticks,
                "total_bitmap_words": row.total_bitmap_words,
                "last_event_block": row.last_event_block,
                "last_updated": row.last_updated,
                "update_count": row.update_count,
            }
    except Exception as e:
        logger.error(f"Error loading snapshot for pool {pool_address}: {e}")
        return None


def load_all_snapshots(
    chain_id: int = 1,
    after_block: int = 0,
    limit: Optional[int] = None
) -> dict[str, dict]:
    """
    Load all liquidity snapshots for a chain.

    Args:
        chain_id: Chain ID (default: 1 for Ethereum)
        after_block: Only load snapshots at or after this block
        limit: Maximum number of snapshots to load (None = all)

    Returns:
        Dict mapping pool_address to snapshot data
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT
        pool_address, snapshot_block, snapshot_timestamp,
        tick_bitmap, tick_data,
        factory, asset0, asset1, fee, tick_spacing, protocol,
        total_ticks, total_bitmap_words,
        last_event_block, last_updated, update_count
    FROM {table_name}
    WHERE snapshot_block >= :after_block
    ORDER BY snapshot_block DESC
    """

    if limit:
        select_sql += f" LIMIT {limit}"

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(select_sql),
                {"after_block": after_block}
            )

            snapshots = {}
            for row in result:
                # Convert integers in tick_bitmap and tick_data keys from strings
                tick_bitmap = {int(k): v for k, v in row.tick_bitmap.items()}
                tick_data = {int(k): v for k, v in row.tick_data.items()}

                snapshots[row.pool_address] = {
                    "pool_address": row.pool_address,
                    "snapshot_block": row.snapshot_block,
                    "snapshot_timestamp": row.snapshot_timestamp,
                    "tick_bitmap": tick_bitmap,
                    "tick_data": tick_data,
                    "factory": row.factory,
                    "asset0": row.asset0,
                    "asset1": row.asset1,
                    "fee": row.fee,
                    "tick_spacing": row.tick_spacing,
                    "protocol": row.protocol,
                    "total_ticks": row.total_ticks,
                    "total_bitmap_words": row.total_bitmap_words,
                    "last_event_block": row.last_event_block,
                    "last_updated": row.last_updated,
                    "update_count": row.update_count,
                }

            logger.info(f"Loaded {len(snapshots)} snapshots for chain {chain_id}")
            return snapshots
    except Exception as e:
        logger.error(f"Error loading all snapshots for chain {chain_id}: {e}")
        return {}


def get_snapshot_metadata(
    pool_address: str,
    chain_id: int = 1
) -> Optional[dict]:
    """
    Get metadata for a liquidity snapshot (without the large JSONB fields).

    Args:
        pool_address: Pool address (checksummed)
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Metadata dict or None if not found
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT
        pool_address, snapshot_block, snapshot_timestamp,
        factory, asset0, asset1, fee, tick_spacing, protocol,
        total_ticks, total_bitmap_words,
        last_event_block, last_updated, update_count
    FROM {table_name}
    WHERE pool_address = :pool_address;
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(select_sql),
                {"pool_address": pool_address}
            )
            row = result.fetchone()

            if not row:
                return None

            return {
                "pool_address": row.pool_address,
                "snapshot_block": row.snapshot_block,
                "snapshot_timestamp": row.snapshot_timestamp,
                "factory": row.factory,
                "asset0": row.asset0,
                "asset1": row.asset1,
                "fee": row.fee,
                "tick_spacing": row.tick_spacing,
                "protocol": row.protocol,
                "total_ticks": row.total_ticks,
                "total_bitmap_words": row.total_bitmap_words,
                "last_event_block": row.last_event_block,
                "last_updated": row.last_updated,
                "update_count": row.update_count,
            }
    except Exception as e:
        logger.error(f"Error getting metadata for pool {pool_address}: {e}")
        return None


def get_snapshot_statistics(chain_id: int = 1) -> dict:
    """
    Get statistics about liquidity snapshots for a chain.

    Args:
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Statistics dict with counts, averages, and other metrics
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    stats_sql = f"""
    SELECT
        COUNT(*) as total_snapshots,
        MAX(snapshot_block) as latest_snapshot_block,
        MIN(snapshot_block) as earliest_snapshot_block,
        AVG(total_ticks) as avg_ticks_per_pool,
        AVG(total_bitmap_words) as avg_bitmap_words_per_pool,
        COUNT(DISTINCT protocol) as protocol_count,
        EXTRACT(EPOCH FROM (NOW() - MAX(last_updated)))/3600 as hours_since_last_update
    FROM {table_name};
    """

    protocols_sql = f"""
    SELECT protocol, COUNT(*) as pool_count
    FROM {table_name}
    GROUP BY protocol
    ORDER BY pool_count DESC;
    """

    try:
        with engine.connect() as conn:
            # Get general statistics
            result = conn.execute(text(stats_sql))
            row = result.fetchone()

            stats = {
                "chain_id": chain_id,
                "total_snapshots": row.total_snapshots or 0,
                "latest_snapshot_block": row.latest_snapshot_block or 0,
                "earliest_snapshot_block": row.earliest_snapshot_block or 0,
                "avg_ticks_per_pool": float(row.avg_ticks_per_pool or 0),
                "avg_bitmap_words_per_pool": float(row.avg_bitmap_words_per_pool or 0),
                "protocol_count": row.protocol_count or 0,
                "hours_since_last_update": float(row.hours_since_last_update or 0),
            }

            # Get per-protocol breakdown
            result = conn.execute(text(protocols_sql))
            stats["snapshots_by_protocol"] = {
                row.protocol: row.pool_count
                for row in result
            }

            return stats
    except Exception as e:
        logger.error(f"Error getting snapshot statistics for chain {chain_id}: {e}")
        return {
            "chain_id": chain_id,
            "error": str(e)
        }


def update_snapshot(
    pool_address: str,
    updated_data: dict,
    chain_id: int = 1
) -> bool:
    """
    Update an existing liquidity snapshot.

    This is a convenience function that loads the existing snapshot,
    merges the updated data, and stores it back.

    Args:
        pool_address: Pool address (checksummed)
        updated_data: Dict with fields to update
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        True if updated successfully, False otherwise
    """
    # Load existing snapshot
    existing = load_liquidity_snapshot(pool_address, chain_id)
    if not existing:
        logger.error(f"Cannot update: snapshot not found for pool {pool_address}")
        return False

    # Merge updated data
    merged = {**existing, **updated_data}

    # Store updated snapshot
    return store_liquidity_snapshot(pool_address, merged, chain_id)


def delete_snapshot(pool_address: str, chain_id: int = 1) -> bool:
    """
    Delete a liquidity snapshot for a pool.

    Args:
        pool_address: Pool address (checksummed)
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        True if deleted successfully, False otherwise
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE pool_address = :pool_address;
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(delete_sql),
                {"pool_address": pool_address}
            )
            conn.commit()
            logger.info(f"Deleted snapshot for pool {pool_address}")
            return result.rowcount > 0
    except Exception as e:
        logger.error(f"Error deleting snapshot for pool {pool_address}: {e}")
        return False
