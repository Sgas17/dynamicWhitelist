"""
TimescaleDB Storage Layer for Liquidity Update Events

This module provides storage and query operations for liquidity update events
(Mint/Burn) stored in TimescaleDB as time-series data.

Schema:
    network_{chain_id}_liquidity_updates:
        - event_time (TIMESTAMPTZ) - Hypertable partition key
        - block_number (BIGINT)
        - pool_address (TEXT)
        - event_type (TEXT) - "Mint" or "Burn"
        - tick_lower, tick_upper (INTEGER)
        - liquidity_delta (NUMERIC) - Signed liquidity change
        - Transaction context + amounts
"""

import logging
from datetime import datetime, UTC
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.core.storage.timescaledb import get_timescale_engine

logger = logging.getLogger(__name__)


def get_table_name(chain_id: int) -> str:
    """Get liquidity updates table name for chain."""
    return f"network_{chain_id}_liquidity_updates"


def setup_liquidity_updates_table(engine: Engine, chain_id: int) -> bool:
    """
    Create liquidity updates hypertable for the specified chain.

    Args:
        engine: SQLAlchemy engine
        chain_id: Chain ID (e.g., 1 for Ethereum mainnet)

    Returns:
        True if table was created or already exists, False on error
    """
    table_name = get_table_name(chain_id)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Time-series primary key
        event_time TIMESTAMPTZ NOT NULL,
        block_number BIGINT NOT NULL,
        transaction_index INTEGER NOT NULL,
        log_index INTEGER NOT NULL,

        -- Pool identification
        pool_address TEXT NOT NULL,

        -- Event data
        event_type TEXT NOT NULL,          -- "Mint" or "Burn"
        tick_lower INTEGER NOT NULL,
        tick_upper INTEGER NOT NULL,
        liquidity_delta NUMERIC(78, 0) NOT NULL,  -- Positive for Mint, negative for Burn

        -- Transaction context
        transaction_hash TEXT NOT NULL,
        sender_address TEXT,               -- Who initiated the event

        -- Amounts (for analytics)
        amount0 NUMERIC(78, 0),
        amount1 NUMERIC(78, 0),

        -- Metadata
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """

    # Convert to hypertable
    create_hypertable_sql = f"""
    SELECT create_hypertable(
        '{table_name}',
        'event_time',
        chunk_time_interval => INTERVAL '7 days',
        if_not_exists => TRUE
    );
    """

    # Create indexes
    create_indexes_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_pool_time
        ON {table_name}(pool_address, event_time DESC);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_block
        ON {table_name}(block_number DESC);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_tx
        ON {table_name}(transaction_hash);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_pool_block
        ON {table_name}(pool_address, block_number DESC);
    """

    # Compression policy (compress after 90 days)
    # Note: Requires compression to be enabled on hypertable first
    enable_compression_sql = f"""
    ALTER TABLE {table_name} SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pool_address'
    );
    """

    compression_sql = f"""
    SELECT add_compression_policy(
        '{table_name}',
        INTERVAL '90 days',
        if_not_exists => TRUE
    );
    """

    # Retention policy (keep 1 year of detailed events)
    retention_sql = f"""
    SELECT add_retention_policy(
        '{table_name}',
        INTERVAL '365 days',
        if_not_exists => TRUE
    );
    """

    try:
        with engine.connect() as conn:
            # Create table
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info(f"Table {table_name} created successfully")

            # Convert to hypertable
            try:
                conn.execute(text(create_hypertable_sql))
                conn.commit()
                logger.info(f"Converted {table_name} to hypertable")
            except Exception as e:
                # Table might already be a hypertable
                if "already a hypertable" in str(e).lower():
                    logger.info(f"{table_name} is already a hypertable")
                else:
                    logger.warning(f"Could not convert to hypertable: {e}")

            # Create indexes
            for index_sql in create_indexes_sql.split(";"):
                if index_sql.strip():
                    conn.execute(text(index_sql))
            conn.commit()
            logger.info(f"Indexes created for {table_name}")

            # Enable compression on hypertable (optional, requires TimescaleDB license for some versions)
            try:
                conn.execute(text(enable_compression_sql))
                conn.commit()
                logger.info(f"Compression enabled for {table_name}")

                # Add compression policy
                try:
                    conn.execute(text(compression_sql))
                    conn.commit()
                    logger.info(f"Compression policy added for {table_name}")
                except Exception as e:
                    conn.rollback()
                    logger.warning(f"Could not add compression policy: {e}")
            except Exception as e:
                conn.rollback()
                logger.info(f"Compression not enabled (may require TimescaleDB license): {e}")

            # Add retention policy (optional)
            try:
                conn.execute(text(retention_sql))
                conn.commit()
                logger.info(f"Retention policy added for {table_name}")
            except Exception as e:
                conn.rollback()
                logger.info(f"Retention policy not added (optional): {e}")

            return True
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        return False


def store_liquidity_update(
    update: dict,
    chain_id: int = 1
) -> bool:
    """
    Store a single liquidity update event.

    Args:
        update: Update dict with required fields
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        True if stored successfully, False otherwise
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    # Validate required fields
    required_fields = [
        "event_time", "block_number", "transaction_index", "log_index",
        "pool_address", "event_type", "tick_lower", "tick_upper",
        "liquidity_delta", "transaction_hash"
    ]
    for field in required_fields:
        if field not in update:
            logger.error(f"Missing required field: {field}")
            return False

    # Validate event_type
    if update["event_type"] not in ["Mint", "Burn"]:
        logger.error(f"Invalid event_type: {update['event_type']}")
        return False

    insert_sql = f"""
    INSERT INTO {table_name} (
        event_time, block_number, transaction_index, log_index,
        pool_address, event_type, tick_lower, tick_upper, liquidity_delta,
        transaction_hash, sender_address, amount0, amount1
    ) VALUES (
        :event_time, :block_number, :transaction_index, :log_index,
        :pool_address, :event_type, :tick_lower, :tick_upper, :liquidity_delta,
        :transaction_hash, :sender_address, :amount0, :amount1
    );
    """

    try:
        with engine.connect() as conn:
            conn.execute(
                text(insert_sql),
                {
                    "event_time": update["event_time"],
                    "block_number": update["block_number"],
                    "transaction_index": update["transaction_index"],
                    "log_index": update["log_index"],
                    "pool_address": update["pool_address"],
                    "event_type": update["event_type"],
                    "tick_lower": update["tick_lower"],
                    "tick_upper": update["tick_upper"],
                    "liquidity_delta": update["liquidity_delta"],
                    "transaction_hash": update["transaction_hash"],
                    "sender_address": update.get("sender_address"),
                    "amount0": update.get("amount0"),
                    "amount1": update.get("amount1"),
                },
            )
            conn.commit()
            logger.debug(
                f"Stored {update['event_type']} event for pool {update['pool_address']} "
                f"at block {update['block_number']}"
            )
            return True
    except Exception as e:
        logger.error(f"Error storing liquidity update: {e}")
        return False


def store_liquidity_updates_batch(
    updates: List[dict],
    chain_id: int = 1,
    batch_size: int = 1000
) -> int:
    """
    Store multiple liquidity update events in batches.

    Args:
        updates: List of update dicts
        chain_id: Chain ID (default: 1 for Ethereum)
        batch_size: Number of records to insert per batch

    Returns:
        Number of successfully stored updates
    """
    if not updates:
        return 0

    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    insert_sql = f"""
    INSERT INTO {table_name} (
        event_time, block_number, transaction_index, log_index,
        pool_address, event_type, tick_lower, tick_upper, liquidity_delta,
        transaction_hash, sender_address, amount0, amount1
    ) VALUES (
        :event_time, :block_number, :transaction_index, :log_index,
        :pool_address, :event_type, :tick_lower, :tick_upper, :liquidity_delta,
        :transaction_hash, :sender_address, :amount0, :amount1
    );
    """

    stored_count = 0
    try:
        with engine.connect() as conn:
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]

                # Validate and prepare batch
                prepared_batch = []
                for update in batch:
                    # Validate required fields
                    required_fields = [
                        "event_time", "block_number", "transaction_index", "log_index",
                        "pool_address", "event_type", "tick_lower", "tick_upper",
                        "liquidity_delta", "transaction_hash"
                    ]
                    if not all(field in update for field in required_fields):
                        logger.warning(f"Skipping invalid update: {update}")
                        continue

                    if update["event_type"] not in ["Mint", "Burn"]:
                        logger.warning(f"Skipping invalid event_type: {update['event_type']}")
                        continue

                    prepared_batch.append({
                        "event_time": update["event_time"],
                        "block_number": update["block_number"],
                        "transaction_index": update["transaction_index"],
                        "log_index": update["log_index"],
                        "pool_address": update["pool_address"],
                        "event_type": update["event_type"],
                        "tick_lower": update["tick_lower"],
                        "tick_upper": update["tick_upper"],
                        "liquidity_delta": update["liquidity_delta"],
                        "transaction_hash": update["transaction_hash"],
                        "sender_address": update.get("sender_address"),
                        "amount0": update.get("amount0"),
                        "amount1": update.get("amount1"),
                    })

                # Execute batch insert
                if prepared_batch:
                    conn.execute(text(insert_sql), prepared_batch)
                    conn.commit()
                    stored_count += len(prepared_batch)

                    if (i // batch_size) % 10 == 0:  # Log every 10 batches
                        logger.info(f"Stored {stored_count}/{len(updates)} updates...")

        logger.info(f"Successfully stored {stored_count} liquidity updates")
        return stored_count
    except Exception as e:
        logger.error(f"Error storing batch updates: {e}")
        return stored_count


def get_updates_since_block(
    pool_address: str,
    after_block: int,
    chain_id: int = 1,
    limit: Optional[int] = None
) -> List[dict]:
    """
    Get liquidity updates for a pool since a specific block.

    Args:
        pool_address: Pool address (checksummed)
        after_block: Get updates after this block number
        chain_id: Chain ID (default: 1 for Ethereum)
        limit: Maximum number of updates to return

    Returns:
        List of update dicts ordered by block_number ASC
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT
        event_time, block_number, transaction_index, log_index,
        pool_address, event_type, tick_lower, tick_upper, liquidity_delta,
        transaction_hash, sender_address, amount0, amount1
    FROM {table_name}
    WHERE pool_address = :pool_address
      AND block_number > :after_block
    ORDER BY block_number ASC, transaction_index ASC, log_index ASC
    """

    if limit:
        select_sql += f" LIMIT {limit}"

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(select_sql),
                {"pool_address": pool_address, "after_block": after_block}
            )

            updates = []
            for row in result:
                updates.append({
                    "event_time": row.event_time,
                    "block_number": row.block_number,
                    "transaction_index": row.transaction_index,
                    "log_index": row.log_index,
                    "pool_address": row.pool_address,
                    "event_type": row.event_type,
                    "tick_lower": row.tick_lower,
                    "tick_upper": row.tick_upper,
                    "liquidity_delta": int(row.liquidity_delta),
                    "transaction_hash": row.transaction_hash,
                    "sender_address": row.sender_address,
                    "amount0": int(row.amount0) if row.amount0 else None,
                    "amount1": int(row.amount1) if row.amount1 else None,
                })

            logger.debug(
                f"Retrieved {len(updates)} updates for pool {pool_address} "
                f"since block {after_block}"
            )
            return updates
    except Exception as e:
        logger.error(f"Error getting updates since block {after_block}: {e}")
        return []


def get_updates_in_range(
    pool_address: str,
    start_block: int,
    end_block: int,
    chain_id: int = 1,
    limit: Optional[int] = None
) -> List[dict]:
    """
    Get liquidity updates for a pool in a specific block range.

    Args:
        pool_address: Pool address (checksummed)
        start_block: Start block number (inclusive)
        end_block: End block number (inclusive)
        chain_id: Chain ID (default: 1 for Ethereum)
        limit: Maximum number of updates to return

    Returns:
        List of update dicts ordered by block_number ASC
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT
        event_time, block_number, transaction_index, log_index,
        pool_address, event_type, tick_lower, tick_upper, liquidity_delta,
        transaction_hash, sender_address, amount0, amount1
    FROM {table_name}
    WHERE pool_address = :pool_address
      AND block_number >= :start_block
      AND block_number <= :end_block
    ORDER BY block_number ASC, transaction_index ASC, log_index ASC
    """

    if limit:
        select_sql += f" LIMIT {limit}"

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(select_sql),
                {
                    "pool_address": pool_address,
                    "start_block": start_block,
                    "end_block": end_block
                }
            )

            updates = []
            for row in result:
                updates.append({
                    "event_time": row.event_time,
                    "block_number": row.block_number,
                    "transaction_index": row.transaction_index,
                    "log_index": row.log_index,
                    "pool_address": row.pool_address,
                    "event_type": row.event_type,
                    "tick_lower": row.tick_lower,
                    "tick_upper": row.tick_upper,
                    "liquidity_delta": int(row.liquidity_delta),
                    "transaction_hash": row.transaction_hash,
                    "sender_address": row.sender_address,
                    "amount0": int(row.amount0) if row.amount0 else None,
                    "amount1": int(row.amount1) if row.amount1 else None,
                })

            logger.debug(
                f"Retrieved {len(updates)} updates for pool {pool_address} "
                f"in range {start_block}-{end_block}"
            )
            return updates
    except Exception as e:
        logger.error(
            f"Error getting updates in range {start_block}-{end_block}: {e}"
        )
        return []


def get_last_processed_block(
    pool_address: str,
    chain_id: int = 1
) -> int:
    """
    Get the last processed block number for a pool.

    Args:
        pool_address: Pool address (checksummed)
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Last block number with events, or 0 if no events found
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    select_sql = f"""
    SELECT MAX(block_number) as last_block
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
            return int(row.last_block) if row.last_block else 0
    except Exception as e:
        logger.error(f"Error getting last processed block: {e}")
        return 0


def get_update_statistics(chain_id: int = 1) -> dict:
    """
    Get statistics about liquidity updates for a chain.

    Args:
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Statistics dict with counts and metrics
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    stats_sql = f"""
    SELECT
        COUNT(*) as total_updates,
        COUNT(DISTINCT pool_address) as unique_pools,
        COUNT(CASE WHEN event_type = 'Mint' THEN 1 END) as mint_count,
        COUNT(CASE WHEN event_type = 'Burn' THEN 1 END) as burn_count,
        MAX(block_number) as latest_block,
        MIN(block_number) as earliest_block,
        EXTRACT(EPOCH FROM (NOW() - MAX(event_time)))/3600 as hours_since_last_event
    FROM {table_name};
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(stats_sql))
            row = result.fetchone()

            return {
                "chain_id": chain_id,
                "total_updates": row.total_updates or 0,
                "unique_pools": row.unique_pools or 0,
                "mint_count": row.mint_count or 0,
                "burn_count": row.burn_count or 0,
                "latest_block": row.latest_block or 0,
                "earliest_block": row.earliest_block or 0,
                "hours_since_last_event": float(row.hours_since_last_event or 0),
            }
    except Exception as e:
        logger.error(f"Error getting update statistics: {e}")
        return {
            "chain_id": chain_id,
            "error": str(e)
        }


def delete_updates_before_block(
    block_number: int,
    chain_id: int = 1
) -> int:
    """
    Delete liquidity updates before a specific block.

    Useful for manual cleanup or testing.

    Args:
        block_number: Delete updates before this block
        chain_id: Chain ID (default: 1 for Ethereum)

    Returns:
        Number of deleted rows
    """
    table_name = get_table_name(chain_id)
    engine = get_timescale_engine()

    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE block_number < :block_number;
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(delete_sql),
                {"block_number": block_number}
            )
            conn.commit()
            deleted = result.rowcount
            logger.info(f"Deleted {deleted} updates before block {block_number}")
            return deleted
    except Exception as e:
        logger.error(f"Error deleting updates: {e}")
        return 0
