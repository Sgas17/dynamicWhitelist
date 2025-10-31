"""
TimescaleDB storage implementation for time-series transfer data.

This module contains all TimescaleDB-specific functionality including:
- Hypertable creation and management
- Time-series data storage and retrieval
- Compression and retention policies
- Schema migrations

Table naming follows the pattern: network_{chain_id}_token_raw_transfers
and network_{chain_id}_token_hourly_transfers to match the rest of the database.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Default chain ID for Ethereum
DEFAULT_CHAIN_ID = 1


def get_table_names(chain_id: int = DEFAULT_CHAIN_ID) -> Dict[str, str]:
    """
    Get chain-specific table names.

    Args:
        chain_id: Blockchain chain ID (1=Ethereum, 8453=Base, 42161=Arbitrum)

    Returns:
        Dict with 'raw' and 'hourly' table names
    """
    return {
        'raw': f'network_{chain_id}_token_raw_transfers',
        'hourly': f'network_{chain_id}_token_hourly_transfers'
    }


def get_timescale_engine():
    """Create SQLAlchemy engine for TimescaleDB."""
    database_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    
    return create_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=3600
    )


def migrate_table_schema(chain_id: int = DEFAULT_CHAIN_ID):
    """Apply any necessary schema migrations."""
    logger.info(f"Checking for schema migrations for chain {chain_id}...")

    tables = get_table_names(chain_id)
    engine = get_timescale_engine()
    with engine.connect() as conn:
        # Check if hourly transfers table exists first
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            )
        """), {"table_name": tables['hourly']}).scalar()

        if table_exists:
            # Check if avg_transfers_24h column exists in hourly table
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND column_name = 'avg_transfers_24h'
            """), {"table_name": tables['hourly']})

            if not result.fetchone():
                logger.info(f"Adding avg_transfers_24h column to {tables['hourly']}")
                conn.execute(text(f"""
                    ALTER TABLE {tables['hourly']}
                    ADD COLUMN avg_transfers_24h NUMERIC(10, 2)
                """))
                conn.commit()

            # Check if mev_transfers column exists in hourly table
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND column_name = 'mev_transfers'
            """), {"table_name": tables['hourly']})

            if not result.fetchone():
                logger.info(f"Adding mev_transfers column to {tables['hourly']}")
                conn.execute(text(f"""
                    ALTER TABLE {tables['hourly']}
                    ADD COLUMN mev_transfers INTEGER DEFAULT 0
                """))
                conn.commit()

        # Check raw table for mev_transfers column
        raw_table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            )
        """), {"table_name": tables['raw']}).scalar()

        if raw_table_exists:
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND column_name = 'mev_transfers'
            """), {"table_name": tables['raw']})

            if not result.fetchone():
                logger.info(f"Adding mev_transfers column to {tables['raw']}")
                conn.execute(text(f"""
                    ALTER TABLE {tables['raw']}
                    ADD COLUMN mev_transfers INTEGER DEFAULT 0
                """))
                conn.commit()

        logger.info(f"Schema is up to date for chain {chain_id}")


def setup_timescale_tables(chain_id: int = DEFAULT_CHAIN_ID):
    """Create and configure TimescaleDB hypertables with compression and retention."""
    logger.info(f"Setting up TimescaleDB tables for chain {chain_id}")

    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    # Apply any schema migrations first
    migrate_table_schema(chain_id)

    with engine.connect() as conn:
        # Create tables if they don't exist
        # Note: Since we're using dynamic table names, we can't use Base.metadata.create_all()
        # We'll create tables directly with SQL

        # Create raw transfers table
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {tables['raw']} (
                timestamp TIMESTAMPTZ NOT NULL,
                token_address TEXT NOT NULL,
                transfer_count INTEGER NOT NULL,
                unique_senders INTEGER NOT NULL,
                unique_receivers INTEGER NOT NULL,
                total_volume NUMERIC,
                mev_transfers INTEGER DEFAULT 0,
                PRIMARY KEY (timestamp, token_address)
            )
        """))

        # Create hourly transfers table
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {tables['hourly']} (
                hour_timestamp TIMESTAMPTZ NOT NULL,
                token_address TEXT NOT NULL,
                transfer_count INTEGER NOT NULL,
                unique_senders INTEGER NOT NULL,
                unique_receivers INTEGER NOT NULL,
                total_volume NUMERIC,
                mev_transfers INTEGER DEFAULT 0,
                avg_transfers_24h NUMERIC(10, 2),
                PRIMARY KEY (hour_timestamp, token_address)
            )
        """))

        # Create hypertables (idempotent operations)

        # Raw transfers hypertable (5-minute chunks)
        try:
            conn.execute(text(f"""
                SELECT create_hypertable('{tables['raw']}', 'timestamp',
                                       chunk_time_interval => INTERVAL '5 minutes',
                                       if_not_exists => TRUE)
            """))
            logger.info(f"Raw transfers hypertable created for chain {chain_id} with 5-minute chunks")
        except Exception as e:
            if "already exists" not in str(e):
                logger.error(f"Failed to create raw transfers hypertable: {e}")

        # Hourly transfers hypertable (1-hour chunks)
        try:
            conn.execute(text(f"""
                SELECT create_hypertable('{tables['hourly']}', 'hour_timestamp',
                                       chunk_time_interval => INTERVAL '1 hour',
                                       if_not_exists => TRUE)
            """))
            logger.info(f"Hourly transfers hypertable created for chain {chain_id} with 1-hour chunks")
        except Exception as e:
            if "already exists" not in str(e):
                logger.error(f"Failed to create hourly transfers hypertable: {e}")

        # Enable compression on both tables

        # Raw transfers compression (compress after 1 day)
        try:
            conn.execute(text(f"""
                ALTER TABLE {tables['raw']} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'token_address',
                    timescaledb.compress_orderby = 'timestamp DESC'
                )
            """))
            logger.info(f"Raw transfers compression enabled for chain {chain_id}")
        except Exception as e:
            if "already set" not in str(e) and "already enabled" not in str(e):
                logger.warning(f"Raw transfers compression setup failed: {e}")

        try:
            conn.execute(text(f"""
                SELECT add_compression_policy('{tables['raw']}', INTERVAL '1 day')
            """))
            logger.info(f"Raw transfers compression policy added for chain {chain_id}")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Raw transfers compression policy failed: {e}")

        # Hourly transfers compression (compress after 1 day)
        try:
            conn.execute(text(f"""
                ALTER TABLE {tables['hourly']} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'token_address',
                    timescaledb.compress_orderby = 'hour_timestamp DESC'
                )
            """))
            logger.info(f"Hourly transfers compression enabled for chain {chain_id}")
        except Exception as e:
            if "already set" not in str(e) and "already enabled" not in str(e):
                logger.warning(f"Hourly transfers compression setup failed: {e}")

        try:
            conn.execute(text(f"""
                SELECT add_compression_policy('{tables['hourly']}', INTERVAL '1 day')
            """))
            logger.info(f"Hourly transfers compression policy added for chain {chain_id}")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Hourly transfers compression policy failed: {e}")

        # Add retention policies

        # Raw data retention (5 days)
        try:
            conn.execute(text(f"""
                SELECT add_retention_policy('{tables['raw']}', INTERVAL '5 days')
            """))
            logger.info(f"Raw transfers retention policy added for chain {chain_id}")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Raw transfers retention policy failed: {e}")

        # Hourly data retention (90 days)
        try:
            conn.execute(text(f"""
                SELECT add_retention_policy('{tables['hourly']}', INTERVAL '90 days')
            """))
            logger.info(f"Hourly transfers retention policy added for chain {chain_id}")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Hourly transfers retention policy failed: {e}")

        conn.commit()
        logger.info(f"TimescaleDB tables initialized successfully for chain {chain_id}")


def store_raw_transfers(
    raw_data: List[Dict[str, Any]],
    interval_start: datetime,
    chain_id: int = DEFAULT_CHAIN_ID
):
    """
    Store raw transfer aggregations in TimescaleDB.

    Args:
        raw_data: List of aggregated transfer data per token
        interval_start: Start timestamp for this data interval
        chain_id: Blockchain chain ID
    """
    if not raw_data:
        return

    tables = get_table_names(chain_id)

    # Convert to format expected by database
    db_records = []
    for record in raw_data:
        db_records.append((
            interval_start,
            record['token_address'],
            record['transfer_count'],
            record['unique_senders'],
            record['unique_receivers'],
            record['total_volume'],
            record.get('mev_transfers', 0)  # Default to 0 if not provided
        ))

    # Use raw psycopg2 for batch insert performance
    connection_string = (
        f"host={os.getenv('DB_HOST')} "
        f"port={os.getenv('DB_PORT')} "
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')}"
    )

    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {tables['raw']}
                    (timestamp, token_address, transfer_count, unique_senders, unique_receivers, total_volume, mev_transfers)
                    VALUES %s
                    ON CONFLICT (timestamp, token_address)
                    DO UPDATE SET
                        transfer_count = EXCLUDED.transfer_count,
                        unique_senders = EXCLUDED.unique_senders,
                        unique_receivers = EXCLUDED.unique_receivers,
                        total_volume = EXCLUDED.total_volume,
                        mev_transfers = EXCLUDED.mev_transfers
                    """,
                    db_records,
                    page_size=1000
                )
                conn.commit()
                logger.info(f"Stored {len(db_records)} raw transfer records at {interval_start} for chain {chain_id}")

    except Exception as e:
        logger.error(f"Error storing raw transfers for chain {chain_id}: {e}")
        raise


def aggregate_raw_to_hourly(
    hour_timestamp: datetime,
    chain_id: int = DEFAULT_CHAIN_ID
) -> List[Dict[str, Any]]:
    """Aggregate raw 5-minute data into hourly summaries."""
    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                token_address,
                SUM(transfer_count) as transfer_count,
                SUM(unique_senders) as unique_senders,
                SUM(unique_receivers) as unique_receivers,
                SUM(total_volume) as total_volume,
                SUM(mev_transfers) as mev_transfers
            FROM {tables['raw']}
            WHERE timestamp >= :start_time AND timestamp < :end_time
            GROUP BY token_address
            HAVING SUM(transfer_count) > 0
        """), {
            "start_time": hour_timestamp,
            "end_time": hour_timestamp + timedelta(hours=1)
        })

        return [
            {
                "token_address": row.token_address,
                "transfer_count": row.transfer_count,
                "unique_senders": row.unique_senders,
                "unique_receivers": row.unique_receivers,
                "total_volume": row.total_volume,
                "mev_transfers": row.mev_transfers
            }
            for row in result
        ]


def store_hourly_transfers(
    hourly_data: List[Dict[str, Any]],
    hour_timestamp: datetime,
    chain_id: int = DEFAULT_CHAIN_ID
):
    """Store hourly aggregated transfer data."""
    if not hourly_data:
        return

    tables = get_table_names(chain_id)

    # Calculate 24-hour averages for each token
    averages = calculate_token_averages(hour_timestamp, chain_id)

    # Merge averages into hourly data
    for record in hourly_data:
        token_addr = record['token_address']
        record['avg_transfers_24h'] = averages.get(token_addr, 0)

    # Convert to database format
    db_records = []
    for record in hourly_data:
        db_records.append((
            hour_timestamp,
            record['token_address'],
            record['transfer_count'],
            record['unique_senders'],
            record['unique_receivers'],
            record['total_volume'],
            record.get('mev_transfers', 0),
            record.get('avg_transfers_24h', 0)
        ))

    # Batch insert with psycopg2
    connection_string = (
        f"host={os.getenv('DB_HOST')} "
        f"port={os.getenv('DB_PORT')} "
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')}"
    )

    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {tables['hourly']}
                    (hour_timestamp, token_address, transfer_count, unique_senders,
                     unique_receivers, total_volume, mev_transfers, avg_transfers_24h)
                    VALUES %s
                    ON CONFLICT (hour_timestamp, token_address)
                    DO UPDATE SET
                        transfer_count = EXCLUDED.transfer_count,
                        unique_senders = EXCLUDED.unique_senders,
                        unique_receivers = EXCLUDED.unique_receivers,
                        total_volume = EXCLUDED.total_volume,
                        mev_transfers = EXCLUDED.mev_transfers,
                        avg_transfers_24h = EXCLUDED.avg_transfers_24h
                    """,
                    db_records,
                    page_size=1000
                )
                conn.commit()
                logger.info(f"Stored {len(db_records)} hourly transfer records for {hour_timestamp} on chain {chain_id}")

    except Exception as e:
        logger.error(f"Error storing hourly transfers for chain {chain_id}: {e}")
        raise


def calculate_token_averages(
    current_hour: datetime,
    chain_id: int = DEFAULT_CHAIN_ID
) -> Dict[str, float]:
    """Calculate 24-hour rolling average transfer counts for tokens."""
    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    start_time = current_hour - timedelta(hours=24)

    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                token_address,
                AVG(transfer_count::float) as avg_transfers_24h
            FROM {tables['hourly']}
            WHERE hour_timestamp >= :start_time AND hour_timestamp < :end_time
            GROUP BY token_address
        """), {
            "start_time": start_time,
            "end_time": current_hour
        })

        return {
            row.token_address: float(row.avg_transfers_24h or 0)
            for row in result
        }


def get_top_tokens_by_average(
    hours_back: int = 24,
    limit: int = 100,
    chain_id: int = DEFAULT_CHAIN_ID
) -> List[Dict[str, Any]]:
    """Get tokens with highest 24-hour average transfer counts."""
    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    cutoff_time = datetime.now() - timedelta(hours=hours_back)

    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                token_address,
                avg_transfers_24h,
                MAX(hour_timestamp) as last_updated
            FROM {tables['hourly']}
            WHERE hour_timestamp >= :cutoff_time
            AND avg_transfers_24h IS NOT NULL
            GROUP BY token_address, avg_transfers_24h
            ORDER BY avg_transfers_24h DESC
            LIMIT :limit
        """), {
            "cutoff_time": cutoff_time,
            "limit": limit
        })

        return [
            {
                "token_address": row.token_address,
                "avg_transfers_24h": float(row.avg_transfers_24h),
                "last_updated": row.last_updated
            }
            for row in result
        ]


def update_token_averages(hour_timestamp: datetime, chain_id: int = DEFAULT_CHAIN_ID):
    """Update 24-hour averages for all tokens in the given hour."""
    tables = get_table_names(chain_id)
    averages = calculate_token_averages(hour_timestamp, chain_id)

    if not averages:
        return

    connection_string = (
        f"host={os.getenv('DB_HOST')} "
        f"port={os.getenv('DB_PORT')} "
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')}"
    )

    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                for token_address, avg_value in averages.items():
                    cur.execute(f"""
                        UPDATE {tables['hourly']}
                        SET avg_transfers_24h = %s
                        WHERE hour_timestamp = %s AND token_address = %s
                    """, (avg_value, hour_timestamp, token_address))

                conn.commit()
                logger.info(f"Updated averages for {len(averages)} tokens at {hour_timestamp} on chain {chain_id}")

    except Exception as e:
        logger.error(f"Error updating token averages for chain {chain_id}: {e}")
        raise


def cleanup_old_data(chain_id: int = DEFAULT_CHAIN_ID):
    """Clean up old data based on retention policies."""
    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    with engine.connect() as conn:
        # Raw data cleanup (older than 5 days)
        raw_cutoff = datetime.now() - timedelta(days=5)
        result = conn.execute(text(f"""
            DELETE FROM {tables['raw']}
            WHERE timestamp < :cutoff_time
        """), {"cutoff_time": raw_cutoff})

        logger.info(f"Cleaned up {result.rowcount} old raw transfer records for chain {chain_id}")

        # Hourly data cleanup (older than 90 days)
        hourly_cutoff = datetime.now() - timedelta(days=90)
        result = conn.execute(text(f"""
            DELETE FROM {tables['hourly']}
            WHERE hour_timestamp < :cutoff_time
        """), {"cutoff_time": hourly_cutoff})

        logger.info(f"Cleaned up {result.rowcount} old hourly transfer records for chain {chain_id}")

        conn.commit()

def cleanup_test_data(engine=None, chain_id: int = DEFAULT_CHAIN_ID):
    """Clean up test data for integration testing."""
    tables = get_table_names(chain_id)
    if engine is None:
        engine = get_timescale_engine()

    with engine.connect() as conn:
        # Clean test data from the last 2 hours
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Clean raw transfers
        result = conn.execute(text(f"""
            DELETE FROM {tables['raw']}
            WHERE timestamp >= :cutoff_time
        """), {"cutoff_time": cutoff_time})

        logger.info(f"Cleaned up {result.rowcount} test raw transfer records for chain {chain_id}")

        # Clean hourly transfers
        result = conn.execute(text(f"""
            DELETE FROM {tables['hourly']}
            WHERE hour_timestamp >= :cutoff_time
        """), {"cutoff_time": cutoff_time})

        logger.info(f"Cleaned up {result.rowcount} test hourly transfer records for chain {chain_id}")

        conn.commit()


def get_database_stats(chain_id: int = DEFAULT_CHAIN_ID) -> Dict[str, Any]:
    """Get TimescaleDB database statistics."""
    tables = get_table_names(chain_id)
    engine = get_timescale_engine()

    with engine.connect() as conn:
        # Table sizes - use dynamic table names
        size_result = conn.execute(text("""
            SELECT
                schemaname,
                tablename as table,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size_pretty,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables
            WHERE tablename IN (:raw_table, :hourly_table)
            ORDER BY size_bytes DESC
        """), {"raw_table": tables['raw'], "hourly_table": tables['hourly']})

        table_sizes = [
            {
                "schemaname": row.schemaname,
                "table": row.table,
                "size_pretty": row.size_pretty,
                "size_bytes": row.size_bytes
            }
            for row in size_result
        ]

        # Raw data stats
        raw_stats = None
        try:
            raw_stats = conn.execute(text(f"""
                SELECT
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_record,
                    MAX(timestamp) as latest_record
                FROM {tables['raw']}
            """)).fetchone()
        except Exception:
            pass  # Table doesn't exist yet

        # Hourly data stats
        hourly_stats = None
        try:
            hourly_stats = conn.execute(text(f"""
                SELECT
                    COUNT(*) as total_records,
                    MIN(hour_timestamp) as earliest_record,
                    MAX(hour_timestamp) as latest_record
                FROM {tables['hourly']}
            """)).fetchone()
        except Exception:
            pass  # Table doesn't exist yet

        # Compression stats
        compression_stats = []
        try:
            compression_result = conn.execute(text("""
                SELECT
                    hypertable_name as table,
                    compression_enabled as enabled,
                    COALESCE(uncompressed_heap_size::float / NULLIF(compressed_heap_size, 0), 1.0) as compression_ratio
                FROM timescaledb_information.hypertables h
                LEFT JOIN timescaledb_information.compression_settings cs ON h.hypertable_name = cs.hypertable_name
                WHERE h.hypertable_name IN (:raw_table, :hourly_table)
            """), {"raw_table": tables['raw'], "hourly_table": tables['hourly']})
            compression_stats = [
                {
                    "table": row.table,
                    "enabled": row.enabled,
                    "compression_ratio": float(row.compression_ratio) if row.compression_ratio else 1.0
                }
                for row in compression_result
            ]
        except Exception as e:
            logger.warning(f"Could not get compression stats for chain {chain_id}: {e}")

        return {
            "chain_id": chain_id,
            "table_sizes": table_sizes,
            "raw_data": {
                "total_records": raw_stats.total_records,
                "earliest_record": raw_stats.earliest_record,
                "latest_record": raw_stats.latest_record,
                "retention_days": 5
            } if raw_stats else None,
            "hourly_data": {
                "total_records": hourly_stats.total_records,
                "earliest_record": hourly_stats.earliest_record,
                "latest_record": hourly_stats.latest_record,
                "retention_days": 90
            } if hourly_stats else None,
            "compression": compression_stats
        }