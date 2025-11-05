"""
PostgreSQL pool storage operations.

Moved from utils/database/database_helper.py for consistency with storage architecture.
Maintains legacy column ordering for compatibility with code using row numbers.

KISS: Simple CRUD operations for DEX pool data.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

# Import config system
from ...config.manager import ConfigManager

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_table_name(chain_id: int) -> str:
    """Get pool table name for chain."""
    return f"network_{chain_id}_dex_pools_cryo"


def get_database_engine():
    """Get PostgreSQL database engine."""
    db_uri = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_uri)


def setup_pools_table(engine, chain_id: int) -> bool:
    """
    Create the network_{chain_id}_dex_pools table if it doesn't exist.

    Maintains legacy column order for compatibility with code using row numbers.
    Legacy columns 1-11, new columns 12-17 added at end.
    """
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = :table_name
                );
            """),
            {"table_name": table_name},
        ).scalar()

        if not table_exists:
            logger.info(f"Creating table {table_name}...")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                -- LEGACY COLUMNS (maintain exact order for row number compatibility)
                address TEXT PRIMARY KEY,        -- Column 1 (pool_address)
                factory TEXT NOT NULL,           -- Column 2 (factory_address)
                asset0 TEXT NOT NULL,            -- Column 3
                asset1 TEXT NOT NULL,            -- Column 4
                asset2 TEXT,                     -- Column 5 (NULL for V3, used in V4)
                asset3 TEXT,                     -- Column 6 (NULL for V3, used in V4)
                creation_block BIGINT NOT NULL,  -- Column 7 (BIGINT for large blocks)
                fee INTEGER NOT NULL,            -- Column 8
                additional_data JSONB,           -- Column 9 (JSONB instead of JSON)
                priority INTEGER,                -- Column 10 (not used but kept)
                tick_spacing INTEGER,            -- Column 11 (NULL for V2, NOT NULL for V3/V4)

                -- NEW COLUMNS (added at end to preserve row number access)
                creation_timestamp TIMESTAMPTZ,  -- Column 12
                creation_tx_hash TEXT,           -- Column 13
                is_blacklisted BOOLEAN DEFAULT FALSE,  -- Column 14
                blacklist_reason TEXT,           -- Column 15
                is_active BOOLEAN DEFAULT TRUE,  -- Column 16
                last_updated TIMESTAMPTZ DEFAULT NOW()  -- Column 17
            );

            -- Indexes for performance
            CREATE INDEX idx_{table_name}_factory ON {table_name}(factory);
            CREATE INDEX idx_{table_name}_asset0 ON {table_name}(asset0);
            CREATE INDEX idx_{table_name}_asset1 ON {table_name}(asset1);
            CREATE INDEX idx_{table_name}_creation_block ON {table_name}(creation_block);
            CREATE INDEX idx_{table_name}_active
                ON {table_name}(is_active) WHERE is_active = TRUE;
            CREATE INDEX idx_{table_name}_blacklisted
                ON {table_name}(is_blacklisted) WHERE is_blacklisted = TRUE;
            CREATE INDEX idx_{table_name}_additional_data
                ON {table_name} USING GIN (additional_data);
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info(f"Table {table_name} created successfully")
        else:
            # Table exists - check if new columns need to be added
            _add_new_columns_if_missing(conn, table_name)
            logger.info(f"Table {table_name} exists and is ready for use")

        return True


def _add_new_columns_if_missing(conn, table_name: str):
    """Add new columns to existing table if they don't exist."""
    new_columns = [
        ("creation_timestamp", "TIMESTAMPTZ"),
        ("creation_tx_hash", "TEXT"),
        ("is_blacklisted", "BOOLEAN DEFAULT FALSE"),
        ("blacklist_reason", "TEXT"),
        ("is_active", "BOOLEAN DEFAULT TRUE"),
        ("last_updated", "TIMESTAMPTZ DEFAULT NOW()"),
    ]

    for col_name, col_type in new_columns:
        # Check if column exists
        result = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name AND column_name = :col_name
            """),
            {"table_name": table_name, "col_name": col_name},
        )

        if not result.fetchone():
            logger.info(f"Adding column {col_name} to {table_name}")
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
            )
            conn.commit()


def safe_hex_or_str(val):
    """Safely convert bytes to hex or any value to string, handling None."""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val.hex()
    return str(val)


def normalize_address(address) -> Optional[str]:
    """
    Normalize Ethereum address to lowercase for consistent storage.

    Args:
        address: Address as string, bytes, or None

    Returns:
        Lowercase address string or None
    """
    if address is None:
        return None
    if isinstance(address, bytes):
        return address.hex().lower()
    return str(address).lower()


# ============================================================================
# CREATE Operations
# ============================================================================


def store_pools_to_database(pool_data: List[Dict], chain_id: int = None):
    """
    Store pool data to the correct network_{chain_id}_dex_pools table using bulk insert.

    All addresses are normalized to lowercase for consistent storage and querying.

    Args:
        pool_data: List of pool dictionaries
        chain_id: Chain ID (defaults to CHAIN_ID env var)
    """
    if not pool_data:
        logger.info("No pool data to store")
        return

    engine = get_database_engine()
    if chain_id is None:
        chain_id = int(os.getenv("CHAIN_ID", "1"))
    table_name = get_table_name(chain_id)

    # Check if table exists first
    if not setup_pools_table(engine, chain_id):
        logger.error(f"Cannot store data - table {table_name} does not exist")
        return

    # Prepare data for bulk insert with normalized lowercase addresses
    values = []
    for pool in pool_data:
        if isinstance(pool, dict) and "address" in pool:
            values.append(
                (
                    normalize_address(pool["address"]),  # Column 1 - LOWERCASE
                    normalize_address(pool.get("factory")),  # Column 2 - LOWERCASE
                    normalize_address(pool.get("asset0")),  # Column 3 - LOWERCASE
                    normalize_address(pool.get("asset1")),  # Column 4 - LOWERCASE
                    normalize_address(pool.get("asset2")),  # Column 5 - LOWERCASE
                    normalize_address(pool.get("asset3")),  # Column 6 - LOWERCASE
                    pool.get("creation_block"),  # Column 7
                    pool.get("fee"),  # Column 8
                    json.dumps(pool.get("additional_data"))
                    if pool.get("additional_data")
                    else None,  # Column 9
                    pool.get("priority"),  # Column 10
                    pool.get("tick_spacing"),  # Column 11
                    # New columns (optional)
                    pool.get("creation_timestamp"),  # Column 12
                    pool.get("creation_tx_hash"),  # Column 13
                    pool.get("is_blacklisted", False),  # Column 14
                    pool.get("blacklist_reason"),  # Column 15
                    pool.get("is_active", True),  # Column 16
                )
            )

    if not values:
        logger.info("No valid pool data to insert")
        return

    # Deduplicate values by address
    seen_addresses = set()
    deduplicated_values = []
    duplicates_removed = 0

    for value_tuple in values:
        address = value_tuple[0]
        if address not in seen_addresses:
            seen_addresses.add(address)
            deduplicated_values.append(value_tuple)
        else:
            duplicates_removed += 1

    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate pool addresses")

    values = deduplicated_values

    # Use bulk insert with ON CONFLICT for upsert behavior
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()

        execute_values(
            cursor,
            f"""
            INSERT INTO {table_name}
            (address, factory, asset0, asset1, asset2, asset3, creation_block, fee,
             additional_data, priority, tick_spacing, creation_timestamp, creation_tx_hash,
             is_blacklisted, blacklist_reason, is_active)
            VALUES %s
            ON CONFLICT (address) DO UPDATE SET
                factory = EXCLUDED.factory,
                asset0 = EXCLUDED.asset0,
                asset1 = EXCLUDED.asset1,
                asset2 = EXCLUDED.asset2,
                asset3 = EXCLUDED.asset3,
                creation_block = EXCLUDED.creation_block,
                fee = EXCLUDED.fee,
                additional_data = EXCLUDED.additional_data,
                priority = EXCLUDED.priority,
                tick_spacing = EXCLUDED.tick_spacing,
                creation_timestamp = EXCLUDED.creation_timestamp,
                creation_tx_hash = EXCLUDED.creation_tx_hash,
                is_blacklisted = EXCLUDED.is_blacklisted,
                blacklist_reason = EXCLUDED.blacklist_reason,
                is_active = EXCLUDED.is_active,
                last_updated = NOW()
            """,
            values,
            template=None,
            page_size=1000,
        )
        conn.commit()
        cursor.close()
        logger.info(f"Successfully stored {len(values)} pools to {table_name}")
    finally:
        conn.close()


# ============================================================================
# READ Operations
# ============================================================================


def get_pool(pool_address: str, chain_id: int) -> Optional[Dict]:
    """Get pool by address."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        result = conn.execute(
            text(f"""
                SELECT *
                FROM {table_name}
                WHERE address = :pool_address
            """),
            {"pool_address": pool_address},
        )
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return None


def get_pools_by_token(
    token_address: str, chain_id: int, factory: Optional[str] = None
) -> List[Dict]:
    """
    Get pools that contain a specific token.

    Args:
        token_address: Token address to search for
        chain_id: Chain ID
        factory: Optional factory address to filter by protocol
    """
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    query = f"""
        SELECT address, fee, asset0, asset1, creation_block, factory, tick_spacing
        FROM {table_name}
        WHERE (asset0 = :token_address OR asset1 = :token_address)
          AND is_active = TRUE
          AND is_blacklisted = FALSE
    """

    params = {"token_address": token_address}

    if factory:
        query += " AND factory = :factory"
        params["factory"] = factory

    query += " ORDER BY creation_block DESC"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)

        pools = []
        for row in result:
            pools.append(
                {
                    "address": row.address,
                    "fee": row.fee,
                    "asset0": row.asset0,
                    "asset1": row.asset1,
                    "creation_block": row.creation_block,
                    "factory": row.factory,
                    "tick_spacing": row.tick_spacing,
                }
            )

        return pools


def get_pools_by_factory(
    factory_address: str, chain_id: int, active_only: bool = True
) -> List[Dict]:
    """Get all pools for a specific factory (protocol)."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE factory = :factory_address
    """

    if active_only:
        query += " AND is_active = TRUE AND is_blacklisted = FALSE"

    query += " ORDER BY creation_block DESC"

    with engine.connect() as conn:
        result = conn.execute(text(query), {"factory_address": factory_address})
        return [dict(row._mapping) for row in result]


def get_recent_pools(
    hours_back: int, chain_id: int, active_only: bool = True
) -> List[Dict]:
    """Get pools created in the last N hours."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    # Calculate cutoff time
    cutoff_time = datetime.now() - timedelta(hours=hours_back)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE creation_timestamp >= :cutoff_time
    """

    if active_only:
        query += " AND is_active = TRUE AND is_blacklisted = FALSE"

    query += " ORDER BY creation_block DESC"

    with engine.connect() as conn:
        result = conn.execute(text(query), {"cutoff_time": cutoff_time})
        return [dict(row._mapping) for row in result]


def get_pools_since_block(
    block_number: int, chain_id: int, active_only: bool = True
) -> List[Dict]:
    """Get pools created after a specific block."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE creation_block >= :block_number
    """

    if active_only:
        query += " AND is_active = TRUE AND is_blacklisted = FALSE"

    query += " ORDER BY creation_block DESC"

    with engine.connect() as conn:
        result = conn.execute(text(query), {"block_number": block_number})
        return [dict(row._mapping) for row in result]


def get_all_pools(
    chain_id: int, active_only: bool = True, limit: Optional[int] = None
) -> List[Dict]:
    """Get all pools, optionally limited."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    query = f"SELECT * FROM {table_name}"

    if active_only:
        query += " WHERE is_active = TRUE AND is_blacklisted = FALSE"

    query += " ORDER BY creation_block DESC"

    if limit:
        query += f" LIMIT {limit}"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row._mapping) for row in result]


# ============================================================================
# UPDATE Operations
# ============================================================================


def update_pool_status(pool_address: str, is_active: bool, chain_id: int) -> bool:
    """Update pool active status."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        conn.execute(
            text(f"""
                UPDATE {table_name}
                SET is_active = :is_active, last_updated = NOW()
                WHERE address = :pool_address
            """),
            {"is_active": is_active, "pool_address": pool_address},
        )
        conn.commit()
        logger.info(f"Updated pool {pool_address} status to is_active={is_active}")
        return True


def mark_pool_blacklisted(pool_address: str, reason: str, chain_id: int) -> bool:
    """Mark pool as blacklisted."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        conn.execute(
            text(f"""
                UPDATE {table_name}
                SET is_blacklisted = TRUE,
                    blacklist_reason = :reason,
                    last_updated = NOW()
                WHERE address = :pool_address
            """),
            {"reason": reason, "pool_address": pool_address},
        )
        conn.commit()
        logger.info(f"Marked pool {pool_address} as blacklisted: {reason}")
        return True


def update_pool_metadata(pool_address: str, metadata: Dict, chain_id: int) -> bool:
    """Update additional pool metadata (JSONB field)."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        conn.execute(
            text(f"""
                UPDATE {table_name}
                SET additional_data = :metadata::jsonb,
                    last_updated = NOW()
                WHERE address = :pool_address
            """),
            {"metadata": json.dumps(metadata), "pool_address": pool_address},
        )
        conn.commit()
        logger.info(f"Updated pool {pool_address} metadata")
        return True


# ============================================================================
# DELETE Operations
# ============================================================================


def delete_pool(pool_address: str, chain_id: int) -> bool:
    """Delete pool (soft delete - sets is_active=False)."""
    return update_pool_status(pool_address, False, chain_id)


def remove_pools_from_database(pool_data: List[Dict], chain_id: int = None):
    """Hard delete pools from the database (legacy function for compatibility)."""
    engine = get_database_engine()
    if chain_id is None:
        chain_id = int(os.getenv("CHAIN_ID", "1"))
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        for pool in pool_data:
            conn.execute(
                text(f"DELETE FROM {table_name} WHERE address = :address"),
                {"address": pool["address"]},
            )
        conn.commit()

    logger.info(f"Successfully removed {len(pool_data)} pools from {table_name}")


def purge_blacklisted_pools(chain_id: int, days_old: int = 30) -> int:
    """Hard delete old blacklisted pools."""
    engine = get_database_engine()
    table_name = get_table_name(chain_id)
    cutoff_date = datetime.now() - timedelta(days=days_old)

    with engine.connect() as conn:
        result = conn.execute(
            text(f"""
                DELETE FROM {table_name}
                WHERE is_blacklisted = TRUE
                  AND last_updated < :cutoff_date
            """),
            {"cutoff_date": cutoff_date},
        )
        conn.commit()
        deleted_count = result.rowcount
        logger.info(
            f"Purged {deleted_count} blacklisted pools older than {days_old} days"
        )
        return deleted_count


# ============================================================================
# HELPER Functions
# ============================================================================


def get_protocol_from_factory(factory_address: str, chain: str = "ethereum") -> str:
    """
    Get protocol name from factory address using ConfigManager.

    Args:
        factory_address: Factory address to look up
        chain: Chain name (ethereum, base, arbitrum)

    Returns:
        Protocol name (uniswap_v3, uniswap_v4, sushiswap_v3, etc.) or "unknown"
    """
    try:
        config = ConfigManager()
        factory_lower = factory_address.lower()

        # Check each protocol's factory addresses
        for protocol_name in [
            "uniswap_v2",
            "uniswap_v3",
            "uniswap_v4",
            "sushiswap_v3",
            "pancakeswap_v3",
        ]:
            try:
                factories = config.protocols.get_factory_addresses(protocol_name, chain)
                if factory_lower in [f.lower() for f in factories]:
                    return protocol_name
            except Exception:
                continue

    except Exception as e:
        logger.warning(f"Error getting protocol from factory: {e}")

    return "unknown"


def get_all_factory_addresses(chain: str = "ethereum") -> Dict[str, List[str]]:
    """
    Get all factory addresses for all protocols on a chain.

    Args:
        chain: Chain name

    Returns:
        {
            "uniswap_v3": ["0x1F98...", "0x0BFb..."],
            "uniswap_v4": ["0x0000..."],
            ...
        }
    """
    try:
        config = ConfigManager()
        result = {}

        for protocol_name in [
            "uniswap_v2",
            "uniswap_v3",
            "uniswap_v4",
            "sushiswap_v3",
            "pancakeswap_v3",
        ]:
            try:
                factories = config.protocols.get_factory_addresses(protocol_name, chain)
                if factories:
                    result[protocol_name] = factories
            except Exception:
                continue

        return result

    except Exception as e:
        logger.error(f"Error getting all factory addresses: {e}")
        return {}


def get_pool_statistics(chain_id: int, chain: str = "ethereum") -> Dict[str, Any]:
    """
    Get statistics about pools.

    Returns:
        {
            "total_pools": 12345,
            "active_pools": 12000,
            "blacklisted_pools": 345,
            "by_factory": {
                "0x1F98...": {"protocol": "uniswap_v3", "count": 8000},
                "0xC0AE...": {"protocol": "sushiswap_v3", "count": 4000}
            }
        }
    """
    engine = get_database_engine()
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        # Total pools
        total_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        total_pools = total_result.scalar()

        # Active pools
        active_result = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {table_name} WHERE is_active = TRUE AND is_blacklisted = FALSE"
            )
        )
        active_pools = active_result.scalar()

        # Blacklisted pools
        blacklisted_result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE is_blacklisted = TRUE")
        )
        blacklisted_pools = blacklisted_result.scalar()

        # By factory
        factory_result = conn.execute(
            text(f"""
                SELECT factory, COUNT(*) as count
                FROM {table_name}
                WHERE is_active = TRUE AND is_blacklisted = FALSE
                GROUP BY factory
                ORDER BY count DESC
            """)
        )

        by_factory = {}
        for row in factory_result:
            protocol = get_protocol_from_factory(row.factory, chain)
            by_factory[row.factory] = {"protocol": protocol, "count": row.count}

        return {
            "total_pools": total_pools,
            "active_pools": active_pools,
            "blacklisted_pools": blacklisted_pools,
            "by_factory": by_factory,
        }


# ============================================================================
# LEGACY Compatibility Functions
# ============================================================================


def check_uniswap_database_results(
    LP_TYPE: str, chain_id: int = None, chain: str = "ethereum"
):
    """Check the results in the database (legacy function for compatibility)."""
    engine = get_database_engine()
    if chain_id is None:
        chain_id = int(os.getenv("CHAIN_ID", "1"))
    table_name = get_table_name(chain_id)

    # Get factory addresses from config
    config = ConfigManager()

    print(f"=== {LP_TYPE} Database Results ===")

    # Check total records
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) as total_pools FROM {table_name}"))
        total_pools = result.scalar()
        print(f"Total pools in database: {total_pools}")

    # Check sample data
    print("\n=== Sample Pool Data ===")

    with engine.connect() as conn:
        if LP_TYPE == "UniswapV3":
            v3_factories = config.protocols.get_factory_addresses("uniswap_v3", chain)
            V3_sql_query = f"""
                SELECT address, fee, asset0, asset1, creation_block, factory, 'V3' as version
                FROM {table_name}
                WHERE factory IN ({",".join([f"'{addr}'" for addr in v3_factories])})
                ORDER BY creation_block DESC
                LIMIT 5;"""
            result = conn.execute(text(V3_sql_query))
        elif LP_TYPE == "UniswapV4":
            v4_pool_manager = config.protocols.get_factory_addresses(
                "uniswap_v4", chain
            )
            V4_sql_query = f"""SELECT address, fee, asset0, asset1, creation_block, factory, additional_data, 'V4' as version
                FROM {table_name}
                WHERE factory IN ({",".join([f"'{addr}'" for addr in v4_pool_manager])})
                ORDER BY creation_block DESC
                LIMIT 5;
            """
            result = conn.execute(text(V4_sql_query))
        else:
            print(f"Unknown protocol: {LP_TYPE}")
            return

        for row in result:
            print(f"  Pool: {row.address}")
            print(f"    Fee: {row.fee}, Assets: {row.asset0} / {row.asset1}")
            print(f"    Block: {row.creation_block}, Factory: {row.factory}")

    print("\n=== Database check completed ===")


def inspect_existing_table_schema(chain_id: int = None):
    """Inspect the existing table schema to understand its structure."""
    engine = get_database_engine()
    if chain_id is None:
        chain_id = int(os.getenv("CHAIN_ID", "1"))
    table_name = get_table_name(chain_id)

    print(f"=== Inspecting table: {table_name} ===")

    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = :table_name
                );
            """),
            {"table_name": table_name},
        ).scalar()

        if not table_exists:
            print(f"Table {table_name} does not exist")
            return None

        # Get table schema
        result = conn.execute(
            text("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    character_maximum_length,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_name = :table_name
                ORDER BY ordinal_position
            """),
            {"table_name": table_name},
        )

        columns = []
        print(f"\nTable schema for {table_name}:")
        for row in result:
            nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
            length_info = (
                f"({row.character_maximum_length})"
                if row.character_maximum_length
                else ""
            )
            default_info = (
                f" DEFAULT {row.column_default}" if row.column_default else ""
            )
            print(
                f"  {row.column_name}: {row.data_type}{length_info} {nullable}{default_info}"
            )
            columns.append(
                {
                    "name": row.column_name,
                    "type": row.data_type,
                    "nullable": row.is_nullable == "YES",
                    "length": row.character_maximum_length,
                    "default": row.column_default,
                }
            )

        # Get sample data
        print(f"\nSample data from {table_name}:")
        sample_result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 3"))

        for row in sample_result:
            print(f"  {dict(row._mapping)}")

        return columns


if __name__ == "__main__":
    # Test with default chain ID
    chain_id = int(os.getenv("CHAIN_ID", "1"))
    check_uniswap_database_results("UniswapV3", chain_id, "ethereum")
