# Pool Creation & Liquidity Snapshot System - Design Document

## Overview

This document outlines the design for two interconnected systems:
1. **Pool Creation Monitoring** - Automated CRUD operations for tracking new DEX pools across protocols
2. **Liquidity Snapshot System** - Efficient tick/liquidity state management for fast startup and real-time updates

## Design Decisions Summary

### Database Technology

1. **Pool Tables**: **PostgreSQL** (not TimescaleDB)
   - Static data with key-value access pattern
   - Query by pool address or token, not by time
   - Low write volume, high read volume

2. **Liquidity Snapshots**: **PostgreSQL** (not TimescaleDB)
   - One snapshot per pool, updated in place
   - Large JSONB documents (MB-sized)
   - Key-value access by pool_address

3. **Liquidity Updates**: **TimescaleDB** (hypertable)
   - True time-series data (Mint/Burn events)
   - High write volume, time-range queries
   - Automatic retention and compression

### Schema Design

1. **Table Naming**: `network_{chain_id}_dex_pools` (chain-specific)
2. **Column Order**: Match legacy table exactly (code uses row numbers)
3. **Protocol Field**: Removed - protocol is implied by factory_address
4. **File Location**: Move `utils/database/database_helper.py` → `core/storage/postgres_pools.py`

## Legacy Schema (Current)

**Table**: `network_{chain_id}_dex_pools_cryo`

```sql
CREATE TABLE network_1_dex_pools_cryo (
    address TEXT PRIMARY KEY,        -- Column 1
    factory TEXT,                    -- Column 2
    asset0 TEXT,                     -- Column 3
    asset1 TEXT,                     -- Column 4
    asset2 TEXT,                     -- Column 5
    asset3 TEXT,                     -- Column 6
    creation_block INTEGER,          -- Column 7
    fee INTEGER,                     -- Column 8
    additional_data JSON,            -- Column 9
    priority INTEGER,                -- Column 10
    tick_spacing INTEGER             -- Column 11
);
```

## New Schema

### Table: `network_{chain_id}_dex_pools`

**Column order matches legacy table exactly** to maintain compatibility with code using row numbers.

```sql
CREATE TABLE network_1_dex_pools (
    -- LEGACY COLUMNS (same order as network_1_dex_pools_cryo)
    address TEXT PRIMARY KEY,        -- Column 1 (pool_address)
    factory TEXT NOT NULL,           -- Column 2 (factory_address)
    asset0 TEXT NOT NULL,            -- Column 3
    asset1 TEXT NOT NULL,            -- Column 4
    asset2 TEXT,                     -- Column 5 (NULL for V3, used in V4)
    asset3 TEXT,                     -- Column 6 (NULL for V3, used in V4)
    creation_block BIGINT NOT NULL,  -- Column 7 (changed to BIGINT for large blocks)
    fee INTEGER NOT NULL,            -- Column 8
    additional_data JSONB,           -- Column 9 (changed to JSONB from JSON)
    priority INTEGER,                -- Column 10 (not used but kept for compatibility)
    tick_spacing INTEGER NOT NULL,   -- Column 11

    -- NEW COLUMNS (added at end to avoid breaking row number access)
    creation_timestamp TIMESTAMPTZ,  -- Column 12 (new)
    creation_tx_hash TEXT,           -- Column 13 (new)
    is_blacklisted BOOLEAN DEFAULT FALSE,  -- Column 14 (new)
    blacklist_reason TEXT,           -- Column 15 (new)
    is_active BOOLEAN DEFAULT TRUE,  -- Column 16 (new)
    last_updated TIMESTAMPTZ DEFAULT NOW()  -- Column 17 (new)
);

-- Indexes for performance
CREATE INDEX idx_network_1_dex_pools_factory ON network_1_dex_pools(factory);
CREATE INDEX idx_network_1_dex_pools_asset0 ON network_1_dex_pools(asset0);
CREATE INDEX idx_network_1_dex_pools_asset1 ON network_1_dex_pools(asset1);
CREATE INDEX idx_network_1_dex_pools_creation_block ON network_1_dex_pools(creation_block);
CREATE INDEX idx_network_1_dex_pools_active ON network_1_dex_pools(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_network_1_dex_pools_blacklisted ON network_1_dex_pools(is_blacklisted) WHERE is_blacklisted = TRUE;

-- GIN index for JSONB queries
CREATE INDEX idx_network_1_dex_pools_additional_data ON network_1_dex_pools USING GIN (additional_data);
```

**Migration from Legacy**:

```sql
-- Copy data from old table to new table
INSERT INTO network_1_dex_pools (
    address, factory, asset0, asset1, asset2, asset3,
    creation_block, fee, additional_data, priority, tick_spacing
)
SELECT
    address, factory, asset0, asset1, asset2, asset3,
    creation_block, fee, additional_data::jsonb, priority, tick_spacing
FROM network_1_dex_pools_cryo
ON CONFLICT (address) DO NOTHING;

-- Optionally rename old table as backup
ALTER TABLE network_1_dex_pools_cryo RENAME TO network_1_dex_pools_cryo_backup;
```

### Protocol Identification

Protocol is **implied by factory_address** (each factory is protocol-specific). No separate `protocol` column needed.

```python
# Helper function to map factory → protocol
def get_protocol_from_factory(factory_address: str) -> str:
    """Get protocol name from factory address using centralized config."""
    config = ConfigManager()

    # Check each protocol's factory addresses
    for protocol_name in ["uniswap_v3", "uniswap_v4", "sushiswap_v3", "pancakeswap_v3"]:
        try:
            factories = config.protocol.get_factory_addresses(protocol_name, "ethereum")
            if factory_address.lower() in [f.lower() for f in factories]:
                return protocol_name
        except Exception:
            continue

    return "unknown"
```

## System 1: Pool Creation Monitoring

### Architecture

```
Scheduled Job (Hourly)
    ↓
UnifiedPoolCreationProcessor
    ↓
CryoFetcher.fetch_pool_events()
    ↓ (Parquet files)
decode_pool_events()
    ↓
filter_blacklisted_tokens()
    ↓
PostgreSQL: network_{chain_id}_dex_pools
```

### CRUD Operations

**File**: `src/core/storage/postgres_pools.py` (NEW)

Move and enhance `src/utils/database/database_helper.py` → `src/core/storage/postgres_pools.py`

```python
"""
PostgreSQL pool storage operations.

Moved from utils/database/database_helper.py for consistency with storage architecture.
Maintains legacy column ordering for compatibility.
"""

def get_table_name(chain_id: int) -> str:
    """Get pool table name for chain."""
    return f"network_{chain_id}_dex_pools"

def get_database_engine():
    """Get PostgreSQL database engine."""
    db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_uri)

def setup_pools_table(engine, chain_id: int) -> bool:
    """
    Create the network_{chain_id}_dex_pools table if it doesn't exist.

    Maintains legacy column order for compatibility with code using row numbers.
    """
    table_name = get_table_name(chain_id)

    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            );
        """), {'table_name': table_name}).scalar()

        if not table_exists:
            logger.info(f"Creating table {table_name}...")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                -- LEGACY COLUMNS (maintain exact order)
                address TEXT PRIMARY KEY,
                factory TEXT NOT NULL,
                asset0 TEXT NOT NULL,
                asset1 TEXT NOT NULL,
                asset2 TEXT,
                asset3 TEXT,
                creation_block BIGINT NOT NULL,
                fee INTEGER NOT NULL,
                additional_data JSONB,
                priority INTEGER,
                tick_spacing INTEGER NOT NULL,

                -- NEW COLUMNS (added at end)
                creation_timestamp TIMESTAMPTZ,
                creation_tx_hash TEXT,
                is_blacklisted BOOLEAN DEFAULT FALSE,
                blacklist_reason TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                last_updated TIMESTAMPTZ DEFAULT NOW()
            );

            -- Indexes
            CREATE INDEX idx_{table_name}_factory ON {table_name}(factory);
            CREATE INDEX idx_{table_name}_asset0 ON {table_name}(asset0);
            CREATE INDEX idx_{table_name}_asset1 ON {table_name}(asset1);
            CREATE INDEX idx_{table_name}_creation_block ON {table_name}(creation_block);
            CREATE INDEX idx_{table_name}_active ON {table_name}(is_active) WHERE is_active = TRUE;
            CREATE INDEX idx_{table_name}_blacklisted ON {table_name}(is_blacklisted) WHERE is_blacklisted = TRUE;
            CREATE INDEX idx_{table_name}_additional_data ON {table_name} USING GIN (additional_data);
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
        ("last_updated", "TIMESTAMPTZ DEFAULT NOW()")
    ]

    for col_name, col_type in new_columns:
        # Check if column exists
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name AND column_name = :col_name
        """), {"table_name": table_name, "col_name": col_name})

        if not result.fetchone():
            logger.info(f"Adding column {col_name} to {table_name}")
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"))
            conn.commit()

# CREATE Operations
async def store_pool(pool_data: Dict[str, Any], chain_id: int, check_blacklist: bool = True) -> bool:
    """
    Store a single pool with optional blacklist checking.

    Args:
        pool_data: Dict with keys matching table columns (in order)
        chain_id: Chain ID
        check_blacklist: Check if tokens are blacklisted

    Returns:
        True if stored successfully
    """

async def store_pools_batch(pools: List[Dict], chain_id: int, check_blacklist: bool = True) -> int:
    """
    Bulk insert pools with blacklist filtering.

    Uses psycopg2 execute_values for optimal performance.
    Maintains legacy column order for compatibility.

    Returns:
        Number of pools stored
    """

# READ Operations
async def get_pool(pool_address: str, chain_id: int) -> Optional[Dict]:
    """Get pool by address."""

async def get_pools_by_token(
    token_address: str,
    chain_id: int,
    factory: Optional[str] = None,
    active_only: bool = True
) -> List[Dict]:
    """
    Get all pools containing a specific token.

    Args:
        token_address: Token address to search for
        chain_id: Chain ID
        factory: Optional factory address to filter by protocol
        active_only: Only return active, non-blacklisted pools
    """

async def get_pools_by_factory(
    factory_address: str,
    chain_id: int,
    active_only: bool = True
) -> List[Dict]:
    """Get all pools for a specific factory (protocol)."""

async def get_recent_pools(
    hours_back: int,
    chain_id: int,
    active_only: bool = True
) -> List[Dict]:
    """Get pools created in the last N hours."""

async def get_pools_since_block(
    block_number: int,
    chain_id: int,
    active_only: bool = True
) -> List[Dict]:
    """Get pools created after a specific block."""

async def get_all_pools(
    chain_id: int,
    active_only: bool = True,
    limit: Optional[int] = None
) -> List[Dict]:
    """Get all pools, optionally limited."""

# UPDATE Operations
async def update_pool_status(pool_address: str, is_active: bool, chain_id: int) -> bool:
    """Update pool active status."""

async def mark_pool_blacklisted(pool_address: str, reason: str, chain_id: int) -> bool:
    """Mark pool as blacklisted."""

async def update_pool_metadata(pool_address: str, metadata: Dict, chain_id: int) -> bool:
    """Update additional pool metadata (JSONB field)."""

# DELETE Operations
async def delete_pool(pool_address: str, chain_id: int) -> bool:
    """Delete pool (soft delete - sets is_active=False)."""

async def purge_blacklisted_pools(chain_id: int, days_old: int = 30) -> int:
    """Hard delete old blacklisted pools."""

# HELPER Functions
def get_protocol_from_factory(factory_address: str) -> str:
    """Get protocol name from factory address using ConfigManager."""

async def get_pool_statistics(chain_id: int) -> Dict[str, Any]:
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
```

### Unified Pool Creation Processor

**File**: `src/processors/pools/unified_pool_creation_processor.py` (NEW)

```python
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path
import polars as pl
import eth_abi.abi as eth_abi
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes

from ..base import BaseProcessor
from ...config.manager import ConfigManager
from ...core.storage.postgres_pools import (
    setup_pools_table,
    store_pools_batch,
    get_pools_since_block,
    mark_pool_blacklisted,
    get_database_engine
)
from ...utils.token_blacklist_manager import TokenBlacklistManager

class UnifiedPoolCreationProcessor(BaseProcessor):
    """
    Unified processor for pool creation events across all protocols.

    Features:
    - Multi-protocol support (Uniswap V3/V4, Sushiswap V3, etc.)
    - Automatic blacklist filtering
    - Incremental processing from last block
    - PostgreSQL storage with chain-specific tables
    - Maintains legacy column order for compatibility
    """

    def __init__(self, chain: str = "ethereum", enable_blacklist: bool = True):
        """Initialize pool creation processor."""
        super().__init__(chain, "pools")
        self.config = ConfigManager()
        self.chain_id = self.config.chains.get_chain_id(self.chain)
        self._setup_database()

        # Initialize blacklist manager
        self.enable_blacklist = enable_blacklist
        self.blacklist_manager = None
        if enable_blacklist:
            self._setup_blacklist()

    def _setup_database(self):
        """Setup PostgreSQL connection and ensure table exists."""
        self.engine = get_database_engine()
        setup_pools_table(self.engine, self.chain_id)
        self.logger.info(f"Pool table ready for chain_id={self.chain_id}")

    def _setup_blacklist(self):
        """Setup token blacklist manager."""
        try:
            self.blacklist_manager = TokenBlacklistManager(
                blacklist_file=f"data/token_blacklist_{self.chain}.json",
                cache_file=f"data/etherscan_cache/etherscan_labels_{self.chain}.json",
                auto_update=False
            )
            count = len(self.blacklist_manager.blacklist)
            self.logger.info(f"Blacklist initialized: {count} entries for {self.chain}")
        except Exception as e:
            self.logger.warning(f"Failed to setup blacklist: {e}")
            self.enable_blacklist = False

    async def process_new_pools(
        self,
        factories: Optional[List[str]] = None,
        hours_back: int = 24,
        filter_blacklisted: bool = True
    ) -> Dict[str, Any]:
        """
        Process new pool creation events.

        Args:
            factories: List of factory addresses (None = all from config)
            hours_back: Hours to look back
            filter_blacklisted: Filter pools with blacklisted tokens

        Returns:
            {
                "success": True,
                "pools_processed": 150,
                "pools_stored": 145,
                "pools_filtered": 5,
                "factories": ["0x1F98...", "0xC0AE..."],
                "block_range": {"start": 12345, "end": 12445},
                "protocols": ["uniswap_v3", "sushiswap_v3"]
            }
        """
        # Implementation details...

    async def scan_and_blacklist_pools(self, limit: int = 100) -> Dict[str, Any]:
        """
        Scan recent pools and mark any with blacklisted tokens.

        Args:
            limit: Number of recent pools to check

        Returns:
            {
                "success": True,
                "pools_scanned": 100,
                "pools_blacklisted": 5,
                "tokens_checked": 200
            }
        """
```

### Scheduled Jobs

**File**: `src/scripts/run_pool_creation_monitor.py` (NEW)

```python
"""
Scheduled job for monitoring new pool creation events.

Run hourly via cron to detect and store new DEX pools across all protocols.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.processors.pools.unified_pool_creation_processor import UnifiedPoolCreationProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Process new pools across all configured factories."""
    processor = UnifiedPoolCreationProcessor(chain="ethereum", enable_blacklist=True)

    # Process all configured factories (from protocol config)
    result = await processor.process_new_pools(
        factories=None,  # None = all factories from config
        hours_back=2,  # Overlap to catch late events
        filter_blacklisted=True
    )

    if result["success"]:
        logger.info(
            f"✅ Pool Creation Complete: "
            f"{result['pools_processed']} processed, "
            f"{result['pools_stored']} stored, "
            f"{result['pools_filtered']} filtered"
        )
        logger.info(f"Protocols: {result['protocols']}")
        logger.info(f"Block range: {result['block_range']['start']} - {result['block_range']['end']}")
    else:
        logger.error(f"❌ Pool processing failed: {result.get('error')}")
        return 1

    # Scan for blacklisted tokens in recent pools
    blacklist_result = await processor.scan_and_blacklist_pools(limit=100)
    logger.info(
        f"Blacklist scan: {blacklist_result['pools_scanned']} scanned, "
        f"{blacklist_result['pools_blacklisted']} blacklisted"
    )

    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
```

**Cron Setup**: `src/scripts/setup_pool_creation_cron.sh` (NEW)

```bash
#!/bin/bash
# Setup automated pool creation monitoring

PROJECT_DIR=$(pwd)
CRON_JOB="10 * * * * cd $PROJECT_DIR && uv run python src/scripts/run_pool_creation_monitor.py >> logs/pool_creation_cron.log 2>&1"

# Remove existing job
(crontab -l 2>/dev/null | grep -v "run_pool_creation_monitor.py") | crontab -

# Add new job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Pool creation monitoring cron job installed"
echo "Schedule: Every hour at 10 minutes past"
echo "Logs: logs/pool_creation_cron.log"

# Create logs directory
mkdir -p logs

# Display current cron jobs
echo ""
echo "Current cron jobs:"
crontab -l | grep -E "(run_pool_creation_monitor|fetch_transfers|liquidity_snapshot)"
```

## System 2: Liquidity Snapshot System

### Database Schema

**Table 1**: `network_{chain_id}_liquidity_snapshots` (PostgreSQL)

Stores the latest complete snapshot for each pool. **One row per pool, updated in place.**

```sql
CREATE TABLE network_1_liquidity_snapshots (
    -- Pool identification (PRIMARY KEY)
    pool_address TEXT PRIMARY KEY,

    -- Snapshot metadata
    snapshot_block BIGINT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),

    -- Tick data (JSONB for flexible storage)
    tick_bitmap JSONB NOT NULL,  -- {word: {bitmap: value, block: int}}
    tick_data JSONB NOT NULL,    -- {tick: {liquidityNet: int, liquidityGross: int}}

    -- Statistics
    total_ticks INTEGER,
    total_liquidity NUMERIC,
    min_tick INTEGER,
    max_tick INTEGER,

    -- Pool metadata reference
    factory_address TEXT NOT NULL
);

-- Indexes
CREATE INDEX idx_network_1_liquidity_snapshots_factory ON network_1_liquidity_snapshots(factory_address);
CREATE INDEX idx_network_1_liquidity_snapshots_block ON network_1_liquidity_snapshots(snapshot_block DESC);

-- GIN indexes for JSONB (only if needed for queries)
CREATE INDEX idx_network_1_liquidity_snapshots_tick_bitmap ON network_1_liquidity_snapshots USING GIN (tick_bitmap);
CREATE INDEX idx_network_1_liquidity_snapshots_tick_data ON network_1_liquidity_snapshots USING GIN (tick_data);
```

**Table 2**: `network_{chain_id}_liquidity_updates` (TimescaleDB)

Stores incremental Mint/Burn events. **True time-series data.**

```sql
CREATE TABLE network_1_liquidity_updates (
    -- Event identification
    pool_address TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,

    -- Event details
    event_type TEXT NOT NULL,  -- 'mint' or 'burn'

    -- Tick parameters
    tick_lower INTEGER NOT NULL,
    tick_upper INTEGER NOT NULL,
    liquidity_delta NUMERIC NOT NULL,

    -- Timestamp (for TimescaleDB partitioning)
    event_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (transaction_hash, log_index)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('network_1_liquidity_updates', 'event_timestamp');

-- Indexes
CREATE INDEX idx_network_1_liquidity_updates_pool ON network_1_liquidity_updates(pool_address, event_timestamp DESC);
CREATE INDEX idx_network_1_liquidity_updates_block ON network_1_liquidity_updates(block_number);

-- Retention policy: Keep 7 days
SELECT add_retention_policy('network_1_liquidity_updates', INTERVAL '7 days');

-- Compression: Compress after 1 day
SELECT add_compression_policy('network_1_liquidity_updates', INTERVAL '1 day');
```

## File Structure

```
src/
├── core/
│   └── storage/
│       ├── postgres.py                      # Base PostgreSQL connection
│       ├── postgres_pools.py                # Pool CRUD (moved from utils/database)
│       ├── postgres_liquidity.py            # Snapshot storage (PostgreSQL)
│       ├── timescaledb.py                   # Transfer data (TimescaleDB)
│       └── timescaledb_liquidity.py         # Liquidity updates (TimescaleDB)
├── processors/
│   ├── pools/
│   │   ├── unified_pool_creation_processor.py        # NEW
│   │   ├── unified_liquidity_snapshot_processor.py   # NEW
│   │   ├── uniswap_v3_pool_fetcher_parquet.py       # Legacy (to deprecate)
│   │   └── uniswap_v3_liquidity_events_processor_parquet.py  # Legacy
│   └── transfers/
│       └── unified_transfer_processor.py
├── scripts/
│   ├── run_pool_creation_monitor.py         # NEW
│   ├── setup_pool_creation_cron.sh          # NEW
│   ├── generate_liquidity_snapshots.py      # NEW
│   ├── run_liquidity_snapshot_updates.py    # NEW
│   └── setup_liquidity_snapshot_cron.sh     # NEW
├── utils/
│   ├── token_blacklist_manager.py
│   ├── etherscan_label_scraper.py
│   └── database/  # TO BE DEPRECATED
│       └── database_helper.py               # Move to core/storage/postgres_pools.py
```

## Migration Steps

### Step 1: Move Database Helper (5 minutes)

```bash
# Move file
mkdir -p src/core/storage
mv src/utils/database/database_helper.py src/core/storage/postgres_pools.py

# Update imports in existing files
rg "from.*database_helper import" --files-with-matches | xargs sed -i 's/from processors.utils.database_helper/from src.core.storage.postgres_pools/g'
rg "from.*database_helper import" --files-with-matches | xargs sed -i 's/from utils.database_helper/from src.core.storage.postgres_pools/g'
```

### Step 2: Create New Tables (10 minutes)

```bash
# Run SQL migration script
uv run python -c "
from src.core.storage.postgres_pools import setup_pools_table, get_database_engine
engine = get_database_engine()
setup_pools_table(engine, chain_id=1)
print('Pool tables created successfully')
"
```

### Step 3: Migrate Existing Data (5 minutes)

```sql
-- Run this SQL directly in PostgreSQL
INSERT INTO network_1_dex_pools (
    address, factory, asset0, asset1, asset2, asset3,
    creation_block, fee, additional_data, priority, tick_spacing
)
SELECT
    address, factory, asset0, asset1, asset2, asset3,
    creation_block, fee, additional_data::jsonb, priority, tick_spacing
FROM network_1_dex_pools_cryo
ON CONFLICT (address) DO NOTHING;
```

## Success Criteria

✅ Pool tables use PostgreSQL (not TimescaleDB) - appropriate for static data
✅ Liquidity snapshots use PostgreSQL - appropriate for key-value access
✅ Liquidity updates use TimescaleDB - appropriate for time-series data
✅ Table names follow pattern: `network_{chain_id}_dex_pools`
✅ Column order matches legacy table exactly (for row number compatibility)
✅ Protocol field removed - implied by factory_address
✅ Database helper moved to `core/storage/` folder
✅ All new columns added at end to preserve row number access

## References

- **Legacy Database Helper**: [src/utils/database/database_helper.py](src/utils/database/database_helper.py)
- **Existing Pool Fetchers**: [src/processors/pools/uniswap_v3_pool_fetcher_parquet.py](src/processors/pools/uniswap_v3_pool_fetcher_parquet.py)
- **Liquidity Event Processor**: [src/processors/pools/uniswap_v3_liquidity_events_processor_parquet.py](src/processors/pools/uniswap_v3_liquidity_events_processor_parquet.py)
- **CryoFetcher**: [src/fetchers/cryo_fetcher.py](src/fetchers/cryo_fetcher.py)
- **Transfer Processing**: [src/processors/transfers/README.md](src/processors/transfers/README.md)
- **Blacklist Integration**: [BLACKLIST_INTEGRATION.md](BLACKLIST_INTEGRATION.md)
