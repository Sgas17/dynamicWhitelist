# Liquidity Snapshot System Design

## Overview

The Liquidity Snapshot System maintains an up-to-date view of liquidity distribution across all tracked Uniswap V3/V4 pools by processing Mint and Burn events. The system creates base snapshots of tick data and applies incremental updates, enabling fast startup and accurate liquidity state reconstruction.

## Problem Statement

**Challenge**: Reconstructing the complete liquidity state of thousands of pools from historical events is time-consuming (30+ minutes for full replay).

**Solution**: Maintain periodic snapshots of tick data and apply only incremental updates:
- **Base Snapshot**: Complete tick_bitmap and tick_data for each pool at a specific block
- **Incremental Updates**: Only process events since last snapshot
- **Fast Startup**: Load snapshot + recent updates instead of full replay

## Architecture

### Components

1. **Base Snapshot Storage** (PostgreSQL)
   - One snapshot per pool at a specific block height
   - Stores complete tick_bitmap and tick_data as JSONB
   - Optimized for key-value access (get snapshot by pool_address)

2. **Liquidity Update Events** (TimescaleDB)
   - Time-series data of Mint/Burn events
   - Compressed historical events (>90 days)
   - Fast queries for incremental updates

3. **Snapshot Processor**
   - Processes liquidity events in chunks
   - Updates tick bitmaps and tick data using degenbot logic
   - Stores snapshots to database periodically

4. **Incremental Update Processor**
   - Loads last snapshot for each pool
   - Applies only new events since snapshot_block
   - Fast startup for live system

## Database Schema

### 1. Base Snapshots Table (PostgreSQL)

**Table**: `network_{chain_id}_liquidity_snapshots`

```sql
CREATE TABLE IF NOT EXISTS network_1_liquidity_snapshots (
    -- Primary key
    pool_address TEXT PRIMARY KEY,

    -- Snapshot metadata
    snapshot_block BIGINT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    snapshot_created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Liquidity state
    tick_bitmap JSONB NOT NULL,        -- {word: {bitmap, block_number}}
    tick_data JSONB NOT NULL,          -- {tick: {liquidityNet, liquidityGross, block_number}}

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
    total_liquidity NUMERIC(78, 0),    -- Sum of all liquidityGross

    -- Update tracking
    last_event_block BIGINT,           -- Last processed event block
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    update_count INTEGER DEFAULT 0     -- Number of updates since creation
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_snapshots_block
    ON network_1_liquidity_snapshots(snapshot_block);
CREATE INDEX IF NOT EXISTS idx_snapshots_factory
    ON network_1_liquidity_snapshots(factory);
CREATE INDEX IF NOT EXISTS idx_snapshots_updated
    ON network_1_liquidity_snapshots(last_updated);
CREATE INDEX IF NOT EXISTS idx_snapshots_protocol
    ON network_1_liquidity_snapshots(protocol);

-- GIN index for JSONB queries (optional, for advanced queries)
CREATE INDEX IF NOT EXISTS idx_snapshots_tick_data_gin
    ON network_1_liquidity_snapshots USING GIN (tick_data);
```

**Why PostgreSQL?**
- One snapshot per pool (key-value access pattern)
- Large JSONB objects (tick_bitmap, tick_data)
- No time-series queries needed
- Fast lookups by pool_address

### 2. Liquidity Update Events Table (TimescaleDB)

**Table**: `network_{chain_id}_liquidity_updates`

```sql
CREATE TABLE IF NOT EXISTS network_1_liquidity_updates (
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

-- Convert to hypertable
SELECT create_hypertable(
    'network_1_liquidity_updates',
    'event_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_updates_pool_time
    ON network_1_liquidity_updates(pool_address, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_updates_block
    ON network_1_liquidity_updates(block_number DESC);
CREATE INDEX IF NOT EXISTS idx_updates_tx
    ON network_1_liquidity_updates(transaction_hash);

-- Compression policy (compress after 90 days)
SELECT add_compression_policy(
    'network_1_liquidity_updates',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- Retention policy (keep 1 year of detailed events)
SELECT add_retention_policy(
    'network_1_liquidity_updates',
    INTERVAL '365 days',
    if_not_exists => TRUE
);
```

**Why TimescaleDB?**
- True time-series data (events over time)
- Compression for old events
- Fast range queries (get events since block X)
- Automatic partitioning by time

### 3. Snapshot Metadata Table (PostgreSQL)

**Table**: `liquidity_snapshot_metadata`

Tracks snapshot generation runs and statistics.

```sql
CREATE TABLE IF NOT EXISTS liquidity_snapshot_metadata (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    snapshot_run_timestamp TIMESTAMPTZ NOT NULL,
    snapshot_block BIGINT NOT NULL,

    -- Statistics
    total_pools_processed INTEGER NOT NULL,
    total_events_processed BIGINT NOT NULL,
    processing_time_seconds NUMERIC(10, 2),

    -- Block range
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,

    -- Success/failure tracking
    status TEXT NOT NULL,              -- "success", "failed", "partial"
    error_message TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_snapshot_meta_chain_block
    ON liquidity_snapshot_metadata(chain_id, snapshot_block DESC);
CREATE INDEX IF NOT EXISTS idx_snapshot_meta_status
    ON liquidity_snapshot_metadata(status);
```

## Data Flow

### Initial Snapshot Creation

```
┌─────────────────┐
│  Parquet Files  │
│ (Mint/Burn)     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ UnifiedLiquidityProcessor│
│ - Read events by chunks │
│ - Decode Mint/Burn      │
│ - Update tick maps      │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  PostgreSQL Snapshots   │
│  - pool_address → data  │
│  - tick_bitmap (JSONB)  │
│  - tick_data (JSONB)    │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ TimescaleDB Updates     │
│ - Individual events     │
│ - Queryable by time     │
└─────────────────────────┘
```

### Incremental Update Flow

```
┌──────────────────┐
│  Live System     │
│  - Fast startup  │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────┐
│ 1. Load Base Snapshots  │
│    - Get snapshot_block │
│    - Load tick_bitmap   │
│    - Load tick_data     │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ 2. Query Recent Updates │
│    WHERE block_number   │
│        > snapshot_block │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ 3. Apply Updates        │
│    - Update tick maps   │
│    - Current state!     │
└─────────────────────────┘
```

## Data Structures

### Tick Bitmap Structure

```python
tick_bitmap: dict[int, dict] = {
    # word (int) -> bitmap data
    -50: {
        "bitmap": 18446744073709551615,  # uint256
        "block_number": 12369621
    },
    0: {
        "bitmap": 255,
        "block_number": 12369621
    },
    # ... more words
}
```

### Simplified Tick Data Structure

**IMPORTANT**: Only storing essential fields for liquidity tracking

```python
tick_data: dict[int, dict] = {
    # tick (int) -> simplified tick info
    -887220: {
        "liquidityNet": 1234567890,        # int128 - net liquidity change
        "liquidityGross": 1234567890,      # uint128 - total liquidity
        "block_number": 12369621           # reference block
    },
    -887210: {
        "liquidityNet": -9876543210,
        "liquidityGross": 9876543210,
        "block_number": 12369625
    },
    # ... more ticks
}
```

**Storage Efficiency**: ~75% reduction in tick_data size vs. full degenbot structure
- Full structure: ~300 bytes per tick (8 fields)
- Simplified: ~75 bytes per tick (3 fields)
- **Per pool: 1000 ticks × 75 bytes = ~75 KB** (down from 300 KB)

### Complete Snapshot Structure

```python
snapshot = {
    "pool_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    "snapshot_block": 21234567,
    "snapshot_timestamp": "2025-10-17T12:00:00Z",
    "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "asset0": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
    "asset1": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
    "fee": 500,
    "tick_spacing": 10,
    "protocol": "uniswap_v3",
    "tick_bitmap": {
        -50: {"bitmap": 18446744073709551615, "block_number": 12369621},
        0: {"bitmap": 255, "block_number": 12369621}
    },
    "tick_data": {
        -887220: {"liquidityNet": 1234567890, "liquidityGross": 1234567890, "block_number": 12369621},
        -887210: {"liquidityNet": -9876543210, "liquidityGross": 9876543210, "block_number": 12369625}
    },
    "total_ticks": 1234,
    "total_bitmap_words": 56,
    "total_liquidity": "123456789012345678901234567890",
    "last_event_block": 21234567,
    "update_count": 42
}
```

## Processing Algorithm

### Initial Snapshot Generation

```python
# Pseudocode for snapshot generation
def generate_snapshot(pool_address: str, start_block: int = 0) -> dict:
    # 1. Load pool metadata from pools table
    pool = get_pool_by_address(pool_address)

    # 2. Initialize empty tick maps
    tick_bitmap = {}
    tick_data = {}  # Only liquidityNet, liquidityGross, block_number

    # 3. Get all liquidity events for this pool
    events = get_liquidity_events(
        pool_address=pool_address,
        start_block=start_block,
        order_by="block_number ASC"
    )

    # 4. Process events chronologically
    for event in events:
        if event.event_type == "Mint":
            liquidity_delta = event.liquidity_delta
        else:  # Burn
            liquidity_delta = -event.liquidity_delta

        # Update tick maps using degenbot logic (simplified)
        update_tick_maps(
            tick_bitmap=tick_bitmap,
            tick_data=tick_data,
            tick_lower=event.tick_lower,
            tick_upper=event.tick_upper,
            liquidity_delta=liquidity_delta,
            tick_spacing=pool.tick_spacing,
            block_number=event.block_number
        )

    # 5. Build snapshot
    snapshot = {
        "pool_address": pool_address,
        "snapshot_block": events[-1].block_number,
        "tick_bitmap": tick_bitmap,
        "tick_data": tick_data,
        # ... metadata
    }

    # 6. Store to database
    store_liquidity_snapshot(snapshot)

    return snapshot
```

### Incremental Update

```python
def load_current_state(pool_address: str) -> dict:
    # 1. Load base snapshot
    snapshot = load_liquidity_snapshot(pool_address)
    snapshot_block = snapshot["snapshot_block"]

    # 2. Load recent updates
    updates = get_updates_since_block(pool_address, after_block=snapshot_block)

    # 3. Apply updates to snapshot
    tick_bitmap = snapshot["tick_bitmap"]
    tick_data = snapshot["tick_data"]  # Simplified structure

    for update in updates:
        update_tick_maps(
            tick_bitmap=tick_bitmap,
            tick_data=tick_data,
            tick_lower=update.tick_lower,
            tick_upper=update.tick_upper,
            liquidity_delta=update.liquidity_delta,
            tick_spacing=snapshot["tick_spacing"],
            block_number=update.block_number
        )

    # 4. Return current state
    return {
        "tick_bitmap": tick_bitmap,
        "tick_data": tick_data,
        "current_block": updates[-1].block_number if updates else snapshot_block
    }
```

## Performance Considerations

### Storage Estimates (Simplified Structure)

**Per Pool Snapshot** (typical Uniswap V3 pool):
- tick_bitmap: ~500 words × 100 bytes = 50 KB
- tick_data (simplified): ~1000 ticks × 75 bytes = **75 KB**
- **Total per pool: ~125 KB**

**For 50,000 pools**:
- Snapshot storage: 50,000 × 125 KB = **6.25 GB**
- **64% smaller than full degenbot structure!**

**Liquidity Update Events** (per day):
- ~1M events/day across all pools
- ~500 bytes per event = **500 MB/day**
- TimescaleDB compression: ~100 MB/day compressed
- 1 year retention: **~36 GB compressed**

### Query Performance

**Load Single Snapshot**: <10ms (indexed by pool_address)
**Load All Snapshots**: ~15 seconds for 50K pools
**Query Recent Updates**: <100ms for 1000 events (indexed by pool+time)
**Incremental Startup**: **<20 seconds** vs. 30+ minutes full replay

## Implementation Plan

### Phase 1: Storage Layer (PRIORITY)

**File**: `src/core/storage/postgres_liquidity.py`

Core functions:
- `setup_liquidity_snapshots_table(engine, chain_id: int) -> bool`
- `store_liquidity_snapshot(pool_address: str, snapshot_data: dict, chain_id: int) -> bool`
- `load_liquidity_snapshot(pool_address: str, chain_id: int) -> dict | None`
- `load_all_snapshots(chain_id: int, after_block: int = 0) -> dict[str, dict]`
- `get_snapshot_metadata(pool_address: str, chain_id: int) -> dict | None`
- `get_snapshot_statistics(chain_id: int) -> dict`
- `update_snapshot(pool_address: str, updated_data: dict, chain_id: int) -> bool`

**File**: `src/core/storage/timescaledb_liquidity.py`

Core functions:
- `setup_liquidity_updates_table(engine, chain_id: int) -> bool`
- `store_liquidity_update(update: dict, chain_id: int) -> bool`
- `store_liquidity_updates_batch(updates: list[dict], chain_id: int) -> int`
- `get_updates_since_block(pool_address: str, after_block: int, chain_id: int) -> list[dict]`
- `get_updates_in_range(pool_address: str, start_block: int, end_block: int, chain_id: int) -> list[dict]`

### Phase 2: Snapshot Processor

**File**: `src/processors/pools/unified_liquidity_processor.py`

Key features:
- Process Mint/Burn events from parquet files
- Decode events using eth_abi
- Update tick_bitmap and tick_data using degenbot's UniswapV3Pool logic
- Store snapshots to PostgreSQL
- Store individual events to TimescaleDB
- Process in chunks (100k blocks) with progress tracking

Abstract methods to implement:
```python
async def process(self, **kwargs) -> Dict[str, Any]
def validate_config(self) -> bool
def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]
```

### Phase 3: Incremental Update System

**File**: `src/processors/pools/liquidity_updater.py`

Key features:
- Load base snapshot from database
- Query only new events since snapshot_block
- Apply updates to tick maps in memory
- Fast (<10 seconds for typical startup)
- Optional: store updated snapshot if significant changes

### Phase 4: Scheduled Jobs

**File**: `src/scripts/run_liquidity_snapshot_generator.py`
- Generate fresh snapshots periodically (daily/weekly)
- Process new events since last snapshot
- Update both PostgreSQL snapshots and TimescaleDB events

**File**: `src/scripts/setup_liquidity_snapshot_cron.sh`
- Automated cron configuration
- Run daily at low-traffic hours (e.g., 2 AM)

## Testing Strategy

### Unit Tests

```python
# test_liquidity_storage.py
def test_store_and_load_snapshot():
    snapshot = create_test_snapshot()  # With simplified tick_data
    store_liquidity_snapshot(snapshot)
    loaded = load_liquidity_snapshot(snapshot["pool_address"])
    assert loaded == snapshot

    # Verify only expected fields in tick_data
    for tick, data in loaded["tick_data"].items():
        assert set(data.keys()) == {"liquidityNet", "liquidityGross", "block_number"}

def test_incremental_update():
    # Store base snapshot
    snapshot = create_test_snapshot_at_block(100)

    # Add updates
    updates = create_test_updates(blocks=[101, 102, 103])
    for update in updates:
        store_liquidity_update(update)

    # Load and verify
    current = load_current_state(snapshot["pool_address"])
    assert current["current_block"] == 103
```

### Integration Tests

```python
# test_liquidity_processor.py
async def test_full_snapshot_generation():
    processor = UnifiedLiquidityProcessor(chain="ethereum")

    # Process events for test pool
    result = await processor.process_pool_snapshot(
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        start_block=12369621,
        end_block=12469621
    )

    assert result["pools_processed"] == 1
    assert result["events_processed"] > 0

    # Verify snapshot stored with simplified structure
    snapshot = load_liquidity_snapshot(
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        chain_id=1
    )
    assert snapshot is not None
    assert snapshot["snapshot_block"] == 12469621

    # Verify simplified tick_data structure
    for tick, data in snapshot["tick_data"].items():
        assert "liquidityNet" in data
        assert "liquidityGross" in data
        assert "block_number" in data
        assert len(data) == 3  # Only 3 fields
```

## Configuration

### Snapshot Update Schedule

```yaml
# In config file or environment
SNAPSHOT_UPDATE_INTERVAL: "daily"  # "hourly", "daily", "weekly"
SNAPSHOT_UPDATE_TIME: "02:00"      # Run at 2 AM
SNAPSHOT_CHUNK_SIZE: 100000        # Process 100k blocks per chunk
```

### Event Processing

```python
# In processor configuration
MINT_EVENT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_EVENT_TOPIC = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

EVENT_CHUNK_SIZE = 100000  # Process 100k blocks at a time
MAX_POOLS_PER_CHUNK = 1000  # Limit memory usage
```

## Monitoring and Alerts

### Key Metrics

```python
# Metrics to track
- snapshot_generation_time_seconds
- snapshot_size_bytes (should be ~125KB per pool)
- events_processed_per_second
- failed_snapshot_updates
- database_query_time_ms
- incremental_update_time_seconds
- storage_efficiency_ratio (actual_size / expected_size)
```

### Health Checks

```python
def check_snapshot_freshness(max_age_hours: int = 48) -> bool:
    """Verify snapshots are not too old."""
    stats = get_snapshot_statistics(chain_id=1)
    oldest_snapshot = stats["oldest_snapshot_age_hours"]
    return oldest_snapshot < max_age_hours

def check_snapshot_structure(pool_address: str) -> bool:
    """Verify tick_data has simplified structure."""
    snapshot = load_liquidity_snapshot(pool_address, chain_id=1)
    for tick, data in snapshot["tick_data"].items():
        if set(data.keys()) != {"liquidityNet", "liquidityGross", "block_number"}:
            return False
    return True
```

## Security Considerations

1. **Database Access**:
   - Use read-only credentials for snapshot queries
   - Separate write permissions for processors

2. **Data Integrity**:
   - Verify snapshot block matches last event block
   - Hash check for JSONB data consistency
   - Validate tick_data structure on load

3. **Resource Limits**:
   - Limit JSONB object size (max 500 KB per snapshot)
   - Query timeouts for large snapshot loads

## Future Enhancements

1. **Differential Snapshots**: Store only changes since last snapshot
2. **Further Compression**: Custom compression for tick_bitmap/tick_data
3. **Sharding**: Split snapshots across multiple databases by pool address range
4. **Real-time Updates**: WebSocket subscriptions for instant liquidity updates
5. **Analytics Layer**: Pre-computed liquidity depth charts, range orders, etc.
6. **Multi-protocol**: Extend to V4, Curve, Balancer, etc.

---

**Design Version**: 2.0 (Simplified tick_data structure)
**Date**: October 17, 2025
**Status**: 📋 Design Complete - Ready for Implementation
**Key Features**:
- Simplified tick_data: 3 fields only (liquidityNet, liquidityGross, block_number)
- 64% storage savings vs. full degenbot structure
- Fresh start (no migration from old snapshots)
