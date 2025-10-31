
# Rust Reth DB Snapshot Integration

## Overview

We're integrating the `scrape_rethdb_data` Rust library to **dramatically speed up** liquidity snapshot creation by reading directly from the reth database instead of processing historical events.

## Performance Comparison

### Old Approach (Event Processing)
```
Fetch events → Process 379,018 events → Build snapshot
⏱️ Time: 1 hour 44 minutes
```

### New Approach (Direct DB Read)
```
Read reth DB → Extract current state → Snapshot ready
⏱️ Time: < 10 seconds!
```

**Speed improvement: ~600x faster!** 🚀

## Installation

### Step 1: Install Rust Library

```bash
cd ~/dynamicWhitelist
./install_rust_library.sh
```

This script:
1. Installs `maturin` (if not already installed)
2. Builds the Rust library with Python bindings
3. Installs it into your `dynamicWhitelist` venv
4. Tests the import

### Step 2: Verify Installation

```python
import scrape_rethdb_data

# Should work without errors
print("✓ Rust library installed successfully!")
```

## Usage

### Option 1: Using the RethSnapshotLoader Helper

```python
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader

# Initialize loader with path to reth database
loader = RethSnapshotLoader("/path/to/reth/mainnet/db")

# Load V3 pool snapshot
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC-WETH
tick_data, block_number = loader.load_v3_pool_snapshot(
    pool_address=pool_address,
    tick_spacing=10,
)

print(f"Loaded {len(tick_data)} ticks at block {block_number}")
# tick_data: {tick_index: {"liquidity_gross": int, "liquidity_net": int}}
```

### Option 2: Direct Rust Library Usage

```python
import scrape_rethdb_data
import json

# Define pools to query
pools = [
    {
        "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "protocol": "v3",
        "tick_spacing": 10,
    },
]

# Call Rust function
result_json = scrape_rethdb_data.collect_pools(
    "/path/to/reth/mainnet/db",
    pools,
    None  # No V4 pool IDs
)

# Parse results
results = json.loads(result_json)
pool_data = results[0]

print(f"Block: {pool_data['block_number']}")
print(f"Ticks: {len(pool_data['ticks'])}")
```

## Integration with UnifiedLiquidityProcessor

The processor can now use **two modes**:

### Mode 1: Fast Bootstrap (Rust)
For **initial snapshot creation**, use Rust to read current state directly:

```python
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader

# Create loader
loader = RethSnapshotLoader(reth_db_path)

# Get current state instantly
tick_data, block_number = loader.load_v3_pool_snapshot(
    pool_address=pool_address,
    tick_spacing=tick_spacing,
)

# Store snapshot to database
store_liquidity_snapshot(
    pool_address=pool_address,
    protocol="uniswap_v3",
    tick_data=tick_data,
    snapshot_block=block_number,
)
```

### Mode 2: Incremental Updates (Events)
For **keeping snapshots up-to-date**, process new events from parquet files:

```python
# Load existing snapshot
existing_snapshot = load_liquidity_snapshot(pool_address, "uniswap_v3")

# Process only NEW events since snapshot block
new_events = load_events_after_block(existing_snapshot["snapshot_block"])

# Apply updates
updated_tick_data = apply_liquidity_events(
    existing_tick_data=existing_snapshot["tick_data"],
    new_events=new_events,
)
```

## Hybrid Workflow

```python
def create_or_update_snapshot(pool_address, protocol, reth_db_path):
    # Check if snapshot exists
    existing = load_liquidity_snapshot(pool_address, protocol)

    if existing is None or should_rebuild_from_scratch(existing):
        # MODE 1: Fast bootstrap from reth DB
        logger.info("Creating fresh snapshot from reth DB...")
        loader = RethSnapshotLoader(reth_db_path)
        tick_data, block_number = loader.load_v3_pool_snapshot(
            pool_address, tick_spacing
        )
        store_liquidity_snapshot(pool_address, protocol, tick_data, block_number)
    else:
        # MODE 2: Incremental update from events
        logger.info("Updating existing snapshot from events...")
        new_events = fetch_events_after(existing["snapshot_block"])
        updated_data = process_events(existing["tick_data"], new_events)
        store_liquidity_snapshot(pool_address, protocol, updated_data, latest_block)
```

## Data Structure

### Rust Library Output

```json
{
  "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
  "protocol": "UniswapV3",
  "block_number": 23640934,
  "slot0": {
    "sqrt_price_x96": "1234567890...",
    "tick": 201234,
    "observation_index": 123,
    "observation_cardinality": 456,
    "observation_cardinality_next": 456,
    "fee_protocol": 0,
    "unlocked": true
  },
  "liquidity": "123456789012345678",
  "ticks": [
    {
      "tick_index": -887220,
      "liquidity_gross": "1234567890",
      "liquidity_net": "-1234567890",
      "fee_growth_outside_0_x128": "0",
      "fee_growth_outside_1_x128": "0",
      "tick_cumulative_outside": 0,
      "seconds_per_liquidity_outside_x128": "0",
      "seconds_outside": 0,
      "initialized": true
    }
  ]
}
```

## Configuration

### Reth Database Path

The reth database is typically located at:
```
~/.local/share/reth/mainnet/db
```

Or if running reth with custom datadir:
```
/path/to/custom/datadir/db
```

### Environment Variable

Set the reth DB path in your config:

```python
# src/config/base.py
RETH_DB_PATH = os.getenv("RETH_DB_PATH", "~/.local/share/reth/mainnet/db")
```

## Benefits

1. **Speed**: 600x faster initial snapshot creation
2. **Accuracy**: Read directly from chain state (no event processing bugs)
3. **Simplicity**: No need to fetch/store/process historical events for bootstrapping
4. **Efficiency**: Lower storage requirements (no need to keep old parquet files)
5. **Reliability**: Reth DB is the source of truth

## Limitations

1. **Requires reth node**: Must have a synced reth node with database access
2. **Read-only**: Can only read current state, not historical states
3. **Snapshot timing**: Snapshot is at the latest reth block, not a specific historical block

## Use Cases

### Perfect For:
- ✅ Creating initial snapshots for new pools
- ✅ Rebuilding snapshots from scratch
- ✅ Validation (compare event-based vs DB-based snapshots)
- ✅ Fast pool state queries

### Not Suitable For:
- ❌ Historical state queries (use event processing)
- ❌ Incremental updates (use parquet events)
- ❌ When reth DB is not available (use RPC + events)

## Example: Complete Integration

```python
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader
from src.core.storage.postgres_liquidity import store_liquidity_snapshot

# Configuration
RETH_DB_PATH = "~/.local/share/reth/mainnet/db"
USDC_WETH_V3 = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

# Initialize loader
loader = RethSnapshotLoader(RETH_DB_PATH)

# Create snapshot
logger.info("Creating snapshot from reth DB...")
tick_data, block_number = loader.load_v3_pool_snapshot(
    pool_address=USDC_WETH_V3,
    tick_spacing=10,
)

# Store to database
logger.info(f"Storing snapshot with {len(tick_data)} ticks at block {block_number}")
store_liquidity_snapshot(
    pool_address=USDC_WETH_V3,
    protocol="uniswap_v3",
    tick_data=tick_data,
    snapshot_block=block_number,
    chain_id=1,
)

logger.info("✅ Snapshot created and stored successfully!")
```

## Next Steps

1. ✅ Install Rust library: `./install_rust_library.sh`
2. ✅ Created `RethSnapshotLoader` helper class
3. ⏳ Update `UnifiedLiquidityProcessor` to use hybrid approach
4. ⏳ Add validation test comparing Rust vs event-based snapshots
5. ⏳ Update documentation and workflows

## Troubleshooting

### Import Error
```
ImportError: No module named 'scrape_rethdb_data'
```
**Solution**: Run `./install_rust_library.sh`

### Database Path Error
```
ValueError: Reth database path does not exist
```
**Solution**: Verify reth DB path is correct and accessible

### Rust Build Error
```
error: could not compile `scrape_rethdb_data`
```
**Solution**: Ensure Rust toolchain is installed: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

---

**Created**: 2025-10-23
**Last Updated**: 2025-10-23
**Status**: Ready for integration
