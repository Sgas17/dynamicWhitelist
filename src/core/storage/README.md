# Storage Architecture

This directory contains the centralized storage implementations for the dynamic whitelist system.

## Database Information

**Database Name**: `itrcap`
**Database Type**: PostgreSQL 14.17 with TimescaleDB extensions
**Schema**: `public`

## Storage Modules

### `timescaledb.py`
**Purpose**: TimescaleDB-specific functionality for time-series transfer data

**Key Features**:
- Hypertable creation and management (5-minute and 1-hour chunks)
- Compression policies (compress after 1 day)
- Retention policies (5 days for raw data, 90 days for hourly data)
- Schema migrations
- Database statistics and monitoring

**Tables Managed**:
- `token_raw_transfers`: 5-minute aggregated transfer data
- `token_hourly_transfers`: Hourly aggregated transfer data with 24-hour rolling averages

**Functions**:
- `setup_timescale_tables()`: Create and configure hypertables
- `store_raw_transfers()`: Store 5-minute aggregated data
- `store_hourly_transfers()`: Store hourly aggregated data
- `get_database_stats()`: Get table sizes and statistics
- `cleanup_old_data()`: Clean up data based on retention policies

### `postgres.py`
**Purpose**: General PostgreSQL storage for tokens, pools, and other data

**Key Features**:
- AsyncPG connection pool management
- Token metadata storage (CoinGecko integration)
- Pool data storage (DEX pools)
- Transaction management

### Other Storage Modules
- `redis.py`: Redis caching implementation
- `json_storage.py`: JSON file storage
- `base.py`: Base storage interfaces and abstractions

## Architecture Benefits

### ✅ Separation of Concerns
- TimescaleDB-specific code is isolated in `storage/timescaledb.py`
- Processors focus on business logic, not database implementation
- Storage implementations can be swapped or extended independently

### ✅ Proper Dependency Direction
- High-level processors depend on storage abstractions
- Storage modules are self-contained and reusable
- Configuration and connection management is centralized

### ✅ Maintainability
- All hypertable setup in one place
- Schema migrations are handled centrally  
- Database statistics and monitoring centralized
- Easy to test storage functionality in isolation

## Migration from Legacy Architecture

**Before**: 
```
src/processors/transfers/token_transfer_timeseries.py (mixed concerns)
```

**After**:
```
src/core/storage/timescaledb.py (storage only)
src/processors/transfers/unified_transfer_processor.py (business logic only)
```

**Changes Made**:
1. Moved all TimescaleDB functionality to `src/core/storage/timescaledb.py`
2. Updated imports in `UnifiedTransferProcessor`  
3. Updated live integration tests to use new storage location
4. Preserved all existing functionality and live data compatibility

## Usage Example

```python
from src.core.storage.timescaledb import (
    setup_timescale_tables,
    store_raw_transfers,
    get_database_stats
)

# Setup hypertables (done automatically in processor init)
setup_timescale_tables()

# Store transfer data
await store_raw_transfers(transfer_data, interval_start)

# Get database information
stats = get_database_stats()
```

## Live Data Integration

All live data integration tests pass with the new architecture:
- ✅ TimescaleDB connection validation
- ✅ Hypertable creation and configuration  
- ✅ MEV analysis with pattern-based detection
- ✅ Data storage and retrieval
- ✅ Redis caching integration
- ✅ End-to-end processing workflows
- ✅ Performance testing (sub-5 second queries)

The storage architecture maintains full backward compatibility while providing a cleaner, more maintainable structure.