# Transfer Processing System

Automated system for collecting, processing, and storing ERC20 token transfer events from Ethereum.

## Overview

The transfer processing system continuously monitors blockchain transfer activity to identify actively traded tokens. It operates on an hourly cron schedule, collecting raw transfer events and aggregating them into time-series data stored in TimescaleDB.

## Architecture

```
Ethereum Node (RPC)
       ↓
    Cryo Fetcher (fetches raw logs)
       ↓
  Parquet Files (data/ethereum/latest_transfers/)
       ↓
UnifiedTransferProcessor
       ↓
TimescaleDB (chain-specific tables)
   ├─ network_1_token_raw_transfers (5-minute data, 5-day retention)
   └─ network_1_token_hourly_transfers (hourly aggregates, 90-day retention)
       ↓
   Redis Cache (optional, for fast API access)
```

## Database Tables

### Table Naming Convention

Tables follow the pattern `network_{chain_id}_token_{granularity}_transfers`:

- **Ethereum (chain_id=1)**:
  - `network_1_token_raw_transfers`
  - `network_1_token_hourly_transfers`

- **Base (chain_id=8453)**:
  - `network_8453_token_raw_transfers`
  - `network_8453_token_hourly_transfers`

### Raw Transfers Table (`network_{chain_id}_token_raw_transfers`)

Stores 5-minute interval snapshots of transfer activity.

**Schema**:
```sql
CREATE TABLE network_1_token_raw_transfers (
    timestamp TIMESTAMPTZ NOT NULL,      -- 5-minute interval timestamp
    token_address TEXT NOT NULL,          -- ERC20 token contract address
    transfer_count INTEGER NOT NULL,      -- Number of transfers in interval
    unique_senders INTEGER NOT NULL,      -- Unique sender addresses
    unique_receivers INTEGER NOT NULL,    -- Unique receiver addresses
    total_volume NUMERIC,                 -- Sum of transfer amounts
    PRIMARY KEY (timestamp, token_address)
);
```

**Features**:
- TimescaleDB hypertable with 5-minute chunks
- Compression enabled after 1 day
- Automatic retention policy: 5 days

### Hourly Transfers Table (`network_{chain_id}_token_hourly_transfers`)

Aggregated hourly data with rolling averages.

**Schema**:
```sql
CREATE TABLE network_1_token_hourly_transfers (
    hour_timestamp TIMESTAMPTZ NOT NULL,  -- Hour boundary timestamp
    token_address TEXT NOT NULL,          -- ERC20 token contract address
    transfer_count INTEGER NOT NULL,      -- Transfers in the hour
    unique_senders INTEGER NOT NULL,      -- Unique senders in hour
    unique_receivers INTEGER NOT NULL,    -- Unique receivers in hour
    total_volume NUMERIC,                 -- Sum of transfer amounts
    avg_transfers_24h NUMERIC(10, 2),     -- 24-hour rolling average
    PRIMARY KEY (hour_timestamp, token_address)
);
```

**Features**:
- TimescaleDB hypertable with 1-hour chunks
- Compression enabled after 1 day
- Automatic retention policy: 90 days
- Rolling 24-hour averages for trend analysis

## Scripts

### 1. fetch_transfers.sh

Fetches raw transfer events from Ethereum using Cryo.

**Usage**:
```bash
./src/scripts/fetch_transfers.sh <hours>
```

**Example**:
```bash
# Fetch last 1 hour of transfers
./src/scripts/fetch_transfers.sh 1

# Fetch last 24 hours
./src/scripts/fetch_transfers.sh 24
```

**What it does**:
1. Calculates block range based on hours requested (~300 blocks/hour)
2. Fetches Transfer events (`0xddf252ad...`) using Cryo
3. Stores raw logs as Parquet files in `data/ethereum/latest_transfers/`
4. Automatically calls the transfer processor

**Configuration**:
- RPC URL: `http://100.104.193.35:8545`
- Block time: 12 seconds (300 blocks per hour)
- Chunk size: 500 blocks per request
- Inner request size: 10 blocks (to avoid event limits)

### 2. run_transfer_processor.py

Processes fetched transfer data and stores in TimescaleDB.

**Usage**:
```bash
uv run python src/scripts/run_transfer_processor.py
```

**What it does**:
1. Initializes `UnifiedTransferProcessor` for Ethereum
2. Validates configuration and TimescaleDB connection
3. Reads Parquet files from `data/ethereum/latest_transfers/`
4. Processes transfers with these parameters:
   - `hours_back=2` - Analyzes last 2 hours (catches late arrivals)
   - `min_transfers=50` - Minimum transfers to include token
   - `store_raw=True` - Stores raw 5-minute data
   - `update_cache=True` - Updates Redis cache (if available)
5. Aggregates raw data to hourly intervals
6. Updates rolling 24-hour averages

**Output**:
- Logs processing statistics
- Shows top 10 most transferred tokens
- Returns exit code 0 on success, 1 on failure

### 3. setup_transfer_cron.sh

Configures automated hourly cron job.

**Usage**:
```bash
./src/scripts/setup_transfer_cron.sh
```

**What it does**:
1. Removes any existing transfer processing cron jobs
2. Adds new cron job: runs every hour at 5 minutes past (e.g., 1:05, 2:05, 3:05)
3. Creates logs directory if needed
4. Displays current cron configuration

**Cron schedule**:
```bash
5 * * * * cd /path/to/project && ./src/scripts/fetch_transfers.sh 1 >> logs/transfer_cron.log 2>&1
```

**Log file**: `logs/transfer_cron.log`

## Setup Instructions

### Prerequisites

1. **TimescaleDB**: Must be running and configured
   ```bash
   # Check TimescaleDB connection
   uv run python -c "from src.core.storage.timescaledb import get_timescale_engine; print(get_timescale_engine())"
   ```

2. **Redis** (Optional): For caching top tokens
   ```bash
   # Check Redis connection
   redis-cli ping
   ```

3. **Cryo**: Ethereum data extraction tool
   ```bash
   # Check Cryo installation
   cryo --version
   ```

4. **Cast** (Foundry): For getting latest block numbers
   ```bash
   # Check Cast installation
   cast --version
   ```

### Installation

1. **Test manually first**:
   ```bash
   # Fetch and process 1 hour of transfers
   ./src/scripts/fetch_transfers.sh 1
   ```

2. **Verify data storage**:
   ```bash
   uv run python -c "from src.core.storage.timescaledb import get_database_stats; print(get_database_stats(1))"
   ```

3. **Set up automated cron job**:
   ```bash
   ./src/scripts/setup_transfer_cron.sh
   ```

4. **Monitor logs**:
   ```bash
   tail -f logs/transfer_cron.log
   ```

## Configuration

### Chain Configuration

The processor automatically determines the chain_id from the chain name using ConfigManager:

```python
processor = UnifiedTransferProcessor(chain="ethereum")  # chain_id=1
processor = UnifiedTransferProcessor(chain="base")      # chain_id=8453
```

### Processing Parameters

Modify parameters in [run_transfer_processor.py](../../scripts/run_transfer_processor.py:50-56):

```python
result = await processor.process(
    data_dir=str(data_dir),
    hours_back=2,          # Look back window (catches late data)
    min_transfers=50,      # Minimum transfers to include token
    store_raw=True,        # Store 5-minute raw data
    update_cache=True      # Update Redis cache
)
```

### RPC Configuration

Update RPC URL in [fetch_transfers.sh](../../scripts/fetch_transfers.sh:20):

```bash
RPC_URL=http://100.104.193.35:8545
```

## Monitoring

### View Database Statistics

```bash
uv run python -c "
from src.core.storage.timescaledb import get_database_stats
import json
stats = get_database_stats(chain_id=1)
print(json.dumps(stats, indent=2, default=str))
"
```

### Query Top Tokens

```bash
uv run python -c "
from src.core.storage.timescaledb import get_top_tokens_by_average
tokens = get_top_tokens_by_average(hours_back=24, limit=10, chain_id=1)
for i, token in enumerate(tokens, 1):
    print(f'{i}. {token['token_address'][:10]}... transfers={token['transfer_count']}')
"
```

### Check Cron Job Status

```bash
# List cron jobs
crontab -l | grep fetch_transfers

# Monitor real-time logs
tail -f logs/transfer_cron.log

# Check recent processing
grep "Transfer Processing Complete" logs/transfer_cron.log | tail -5
```

### Manually Trigger Processing

```bash
# Process existing parquet files
uv run python src/scripts/run_transfer_processor.py

# Fetch new data and process
./src/scripts/fetch_transfers.sh 1
```

## Troubleshooting

### No Data Collected

**Symptom**: "No transfer or log parquet files found"

**Solutions**:
1. Check data directory exists: `ls -la data/ethereum/latest_transfers/`
2. Verify RPC connection: `cast block -f number -r http://100.104.193.35:8545 finalized`
3. Check Cryo installation: `cryo --version`

### TimescaleDB Connection Failed

**Symptom**: "Failed to setup TimescaleDB"

**Solutions**:
1. Verify TimescaleDB is running
2. Check database credentials in `.env`
3. Test connection: `psql -U <user> -d <database> -c "SELECT version()"`

### Redis Connection Failed

**Symptom**: "Error 111 connecting to localhost:6379"

**Solutions**:
- Redis is optional - processing continues without cache
- Start Redis: `sudo systemctl start redis`
- Or disable cache updates: Set `update_cache=False` in run_transfer_processor.py

### Cron Job Not Running

**Symptom**: No new entries in `logs/transfer_cron.log`

**Solutions**:
1. Check cron service: `sudo systemctl status cron`
2. Verify cron job exists: `crontab -l`
3. Check permissions: `ls -l src/scripts/fetch_transfers.sh`
4. Test manually: `./src/scripts/fetch_transfers.sh 1`

### Compression Policy Warnings

**Symptom**: "Hourly transfers compression setup failed"

**Solutions**:
- These are usually harmless (policies may already exist)
- Verify manually: Check TimescaleDB compression policies
- Data storage and retention still work correctly

## Data Cleanup

### Manual Cleanup

```bash
uv run python -c "
from src.core.storage.timescaledb import cleanup_old_data, cleanup_test_data

# Remove data older than retention policies
cleanup_old_data(chain_id=1)

# Remove test data (use with caution)
# cleanup_test_data(chain_id=1)
"
```

### Automatic Cleanup

TimescaleDB retention policies automatically remove:
- Raw transfers older than 5 days
- Hourly transfers older than 90 days

## Performance

### Expected Performance

- **Cryo fetch**: ~10 seconds for 300 blocks (1 hour)
- **Processing**: ~2 seconds for 90,000 transfer events
- **Storage**:
  - Raw: ~2,500 unique tokens per hour
  - Hourly: ~2,500 aggregated records per hour
- **Disk usage**: Minimal due to compression (starts after 1 day)

### Optimization Tips

1. **Reduce fetch frequency**: Lower `hours_back` if processing is slow
2. **Adjust min_transfers**: Increase threshold to reduce stored tokens
3. **Disable Redis**: Set `update_cache=False` if not using cache
4. **Tune RPC**: Adjust `BLOCKS_PER_REQUEST` in fetch_transfers.sh

## Integration

### Using Transfer Data in Other Modules

```python
from src.core.storage.timescaledb import (
    get_top_tokens_by_average,
    get_table_names
)

# Get actively traded tokens for Ethereum
top_tokens = get_top_tokens_by_average(
    hours_back=24,
    limit=100,
    chain_id=1  # Ethereum
)

# Get table names for custom queries
tables = get_table_names(chain_id=1)
print(tables['raw'])     # network_1_token_raw_transfers
print(tables['hourly'])  # network_1_token_hourly_transfers
```

### Multi-Chain Support

The system supports multiple chains with isolated data:

```python
# Ethereum
processor_eth = UnifiedTransferProcessor(chain="ethereum")
await processor_eth.process(...)

# Base
processor_base = UnifiedTransferProcessor(chain="base")
await processor_base.process(...)
```

Each chain maintains separate tables:
- Ethereum: `network_1_token_*`
- Base: `network_8453_token_*`
- Arbitrum: `network_42161_token_*`

## References

- **UnifiedTransferProcessor**: [unified_transfer_processor.py](./unified_transfer_processor.py)
- **TimescaleDB Module**: [../../core/storage/timescaledb.py](../../core/storage/timescaledb.py)
- **Cryo Documentation**: https://github.com/paradigmxyz/cryo
- **TimescaleDB Documentation**: https://docs.timescale.com/
