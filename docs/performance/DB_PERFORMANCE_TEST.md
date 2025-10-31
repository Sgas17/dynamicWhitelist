# Database vs RPC Performance Testing

This document explains how to test the performance difference between direct database access and RPC batch calls for fetching Uniswap V3 tick data.

## Overview

The `test_db_vs_rpc_performance.py` script compares two approaches:

1. **Direct DB Access**: Uses `scrape_rethdb_data` Rust library with PyO3 bindings to directly query the Reth MDBX database
2. **RPC Batch Calls**: Uses the existing production `UniswapV3TickBatcher` that deploys Solidity contracts and calls them via `eth.call()`

## Installation

### 1. Install scrape_rethdb_data

You need to install the Rust library with Python bindings. Choose one of the following methods:

**Option A: Using maturin (recommended for development)**
```bash
cd ~/scrape_rethdb_data
maturin develop --release --features=python
```

**Option B: Using uv**
```bash
uv pip install -e ~/scrape_rethdb_data --config-settings="--features=python"
```

**Option C: Build wheel and install**
```bash
cd ~/scrape_rethdb_data
maturin build --release --features=python
pip install target/wheels/scrape_rethdb_data-*.whl
```

### 2. Set Environment Variables

```bash
# Path to your Reth database
export RETH_DB_PATH=/mnt/data/reth/mainnet/db

# Your RPC URL should already be configured in config
# But you can also set it via environment if needed
```

### 3. Install Optional Dependencies

For memory monitoring:
```bash
pip install psutil
```

## Usage

### Basic Test with Specific Ticks

Test with a few specific tick values:
```bash
python test_db_vs_rpc_performance.py \
  --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 \
  --ticks -887220,-887160,887220
```

### Realistic Test with All Initialized Ticks

Test with all initialized ticks for a pool (recommended for real performance comparison):
```bash
python test_db_vs_rpc_performance.py \
  --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 \
  --discover-ticks \
  --tick-spacing 10
```

This will:
1. Query the pool's current tick
2. Fetch tick bitmaps for a ±100,000 tick range
3. Find all initialized ticks in that range
4. Test fetching all those ticks via both methods

### Different Pool Examples

**USDC/WETH 0.05% (tick spacing 10)**
```bash
python test_db_vs_rpc_performance.py \
  --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 \
  --discover-ticks \
  --tick-spacing 10
```

**USDC/WETH 0.3% (tick spacing 60)**
```bash
python test_db_vs_rpc_performance.py \
  --pool 0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8 \
  --discover-ticks \
  --tick-spacing 60
```

**WETH/WBTC 0.3% (tick spacing 60)**
```bash
python test_db_vs_rpc_performance.py \
  --pool 0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36 \
  --discover-ticks \
  --tick-spacing 60
```

### Custom DB Path

If your database is in a different location:
```bash
python test_db_vs_rpc_performance.py \
  --db-path /custom/path/to/reth/db \
  --discover-ticks
```

## Understanding the Output

The script will output:

1. **RPC Batch Method Results**
   - Number of ticks fetched
   - Duration in seconds
   - Rate (ticks per second)
   - Memory delta
   - Sample tick data

2. **Direct DB Method Results**
   - Same metrics as RPC method

3. **Performance Comparison Summary**
   - Side-by-side comparison table
   - Speedup calculation (how many times faster DB access is)

### Example Output

```
==============================================================
PERFORMANCE COMPARISON SUMMARY
==============================================================

Method          Duration     Ticks      Rate (t/s)   Memory (MB)  Status
--------------------------------------------------------------------------------
RPC Batch       2.453        150        61.2         5.23         SUCCESS
Direct DB       0.089        150        1685.4       2.11         SUCCESS

==============================================================
Direct DB is 27.56x faster than RPC Batch
==============================================================
```

## Expected Performance

Based on the architecture:

- **RPC Batch Calls**:
  - Limited by network latency and RPC node processing
  - Typically 50-200 ticks/second
  - Each batch requires contract deployment and execution

- **Direct DB Access**:
  - Limited only by disk I/O and MDBX read performance
  - Typically 1000-5000 ticks/second on SSD
  - Direct memory-mapped database access
  - Expected speedup: **10-50x faster** than RPC

## Integration into Your Workflow

Once you've validated the performance benefits, you can integrate `scrape_rethdb_data` into your main workflow:

1. **Token Whitelist Creation**: Use existing logic
2. **Pool Filtering**: Filter V2/V3/V4 pools as before
3. **Pool Data Collection**: Replace RPC calls with DB access
   ```python
   import scrape_rethdb_data

   # Collect tick data for multiple pools
   pool_inputs = [
       scrape_rethdb_data.PoolInput.new_v3_tick(pool_addr, tick)
       for pool_addr in pools
       for tick in pool_ticks[pool_addr]
   ]

   results = scrape_rethdb_data.collect_pool_data(
       db_path,
       pool_inputs,
       None  # block_number
   )
   ```
4. **Update Database & Redis**: Process results and update as normal

## Troubleshooting

### Import Error: No module named 'scrape_rethdb_data'

Make sure you've installed the library:
```bash
cd ~/scrape_rethdb_data
maturin develop --release --features=python
```

### Database Not Found Error

Verify your database path:
```bash
ls -la $RETH_DB_PATH/data.mdb
```

If the file doesn't exist, update `RETH_DB_PATH` to the correct location.

### Permission Denied

Ensure you have read permissions on the database:
```bash
chmod -R 755 /path/to/reth/db
```

### RPC Connection Failed

Check your RPC configuration in `src/config/manager.py` or set directly:
```python
from web3 import Web3
web3 = Web3(Web3.HTTPProvider("http://100.104.193.35:8545"))
```

## Next Steps

After confirming the performance benefits:

1. Integrate DB access into your main pool data collection pipeline
2. Consider running the DB access as a background service
3. Use the speedup to increase your data collection frequency
4. Monitor memory usage if processing very large pools (10,000+ ticks)

## Notes

- The script uses the **existing production batcher** (`UniswapV3TickBatcher`) for RPC calls, ensuring fair comparison
- Memory monitoring requires `psutil` but is optional
- The script handles missing ticks gracefully (returns count of successful fetches)
- For production use, consider caching frequently accessed pools in Redis
