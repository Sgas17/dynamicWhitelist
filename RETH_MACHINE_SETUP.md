# Setup Guide for Machine with Reth Node

## Prerequisites

This machine must have:
- ✅ Running reth node with synced database
- ✅ Python 3.12+
- ✅ UV package manager
- ✅ Rust toolchain installed
- ✅ Access to reth database directory

## Step 1: Clone/Pull Repository

```bash
cd ~/
git clone https://github.com/Sgas17/dynamicWhitelist.git
# OR if already cloned:
cd ~/dynamicWhitelist
git pull origin feature/hyperliquid-redis-storage
```

## Step 2: Find Your Reth Database Path

Typical locations:
```bash
# Default mainnet location
~/.local/share/reth/mainnet/db

# Check reth data directory
reth --help | grep datadir

# Or check where reth is running
ps aux | grep reth
```

Verify the database exists:
```bash
ls -lh ~/.local/share/reth/mainnet/db/
# Should show files like: data.mdb, lock.mdb, etc.
```

## Step 3: Install Dependencies

```bash
cd ~/dynamicWhitelist

# Install Python dependencies
uv sync

# Install Rust library with Python bindings
./install_rust_library.sh
```

The installation script will:
1. Install `maturin` (Rust-Python bridge builder)
2. Build the `scrape_rethdb_data` library from `~/scrape_rethdb_data/`
3. Install it into your venv
4. Verify the import works

## Step 4: Configure Reth Path

Edit your `.env` file:
```bash
cd ~/dynamicWhitelist
cp .env.example .env  # if not exists

# Add reth path
echo "RETH_DB_PATH=/path/to/your/reth/mainnet/db" >> .env
```

Or export it:
```bash
export RETH_DB_PATH=~/.local/share/reth/mainnet/db
```

## Step 5: Test the Integration

### Test 1: Quick Import Test
```bash
uv run python -c "
import scrape_rethdb_data
print('✓ Rust library imported successfully')
"
```

### Test 2: Load a Snapshot
```python
# test_reth_snapshot.py
from src.processors/pools.reth_snapshot_loader import RethSnapshotLoader

# Initialize with your reth path
loader = RethSnapshotLoader("~/.local/share/reth/mainnet/db")

# Test with USDC-WETH pool
pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
tick_data, block_number = loader.load_v3_pool_snapshot(
    pool_address=pool_address,
    tick_spacing=10,
)

print(f"✓ Loaded {len(tick_data)} ticks at block {block_number}")
print(f"  First few ticks: {list(tick_data.keys())[:5]}")
```

Run it:
```bash
uv run python test_reth_snapshot.py
```

Expected output:
```
✓ Loaded 1470 ticks at block 23640934
  First few ticks: [-887220, -887210, -887200, -887190, -887180]
```

## Step 6: Run End-to-End Test

Run the validation test that compares Rust snapshot vs event-based processing:

```bash
cd ~/dynamicWhitelist
uv run pytest src/processors/pools/tests/test_unified_liquidity_processor.py::test_liquidity_snapshot_validation -v -s
```

This test will:
1. Load snapshot from reth DB (fast)
2. Process events from parquet (slow)
3. Compare results
4. Report match percentage

Expected: ~100% match

## Performance Comparison

### Old Method (Event Processing)
```bash
# Process 379,018 events from deployment to current
⏱️ Time: ~1.5 hours
```

### New Method (Reth DB Direct)
```bash
# Read current state from reth database
⏱️ Time: < 10 seconds
```

**600x faster!** 🚀

## Usage in Production

### Creating Initial Snapshots

```python
from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader
from src.core.storage.postgres_liquidity import store_liquidity_snapshot

# Initialize
loader = RethSnapshotLoader(reth_db_path)

# Load snapshot
tick_data, block_number = loader.load_v3_pool_snapshot(
    pool_address=pool_address,
    tick_spacing=tick_spacing,
)

# Store to database
store_liquidity_snapshot(
    pool_address=pool_address,
    protocol="uniswap_v3",
    tick_data=tick_data,
    snapshot_block=block_number,
    chain_id=1,
)
```

### Batch Loading Multiple Pools

```python
# Load multiple pools at once
pool_configs = [
    {"address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", "tick_spacing": 10},  # USDC-WETH
    {"address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", "tick_spacing": 60},  # USDC-ETH
    {"address": "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36", "tick_spacing": 10},  # WETH-USDT
]

results = loader.batch_load_v3_pools(pool_configs)

for pool_address, tick_data, block_number in results:
    print(f"Pool {pool_address}: {len(tick_data)} ticks at block {block_number}")
    store_liquidity_snapshot(...)
```

## Documentation

All documentation is now organized in `docs/`:

- **Architecture**: `docs/architecture/LIQUIDITY_SNAPSHOT_DESIGN.md`
- **Integration**: `docs/integration/RUST_SNAPSHOT_INTEGRATION.md`
- **Performance**: `docs/performance/DB_PERFORMANCE_TEST.md`
- **Development**: `CLAUDE.md`

## Troubleshooting

### Issue: "Reth database path does not exist"
**Solution**: Verify your reth database path is correct
```bash
ls -lh ~/.local/share/reth/mainnet/db/
```

### Issue: "Cannot import scrape_rethdb_data"
**Solution**: Re-run the installation script
```bash
./install_rust_library.sh
```

### Issue: "Permission denied" accessing reth database
**Solution**: Reth database is usually read-only safe, but check permissions
```bash
ls -la ~/.local/share/reth/mainnet/db/
# Should be readable by your user
```

### Issue: Rust build errors
**Solution**: Ensure Rust is installed and up to date
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update
```

## Next Steps

1. ✅ Pull latest code
2. ✅ Install dependencies
3. ✅ Configure reth path
4. ✅ Run tests
5. ⏳ Create snapshots for production pools
6. ⏳ Set up incremental update workflow

## Hybrid Workflow

For optimal performance:

1. **Initial bootstrap**: Use Rust to read from reth DB
2. **Incremental updates**: Process new events from parquet files
3. **Periodic refresh**: Re-sync from reth DB daily/weekly

This gives you the best of both worlds:
- ⚡ Fast initial snapshots
- 📈 Real-time incremental updates
- ✅ Periodic validation against source of truth

---

**Created**: 2025-10-23
**Branch**: `feature/hyperliquid-redis-storage`
**Ready for**: Machine with reth node access
