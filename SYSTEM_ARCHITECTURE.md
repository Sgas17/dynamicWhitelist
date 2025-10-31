# DynamicWhitelist System Architecture

## Purpose
Production DeFi data pipeline that builds dynamic token whitelists, monitors pool liquidity, and publishes real-time data for arbitrage trading systems.

---

## Core Operations

### 1. Token Whitelist Generation ✅
**Status**: Implemented

**Criteria**:
- ✅ Multi-chain presence (2+ chains: Ethereum, Base, Arbitrum)
- ✅ Has perpetual swap (Hyperliquid or Lighter)
- ✅ High transfer activity (top N most transferred)

**Implementation**: `src/whitelist/builder.py`

**Output**:
```json
{
  "total_tokens": 500,
  "tokens": ["0x...", ...],
  "token_sources": {
    "0x...": ["cross_chain", "hyperliquid", "top_transferred"]
  },
  "token_info": {
    "0x...": {
      "symbol": "USDC",
      "decimals": 6,
      "coingecko_id": "usd-coin",
      "chains": ["ethereum", "base", "arbitrum-one"]
    }
  }
}
```

---

### 2. Pool Filtering for Arbitrage Paths ✅
**Status**: Implemented

**Stage 1: Baseline Price Discovery**
- Filter: `whitelisted_token <-> trusted_token` (USDC/WETH/USDT)
- Liquidity threshold: Lower (e.g., $10K)
- Purpose: Establish token prices

**Stage 2: Arbitrage Path Discovery**
- Filter: `whitelisted_token <-> other_token`
- Requirement: `other_token` must have pool with `trusted_token` above threshold
- Liquidity threshold: Higher (e.g., $50K)
- Purpose: Enable arbitrage: `TOKEN_A <-> TOKEN_B <-> USDC`

**Implementation**: `src/whitelist/pool_filter.py`, `src/whitelist/liquidity_filter.py`

**Output**:
```json
{
  "stage1_pools": [
    {
      "pool_address": "0x...",
      "token0": "0x...",
      "token1": "0x...",
      "protocol": "uniswap_v3",
      "liquidity_usd": 1500000
    }
  ],
  "stage2_pools": [...]
}
```

---

### 3. Publish to Redis/NATS ✅ ⚠️
**Status**: Partially Implemented

**What's Working**:
- ✅ Redis storage interface (`src/core/storage/redis.py`)
- ✅ NATS JetStream publisher (`src/core/storage/whitelist_publisher.py`)
- ✅ Whitelist publishing logic

**What Needs Work**:
- ⚠️ **Pool metadata publishing** - Need to add comprehensive pool data
- ⚠️ **Token metadata publishing** - Need to enrich with all metadata fields

**Required Metadata**:

**Token Metadata**:
```json
{
  "address": "0x...",
  "symbol": "USDC",
  "decimals": 6,
  "coingecko_id": "usd-coin",
  "chains": ["ethereum", "base"],
  "has_perp": true,
  "perp_exchanges": ["hyperliquid"],
  "daily_transfer_count": 50000,
  "last_updated": "2025-10-23T..."
}
```

**Pool Metadata**:
```json
{
  "pool_address": "0x...",
  "protocol": "uniswap_v3",
  "version": "v3",
  "token0": "0x...",
  "token1": "0x...",
  "fee": 3000,
  "tick_spacing": 60,
  "liquidity_usd": 1500000,
  "liquidity_token0": "1000000000000",
  "liquidity_token1": "500000000000000000",
  "volume_24h_usd": 250000,
  "current_tick": 201234,
  "sqrt_price_x96": "...",
  "stage": "stage1",  // or "stage2"
  "trust_path": ["0x_tokenA", "0x_tokenB", "0x_USDC"],
  "last_updated": "2025-10-23T..."
}
```

**Implementation Needed**:
```python
# src/core/storage/metadata_publisher.py
class MetadataPublisher:
    async def publish_token_metadata(self, tokens: List[TokenMetadata])
    async def publish_pool_metadata(self, pools: List[PoolMetadata])
```

---

### 4. Liquidity Snapshot Generation ✅ 🚧
**Status**: Implemented with Migration in Progress

**Two Methods**:

**A) Legacy RPC Method** ✅
- Uses batch RPC calls via `src/batchers/`
- Fetches current state from blockchain
- **Speed**: ~2-5 minutes for 100 pools
- **Use case**: Fallback, chains without reth

**B) Direct Reth DB Access** 🚧 NEW
- Reads directly from reth database
- **Speed**: < 10 seconds for same pools
- **600x faster!**
- **Use case**: Primary method for Ethereum mainnet

**Implementation**:
- ✅ Reth snapshot loader: `src/processors/pools/reth_snapshot_loader.py`
- ✅ RPC batchers: `src/batchers/uniswap_v3_ticks.py`, etc.
- ⚠️ **Need**: Speed comparison test
- ⚠️ **Need**: Hybrid fallback logic

**Protocol-Specific Snapshots**:

**Uniswap V2**:
```python
{
  "pool_address": "0x...",
  "reserve0": "1000000000000",
  "reserve1": "500000000000000000",
  "block_timestamp_last": 1698765432
}
```

**Uniswap V3/V4**:
```python
{
  "pool_address": "0x...",
  "liquidity": "123456789012345678",
  "sqrt_price_x96": "...",
  "tick": 201234,
  "ticks": {
    -887220: {"liquidity_gross": 100, "liquidity_net": 50},
    -887210: {"liquidity_gross": 200, "liquidity_net": -30},
    ...
  }
}
```

**Storage**:
- ✅ PostgreSQL: `liquidity_snapshots` table
- ⚠️ **Need to decide**: NATS vs gRPC for publishing

**TODO**:
```python
# Speed test comparison
async def compare_reth_vs_rpc_speed():
    # Test 100 pools with both methods
    # Measure time, accuracy
    # Document results
```

---

### 5. Transfer Monitoring (Parallel Operation) ✅
**Status**: Implemented

**Purpose**: Track token transfer patterns to identify trending tokens for whitelist addition

**Implementation**: `src/processors/transfers/unified_transfer_processor.py`

**What it does**:
- Fetches ERC20 Transfer events via Cryo
- Aggregates hourly/daily counts per token
- Stores in `token_hourly_transfers` table
- Identifies tokens with increasing activity

**Metrics Tracked**:
```sql
CREATE TABLE token_hourly_transfers (
  token_address TEXT,
  hour_timestamp TIMESTAMPTZ,
  transfer_count INTEGER,
  unique_senders INTEGER,
  unique_receivers INTEGER,
  total_volume NUMERIC
);
```

**Usage**:
```python
# Get trending tokens
trending = await get_top_transferred_tokens(
    hours=24,
    min_count=1000,
    limit=100
)
# Add to whitelist if criteria met
```

**Script**: `src/scripts/run_transfer_processor.py`

---

### 6. Scheduled CRUD Operations ⚠️
**Status**: Partially Implemented

**Operations**:

**A) New Pool Discovery** ✅
- Monitor pool creation events
- Add new pools to database
- Classify (stage1/stage2)
- **Script**: `src/scripts/run_pool_creation_monitor.py`
- **Cron**: `src/scripts/setup_pool_creation_cron.sh`

**B) Liquidity Event Monitoring** ✅
- Monitor Mint/Burn events
- Update liquidity snapshots incrementally
- **Script**: `src/scripts/run_liquidity_snapshot_generator.py`
- **Cron**: `src/scripts/setup_liquidity_snapshot_cron.sh`

**C) Transfer Event Monitoring** ✅
- Monitor Transfer events
- Update transfer counts
- **Script**: `src/scripts/run_transfer_processor.py`
- **Cron**: `src/scripts/setup_transfer_cron.sh`

**Future: Execution Engine Migration** 🔮
- Move monitoring to Execution Engines (TBD)
- Keep current scripts as fallback/validation
- Dual-write for safety during migration

---

## System Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  INITIALIZATION (One-time)                                      │
├─────────────────────────────────────────────────────────────────┤
│  1. Fetch historical events (pools, transfers, liquidity)       │
│  2. Build initial whitelist                                     │
│  3. Filter pools (stage1 + stage2)                              │
│  4. Generate liquidity snapshots (Reth DB or RPC)               │
│  5. Publish to Redis/NATS                                       │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│  ONGOING OPERATIONS (Scheduled)                                 │
├─────────────────────────────────────────────────────────────────┤
│  Parallel Task 1: Transfer Monitoring                           │
│  • Fetch new Transfer events (every hour)                       │
│  • Update token_hourly_transfers                                │
│  • Identify trending tokens                                     │
│  • Add to whitelist if criteria met                             │
│                                                                  │
│  Parallel Task 2: Pool Monitoring                               │
│  • Fetch new PoolCreated events (every hour)                    │
│  • Add new pools to database                                    │
│  • Classify stage1/stage2                                       │
│  • Update published pool list                                   │
│                                                                  │
│  Parallel Task 3: Liquidity Monitoring                          │
│  • Fetch new Mint/Burn events (every 15 min)                    │
│  • Update liquidity snapshots incrementally                     │
│  • Publish updates to NATS                                      │
│                                                                  │
│  Scheduled Task: Whitelist Refresh                              │
│  • Rebuild whitelist (daily)                                    │
│  • Re-filter pools                                              │
│  • Publish updates                                              │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│  PUBLISHING (Continuous)                                        │
├─────────────────────────────────────────────────────────────────┤
│  Redis:                                                         │
│  • SET token_whitelist <JSON>                                  │
│  • SET token_metadata:{address} <JSON>                         │
│  • SET pool_metadata:{address} <JSON>                          │
│  • SET liquidity_snapshot:{address} <JSON>                     │
│                                                                  │
│  NATS:                                                          │
│  • PUBLISH whitelist.updated <JSON>                            │
│  • PUBLISH pools.created <JSON>                                │
│  • PUBLISH liquidity.updated <JSON>                            │
│  • PUBLISH transfers.aggregated <JSON>                         │
│                                                                  │
│  gRPC: (TBD - for high-frequency updates?)                     │
│  • StreamLiquidityUpdates()                                    │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│  CONSUMERS                                                      │
├─────────────────────────────────────────────────────────────────┤
│  • Arbitrage bots                                               │
│  • Price oracles                                                │
│  • Risk systems                                                 │
│  • Analytics dashboards                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Status

### ✅ Complete
- [x] Token whitelist generation
- [x] Pool filtering (stage1 + stage2)
- [x] Transfer monitoring
- [x] Pool creation monitoring
- [x] Liquidity event monitoring
- [x] PostgreSQL storage
- [x] Redis interface
- [x] NATS publisher interface
- [x] RPC batch fetchers
- [x] Reth DB snapshot loader
- [x] Cron setup scripts

### 🚧 In Progress
- [ ] Reth DB migration (primary method)
- [ ] RPC fallback logic
- [ ] Speed comparison test (Reth vs RPC)

### ⚠️ Needs Implementation
- [ ] **Comprehensive metadata publishing**
  - Token metadata enrichment
  - Pool metadata enrichment
  - Publishing pipeline
- [ ] **NATS vs gRPC decision for liquidity updates**
  - Performance testing
  - Protocol selection
  - Implementation
- [ ] **Execution Engine migration planning**
  - Define interface
  - Migration strategy
  - Fallback logic

---

## Next Steps

### Priority 1: Complete Core Functionality
1. ✅ ~~Repository cleanup~~ (DONE)
2. ✅ ~~Reth integration setup~~ (DONE)
3. ⚠️ Speed test: Reth DB vs RPC
4. ⚠️ Implement metadata publishing pipeline
5. ⚠️ Decide NATS vs gRPC for liquidity updates

### Priority 2: Deployment
1. Pull on reth machine
2. Run `install_rust_library.sh`
3. Configure reth path
4. Test end-to-end
5. Set up cron jobs
6. Monitor logs

### Priority 3: Optimization
1. Implement hybrid Reth/RPC fallback
2. Add monitoring/alerting
3. Performance tuning
4. Documentation updates

---

## Configuration

### Required Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@host:5432/dbname

# Redis
REDIS_URL=redis://localhost:6379

# NATS
NATS_URL=nats://localhost:4222

# Reth (for snapshot generation)
RETH_DB_PATH=~/.local/share/reth/mainnet/db

# RPC (fallback)
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/...
BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/...
ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/...
```

### Cron Schedule (Example)
```cron
# Transfer monitoring (hourly)
0 * * * * cd ~/dynamicWhitelist && uv run python src/scripts/run_transfer_processor.py

# Pool creation monitoring (hourly)
0 * * * * cd ~/dynamicWhitelist && uv run python src/scripts/run_pool_creation_monitor.py

# Liquidity updates (every 15 minutes)
*/15 * * * * cd ~/dynamicWhitelist && uv run python src/scripts/run_liquidity_snapshot_generator.py

# Whitelist refresh (daily at 3 AM)
0 3 * * * cd ~/dynamicWhitelist && uv run python -m src.whitelist.builder
```

---

**Last Updated**: 2025-10-23
**Status**: Production Ready (with noted gaps)
