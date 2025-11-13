# Pool Scraping Performance Testing Plan

## Overview

We need to test the actual scraping performance of `pool_state_arena` using real RethDB queries instead of mock estimates. This will validate (or refine) our startup time estimates.

## Test Data

We have a cached whitelist from `dynamicWhitelist` with real pool data:

```
Location: /home/sam-sullivan/dynamicWhitelist/data/filtered_pools_ethereum.json

Pool counts:
- V2: 399 pools
- V3: 713 pools
- V4: 548 pools
- Total: 1660 pools
```

### Test Samples Created

Located in `/home/sam-sullivan/dynamicWhitelist/data/test_samples/`:
- `v2_pools_sample.json` - 10 V2 pools
- `v3_pools_sample.json` - 10 V3 pools
- `v4_pools_sample.json` - 10 V4 pools

## Current Estimates (To Be Validated)

| Protocol | Speed Estimate | Reasoning |
|----------|---------------|-----------|
| V2 | 10-20 pools/sec | Only scrapes `reserve0`, `reserve1` (2 storage slots) |
| V3 | 1-2 pools/sec | Scrapes `slot0` + ticks + bitmaps (~30k ticks for active pools) |
| V4 | 1-2 pools/sec | Similar to V3 (ticks + bitmaps) |

**Estimated Cold Start Time (1660 pools):**
- V2 (399 pools): ~20-40 seconds @ 10-20 pools/sec
- V3 (713 pools): ~350-700 seconds @ 1-2 pools/sec
- V4 (548 pools): ~275-550 seconds @ 1-2 pools/sec
- **Total: ~645-1290 seconds (10-21 minutes)**

## What to Test

### 1. **V2 Pool Scraping** (Baseline)

Test scraping V2 pools to establish the baseline RethDB query performance.

**What gets scraped:**
```rust
// V2 only needs 2 storage reads
struct UniswapV2PoolState {
    reserve0: u128,  // Storage slot 8
    reserve1: u128,  // Storage slot 8 (packed)
}
```

**Expected queries:**
- 1 RethDB query per pool (reading reserves from slot 8)
- Very fast (reserves are packed in single slot)

**Test procedure:**
1. Scrape 10 V2 pools from test sample
2. Measure total time and calculate pools/second
3. Extrapolate to 399 V2 pools

### 2. **V3 Pool Scraping** (Slow Path)

Test scraping V3 pools with full tick/bitmap data.

**What gets scraped:**
```rust
struct UniswapV3PoolState {
    // Slot0 data (1 query)
    sqrt_price_x96: U256,
    tick: i32,
    liquidity: u128,

    // Tick data (many queries)
    ticks: Vec<(i32, TickInfo)>,  // ~30k ticks for active pools

    // Bitmap data (many queries)
    tick_bitmaps: HashMap<i16, U256>,  // ~100 bitmaps for active pools
}
```

**Expected queries:**
- 1 RethDB query for `slot0`
- N queries for ticks (N = number of initialized ticks, ~100-30,000)
- M queries for bitmaps (M = number of bitmap words, ~10-100)

**Test procedure:**
1. Scrape 10 V3 pools from test sample
2. Measure total time per pool
3. Break down by:
   - Time for slot0 query
   - Time for tick queries
   - Time for bitmap queries
4. Extrapolate to 713 V3 pools

### 3. **V4 Pool Scraping** (Similar to V3)

Test scraping V4 pools with tick/bitmap data.

**What gets scraped:**
```rust
struct UniswapV4PoolState {
    // Similar to V3
    pool_id: [u8; 32],  // 32-byte pool ID instead of address
    sqrt_price_x96: U256,
    tick: i32,
    liquidity: u128,
    ticks: Vec<(i32, TickInfo)>,
    tick_bitmaps: HashMap<i16, U256>,
}
```

**Expected performance:**
- Similar to V3 (same data structures)
- May be slightly different due to V4 storage layout

**Test procedure:**
1. Scrape 10 V4 pools from test sample
2. Measure total time and calculate pools/second
3. Extrapolate to 548 V4 pools

## Implementation Approach

### Option 1: Rust Benchmark in pool_state_arena

Create a Rust binary that:
1. Reads test samples from JSON
2. Connects to RethDB
3. Scrapes pools using actual scraper implementation
4. Measures and reports timing

**Pros:**
- Tests actual Rust code that will be used in production
- Most accurate performance measurement
- Can use Criterion.rs for proper benchmarking

**Cons:**
- Requires RethDB scraper implementation to be complete
- More setup required

### Option 2: Python Prototype

Create a Python script that:
1. Connects to RethDB (using existing connection)
2. Queries pool state for test samples
3. Measures timing per protocol
4. Extrapolates to full whitelist

**Pros:**
- Quick to implement
- Can reuse existing DB connections
- Easy to iterate

**Cons:**
- Python may be slower than Rust
- Not testing actual production code

## Recommended Approach

**Start with Python prototype to get quick measurements, then validate with Rust:**

1. **Phase 1: Python Quick Test** (30 minutes)
   - Create Python script using test samples
   - Measure actual RethDB query times
   - Get rough estimates for V2/V3/V4 speeds

2. **Phase 2: Rust Validation** (if needed)
   - Implement Rust benchmark if Python shows significantly different results
   - Verify performance with production Rust code

## Next Steps

### Immediate (Python Quick Test)

```python
# Script: scripts/measure_scraping_performance.py

import asyncio
import asyncpg
import json
import time
from pathlib import Path

async def test_v2_scraping(pools, db_conn):
    """Measure V2 pool scraping performance."""
    times = []

    for pool in pools:
        start = time.perf_counter()

        # Query V2 reserves from RethDB
        # TODO: Replace with actual query
        query = """
        SELECT ...
        FROM storage_slots
        WHERE address = $1
        """

        await db_conn.fetchrow(query, pool['address'])

        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    pools_per_sec = 1 / avg_time

    return {
        'avg_time_ms': avg_time * 1000,
        'pools_per_sec': pools_per_sec,
        'total_pools': len(pools),
        'total_time_sec': sum(times)
    }

# Similar for V3 and V4...
```

### Follow-up (Rust Benchmark)

```rust
// Location: pool_state_arena/benches/scraping_performance.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pool_state_arena::subscriber::reth_scraper::RethDbScraper;

fn bench_v2_scraping(c: &mut Criterion) {
    let scraper = RethDbScraper::new();
    let pools = load_test_pools("v2_pools_sample.json");

    c.bench_function("v2_scraping", |b| {
        b.iter(|| {
            for pool in &pools {
                black_box(scraper.scrape_v2_pool(pool));
            }
        });
    });
}

criterion_group!(benches, bench_v2_scraping);
criterion_main!(benches);
```

## Expected Outcomes

### Scenario 1: Estimates are accurate

- V2: ~10-20 pools/sec ✓
- V3/V4: ~1-2 pools/sec ✓
- Cold start: ~10-21 minutes

**Action:** Proceed with implementation as designed

### Scenario 2: Faster than estimated

- V2: ~50-100 pools/sec (optimized storage reads)
- V3/V4: ~5-10 pools/sec (parallel tick queries)
- Cold start: ~2-5 minutes

**Action:** Update documentation, consider smaller event buffer

### Scenario 3: Slower than estimated

- V2: ~5 pools/sec (network latency)
- V3/V4: ~0.5 pools/sec (sequential tick queries)
- Cold start: ~20-40 minutes

**Action:**
- Optimize queries (batch reads, parallel fetching)
- Consider incremental scraping strategy
- Larger event buffer needed

## Questions to Answer

1. **What is the actual RethDB query latency?**
   - Local vs remote RethDB
   - Query complexity impact
   - Batch vs sequential reads

2. **Are ticks/bitmaps the bottleneck for V3/V4?**
   - Can we parallelize tick queries?
   - Is there a way to batch-read ticks?
   - Do we need ALL ticks or just active range?

3. **How does network affect performance?**
   - Local RethDB: minimal latency
   - Remote RethDB: 10-50ms per query
   - This could 10x our estimates

4. **Can we optimize scraping order?**
   - Scrape V2 pools first (fast)
   - Set reference block early
   - Minimize V3/V4 buffering time

## Success Criteria

✅ Measure actual V2 scraping speed (pools/sec)
✅ Measure actual V3 scraping speed (pools/sec)
✅ Measure actual V4 scraping speed (pools/sec)
✅ Calculate realistic cold start time for 1660 pools
✅ Update documentation with real measurements
✅ Decide if optimization is needed before implementation

## Resources

- Test samples: `/home/sam-sullivan/dynamicWhitelist/data/test_samples/`
- Full whitelist: `/home/sam-sullivan/dynamicWhitelist/data/filtered_pools_ethereum.json`
- RethDB connection: Use existing connection from `eventCaptureService`
- Documentation: `docs/poolStateArena_socket_integration_summary.md`
