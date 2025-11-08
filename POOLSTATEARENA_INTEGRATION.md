# poolStateArena Integration Guide

## Overview
This document describes the changes needed to integrate dynamicWhitelist with poolStateArena for in-memory pool state management.

## NATS Message Format

### Full Topic: `whitelist.pools.{chain}.full`

The full NATS message includes all metadata needed for poolStateArena to:
1. Scrape pool state from rethDB
2. Perform swap calculations
3. Validate ticks and navigate tick trees

```json
{
  "type": "add",
  "pools": [
    {
      "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
      "protocol": "v3",
      "fee": 500,
      "tick_spacing": 10,
      "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
      "token0": {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
        "symbol": "USDC",
        "name": "USD Coin"
      },
      "token1": {
        "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "decimals": 18,
        "symbol": "WETH",
        "name": "Wrapped Ether"
      }
    },
    {
      "pool_id": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
      "address": "0x0BFBb8b0000000000000000000000000000BBBBb8",
      "protocol": "v4",
      "fee": 3000,
      "tick_spacing": 60,
      "factory": "0x0000000000000000000000000000000000000000",
      "token0": {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
        "symbol": "USDC"
      },
      "token1": {
        "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "decimals": 18,
        "symbol": "WETH"
      }
    }
  ],
  "chain": "ethereum",
  "timestamp": "2025-11-08T12:00:00.000Z",
  "snapshot_id": 1731067200000
}
```

### Field Requirements by Protocol

| Field | V2 | V3 | V4 | Purpose |
|-------|----|----|----|----|
| `address` | ✅ Required | ✅ Required | ✅ Required | Pool Manager address (for scraping) |
| `pool_id` | ❌ N/A | ❌ N/A | ✅ Required | 32-byte pool identifier |
| `fee` | ❌ N/A | ✅ Required | ✅ Required | Fee for swap calculations |
| `tick_spacing` | ❌ N/A | ✅ Required | ✅ Required | Tick validation & iteration |
| `token0.decimals` | ✅ Required | ✅ Required | ✅ Required | Amount conversions |
| `token1.decimals` | ✅ Required | ✅ Required | ✅ Required | Amount conversions |

**Note**: V4 hook support is not included in initial implementation. Only V4 pools with no hooks (zero address) are supported. When hook support is added later, the arena will be rebuilt from scratch.

## poolStateArena Struct Changes

### Current Issues

1. **pool_id is 20 bytes** - V4 requires 32 bytes
2. **sqrt_price_x96 is [u8; 20]** - Should be u160/U256
3. **Missing critical fields**: fee, tick_spacing, decimals

### Recommended Changes

#### 1. Update Protocol Trait

```rust
// src/protocol.rs

/// Generic trait for all pool data types
pub trait PoolData: Send + Sync + Default {
    /// Protocol this pool belongs to
    fn protocol() -> Protocol;

    /// Get the pool identifier (20 bytes for V2/V3, 32 bytes for V4)
    /// Returns a fixed-size array - use PoolIdentifier enum for flexibility
    fn pool_id(&self) -> PoolIdentifier;

    /// Set the pool identifier
    fn set_pool_id(&mut self, pool_id: PoolIdentifier);

    // ... rest of existing methods
}

/// Pool identifier that supports both V2/V3 (20 bytes) and V4 (32 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolIdentifier {
    Address([u8; 20]),  // V2/V3: Pool contract address
    PoolId([u8; 32]),   // V4: Pool ID hash
}

impl PoolIdentifier {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PoolIdentifier::Address(addr) => addr.as_slice(),
            PoolIdentifier::PoolId(id) => id.as_slice(),
        }
    }
}
```

#### 2. Update Uniswap V3 Pool Data

```rust
// src/uniswap_v3.rs

use alloy_primitives::U256;  // Or use a U160 type

#[repr(C, align(64))]
pub struct UniswapV3PoolData<const TICK_CAPACITY: usize, const BITMAP_CAPACITY: usize> {
    // Hot data (first cache line)
    pub sqrt_price_x96: U256,  // ← CHANGED: Was [u8; 20], now proper U256
    pub tick: i32,
    pub liquidity: u128,
    pub tick_count: u16,
    pub bitmap_count: u16,
    pub pool_tier: PoolTier,

    // ADDED: Critical metadata for calculations
    pub fee: u32,              // ← NEW: Fee in hundredths of bip (500 = 0.05%)
    pub tick_spacing: i32,     // ← NEW: Tick spacing (10, 60, 200, etc.)
    pub token0_decimals: u8,   // ← NEW: Token0 decimals for amount conversion
    pub token1_decimals: u8,   // ← NEW: Token1 decimals for amount conversion

    pub _pad: [u8; 2],  // Adjust padding as needed

    // Existing fields
    pub token0: [u8; 20],
    pub token1: [u8; 20],
    pub common: CommonPoolFields,
    pub ticks: [(i32, i128); TICK_CAPACITY],
    pub tick_bitmap: [(i16, [u8; 32]); BITMAP_CAPACITY],
}
```

#### 3. Add Uniswap V4 Pool Data

```rust
// src/uniswap_v4.rs

use alloy_primitives::U256;

#[repr(C, align(64))]
pub struct UniswapV4PoolData<const TICK_CAPACITY: usize, const BITMAP_CAPACITY: usize> {
    // Hot data
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub tick_count: u16,
    pub bitmap_count: u16,
    pub pool_tier: PoolTier,

    // V4-specific metadata
    pub pool_id: [u8; 32],     // ← 32-byte pool identifier
    pub fee: u32,              // ← Fee (dynamic in V4)
    pub tick_spacing: i32,     // ← Independent of fee in V4
    pub token0_decimals: u8,
    pub token1_decimals: u8,

    pub _pad: [u8; 10],        // Padding for alignment

    // Existing fields
    pub token0: [u8; 20],
    pub token1: [u8; 20],
    pub common: CommonPoolFields,
    pub ticks: [(i32, i128); TICK_CAPACITY],
    pub tick_bitmap: [(i16, [u8; 32]); BITMAP_CAPACITY],
}

// Note: Hook support omitted for initial implementation
// Only V4 pools with hooks = 0x0 are supported
// When adding hook support later, struct will be modified and arena rebuilt

// Type aliases for V4 tiers
pub type UniswapV4LowPoolData = UniswapV4PoolData<50, 10>;
pub type UniswapV4ActivePoolData = UniswapV4PoolData<200, 25>;
pub type UniswapV4PopularPoolData = UniswapV4PoolData<500, 50>;
pub type UniswapV4MajorPoolData = UniswapV4PoolData<1000, 100>;
```

#### 4. Update Registry for V4

```rust
// src/registry.rs

pub type UniswapV4Arena = ProtocolArena<
    UniswapV4LowPoolData,
    UniswapV4ActivePoolData,
    UniswapV4PopularPoolData,
    UniswapV4MajorPoolData,
>;

pub struct PoolArenaRegistry {
    uniswap_v3: UniswapV3Arena,
    uniswap_v2: UniswapV2Arena,
    uniswap_v4: UniswapV4Arena,  // ← ADD

    // Updated registry to support both 20-byte and 32-byte identifiers
    pool_registry: HashMap<PoolIdentifier, Protocol>,
}

impl PoolArenaRegistry {
    /// Add a Uniswap V4 pool
    pub fn add_uniswap_v4_pool(
        &mut self,
        pool_data: UniswapV4LowPoolData,
    ) -> Result<()> {
        let pool_id = PoolIdentifier::PoolId(pool_data.pool_id);

        if self.pool_registry.contains_key(&pool_id) {
            return Err(PoolError::PoolAlreadyExists { pool_id });
        }

        self.uniswap_v4.add_pool(pool_data)?;
        self.pool_registry.insert(pool_id, Protocol::UniswapV4);

        Ok(())
    }
}
```

#### 5. Update Protocol Enum

```rust
// src/protocol.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Protocol {
    UniswapV2 = 0,
    UniswapV3 = 1,
    UniswapV4 = 2,  // ← ADD
}
```

## Integration Flow

### 1. NATS Subscriber (Rust)

```rust
// In your poolStateArena integration crate

use async_nats;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct WhitelistMessage {
    #[serde(rename = "type")]
    update_type: String,  // "add", "remove", "full"
    pools: Vec<PoolMetadata>,
    chain: String,
    timestamp: String,
    snapshot_id: u64,
}

#[derive(Deserialize)]
struct PoolMetadata {
    address: String,                  // Pool Manager address
    #[serde(default)]
    pool_id: Option<String>,          // V4: 32-byte pool ID
    protocol: String,                 // "v2", "v3", "v4"
    fee: Option<u32>,                 // V3/V4: Required
    tick_spacing: Option<i32>,        // V3/V4: Required
    #[serde(default)]
    hooks: Option<String>,            // V4: Hook address
    factory: String,
    token0: TokenInfo,
    token1: TokenInfo,
}

#[derive(Deserialize)]
struct TokenInfo {
    address: String,
    decimals: u8,
    symbol: String,
    #[serde(default)]
    name: Option<String>,
}

async fn subscribe_to_whitelist(arena: Arc<Mutex<PoolArenaRegistry>>) -> Result<()> {
    let nc = async_nats::connect("nats://localhost:4222").await?;
    let mut sub = nc.subscribe("whitelist.pools.ethereum.full").await?;

    while let Some(msg) = sub.next().await {
        let whitelist: WhitelistMessage = serde_json::from_slice(&msg.payload)?;

        for pool_meta in whitelist.pools {
            match pool_meta.protocol.as_str() {
                "v3" => {
                    // 1. Scrape state from rethDB
                    let pool_state = scrape_v3_pool_from_rethdb(&pool_meta.address).await?;

                    // 2. Create pool data with metadata
                    let mut pool_data = UniswapV3LowPoolData::default();
                    pool_data.sqrt_price_x96 = pool_state.sqrt_price_x96;
                    pool_data.tick = pool_state.tick;
                    pool_data.liquidity = pool_state.liquidity;
                    pool_data.fee = pool_meta.fee.unwrap();
                    pool_data.tick_spacing = pool_meta.tick_spacing.unwrap();
                    pool_data.token0_decimals = pool_meta.token0.decimals;
                    pool_data.token1_decimals = pool_meta.token1.decimals;
                    pool_data.ticks = pool_state.ticks;
                    pool_data.tick_bitmap = pool_state.bitmaps;

                    // 3. Add to arena
                    arena.lock().unwrap().add_uniswap_v3_pool(pool_data)?;
                }
                "v4" => {
                    // Similar for V4...
                }
                _ => {}
            }
        }
    }

    Ok(())
}
```

### 2. rethDB Scraper Integration

Use your existing `~/scrape_rethdb_data` project to fetch pool state:

```rust
// Call your scraper for each pool
async fn scrape_v3_pool_from_rethdb(pool_address: &str) -> Result<V3PoolState> {
    // Use your existing scraper logic
    // This should query rethDB for:
    // - sqrt_price_x96
    // - tick
    // - liquidity
    // - initialized ticks with liquidity_net
    // - tick bitmap words

    // Return structured data ready to populate poolStateArena
    Ok(V3PoolState {
        sqrt_price_x96: ...,
        tick: ...,
        liquidity: ...,
        ticks: ...,
        bitmaps: ...,
    })
}
```

## Memory Impact

### Per-Pool Memory Addition

```
V3 Pool:
  fee:              4 bytes
  tick_spacing:     4 bytes
  token0_decimals:  1 byte
  token1_decimals:  1 byte
  ----------------
  Total:           10 bytes per pool

V4 Pool:
  pool_id:         32 bytes (in addition to address field)
  fee:              4 bytes
  tick_spacing:     4 bytes
  token0_decimals:  1 byte
  token1_decimals:  1 byte
  ----------------
  Total:           42 bytes per pool

Note: Hooks field NOT included (saves 20 bytes per pool)
When hooks are added later, arena will be rebuilt with updated struct.
```

### Total System Impact

For 800 pools (current arena capacity):
- V3 additions: 800 × 10 bytes = **8 KB**
- V4 additions: 800 × 12 bytes = **9.6 KB**
- **Total: ~18 KB additional memory** (negligible!)

## Testing Plan

1. **Unit Tests**: Test struct sizes and alignment
2. **Integration Tests**: Test NATS message parsing
3. **Load Tests**: Verify 800 pools load correctly
4. **Calculation Tests**: Verify swap calculations work with new fields

## Migration Path

1. ✅ Update dynamicWhitelist NATS messages (DONE)
2. ⏳ Update poolStateArena structs (IN PROGRESS)
3. ⏳ Create NATS subscriber in Rust
4. ⏳ Integrate rethDB scraper
5. ⏳ Test end-to-end flow
6. ⏳ Deploy and monitor

---

**Status**: Phase 1 (NATS messages) complete. Ready for poolStateArena struct updates.
