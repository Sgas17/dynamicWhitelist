# ExEx Integration Architecture

## Overview

This document describes the architecture for integrating three separate services:

1. **dynamicWhitelist** (this repo): Creates and publishes pool/token whitelists to NATS
2. **ExEx**: Filters blockchain events for whitelisted pools only
3. **poolStateArena**: Maintains real-time pool state using snapshots + ExEx events

## System Architecture

```
┌──────────────────────┐
│  dynamicWhitelist    │
│  (This Repo)         │
│                      │
│  - Build token list  │
│  - Find pools        │
│  - Filter by liq.    │
│  - Publish to NATS   │
└──────────┬───────────┘
           │ Publishes whitelist
           ↓
    ┌─────────────┐
    │    NATS     │
    └──────┬──────┘
           │
           ├─────────────────┐
           ↓                 ↓
    ┌─────────────┐   ┌─────────────┐
    │    ExEx     │   │poolStateArena│
    │             │   │              │
    │ - Cache WL  │   │ - Scrape     │
    │ - Filter    │   │ - Apply evts │
    │   events    │   │ - Maintain   │
    │ - Forward   │───┤   state      │
    └─────────────┘   └──────────────┘
         Events
```

## The Challenge

When scraping pool state in **poolStateArena**:

1. **Scraping takes time**: Processing hundreds/thousands of pools can span multiple blocks
2. **Different protocols have different speeds**: V2 (1 storage slot) is faster than V3/V4 (multiple ticks + bitmaps)
3. **Need consistent state**: poolStateArena needs to know from which block each snapshot is valid
4. **Ethereum blocks are fast**: 12-second block time means state changes during scraping

## Solution: Block-Synchronized Batch Scraping

### Core Architecture (poolStateArena Scraping)

```
dynamicWhitelist                  ExEx                    poolStateArena
      │                            │                            │
      │ Publish whitelist          │                            │
      ├───────────────────────────→│                            │
      │                            │ Cache whitelist            │
      │                            ├───────────────────────────→│
      │                            │                            │
      │                            │                     Block N│
      │                            │    ┌───────────────────────┤
      │                            │    │ Capture ref block N   │
      │                            │    │ Scrape batch 1 (V2)   │
      │                            │    └───────────────────────┤
      │                            │                            │
      │                        Block N events                   │
      │                            ├───────────────────────────→│
      │                            │   (Mint/Burn/Swap)         │
      │                            │                            │
      │                            │                   Block N+1│
      │                            │    ┌───────────────────────┤
      │                            │    │ Capture ref block N+1 │
      │                            │    │ Scrape batch 2 (V3)   │
      │                            │    │ Apply events > block N│
      │                            │    └───────────────────────┤
      │                            │                            │
      │                      Block N+1 events                   │
      │                            ├───────────────────────────→│
      │                            │                            │
      │                            │                   Block N+2│
      │                            │    ┌───────────────────────┤
      │                            │    │ Capture ref block N+2 │
      │                            │    │ Scrape batch 3 (V4)   │
      │                            │    │ Apply events > N+1    │
      │                            │    └───────────────────────┤
      │                            │                            │
      │                      Block N+2 events                   │
      │                            ├───────────────────────────→│
      │                            │                            │
      │                            │                  All synced│
      │                      Block N+3 events                   │
      │                            ├───────────────────────────→│
      │                            │   Real-time processing     │
```

### Key Principles

1. **Separation of Concerns**
   - **dynamicWhitelist**: Only creates and publishes whitelists (no state management)
   - **ExEx**: Only filters events for whitelisted pools (no state management)
   - **poolStateArena**: Only maintains pool state (scrapes + applies events)

2. **ExEx Caching**
   - ExEx caches whitelist on startup
   - Uses cached whitelist until new one published to NATS
   - Filters ALL events, forwarding only whitelisted pool events
   - No state updates - pure event filtering

3. **poolStateArena Batch Scraping**
   - Per-batch reference blocks captured BEFORE scraping
   - V2: ~200 pools/batch (fastest)
   - V3: ~50 pools/batch (moderate)
   - V4: ~30 pools/batch (slowest)
   - Batch sizes tuned to complete within ~80% of 12-second block time

4. **State Synchronization**
   - poolStateArena receives ExEx events continuously
   - For each batch: captures reference block, scrapes snapshot
   - Applies ExEx events received AFTER reference block
   - State converges as batches complete

## NATS Topics

### 1. Pool Whitelist - Minimal (For ExEx)

**Topic:** `whitelist.pools.{chain}.minimal`

**Publisher:** dynamicWhitelist

**Consumer:** ExEx (caches for event filtering)

**Published:** Once per whitelist update (typically hourly/daily)

**Message Format:**
```json
{
    "pools": [
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
        "0x..."
    ],
    "chain": "ethereum",
    "timestamp": "2025-10-31T14:25:30.123456+00:00"
}
```

**ExEx Usage:**
- Caches pool addresses on startup
- Updates cache when new whitelist published
- Uses cache to filter events: if `event.pool_address in cached_whitelist` → forward

### 2. Pool Whitelist - Full (For poolStateArena)

**Topic:** `whitelist.pools.{chain}.full`

**Publisher:** dynamicWhitelist

**Consumer:** poolStateArena (for metadata + batch scraping)

**Published:** Once per whitelist update

**Message Format:**
```json
{
    "pools": [
        {
            "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            "token0": {
                "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "decimals": 6,
                "symbol": "USDC"
            },
            "token1": {
                "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "decimals": 18,
                "symbol": "WETH"
            },
            "protocol": "UniswapV3",
            "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
            "fee": 500,
            "tick_spacing": 10
        }
    ],
    "chain": "ethereum",
    "timestamp": "2025-10-31T14:25:30.123456+00:00"
}
```

**poolStateArena Usage:**
- Receives full pool metadata
- Initiates batch scraping for these pools
- Uses metadata for proper scraping configuration

### 3. Snapshot Reference Block (Per Batch) - OPTIONAL

**Topic:** `whitelist.snapshots.{chain}.reference_block`

**Publisher:** dynamicWhitelist OR poolStateArena (depending on architecture)

**Consumer:** poolStateArena (for synchronization tracking)

**Note:** This topic is OPTIONAL and only needed if dynamicWhitelist performs the scraping.
If poolStateArena does its own scraping, it captures its own reference blocks internally.

**Message Format:**
```json
{
    "chain": "ethereum",
    "reference_block": 21234567,
    "snapshot_timestamp": "2025-10-31T14:20:06.439002+00:00",
    "metadata": {
        "batch_number": 1,
        "total_batches": 10,
        "protocol": "v2",
        "pools_in_batch": 200
    }
}
```

## ExEx Implementation (Rust Binary in Reth Node)

**ExEx (Execution Extension)** is a Rust binary compiled into the reth node that filters blockchain events and sends them via Unix socket to poolStateArena.

### Architecture

```
┌─────────────────────────────┐
│      Reth Node              │
│                             │
│  ┌──────────────────────┐   │
│  │   ExEx (Rust)        │   │
│  │                      │   │
│  │  - Sub to NATS WL    │   │
│  │  - Cache whitelist   │   │
│  │  - Filter events     │   │
│  │  - Send via socket   │   │
│  └──────────┬───────────┘   │
│             │                │
└─────────────┼────────────────┘
              │ Unix Socket
              │ (/tmp/exex.sock)
              ↓
       ┌──────────────────┐
       │ poolStateArena   │
       │ (Python/Rust)    │
       │                  │
       │ - Scrape batches │
       │ - Apply events   │
       │ - Maintain state │
       └──────────────────┘
              ↑
              │ NATS subscription
              │
       ┌──────┴──────┐
       │    NATS     │
       └─────────────┘
           ↑
           │ Publishes whitelist
   ┌───────┴───────┐
   │ dynamicWL     │
   └───────────────┘
```

### ExEx Responsibilities

1. **Subscribe to NATS**: `whitelist.pools.ethereum.minimal` to get pool addresses
2. **Cache Whitelist**: Store addresses in HashSet for fast lookup
3. **Filter Events**: For each block event, check if pool address is whitelisted
4. **Forward via Socket**: Send filtered events to poolStateArena via Unix socket

### Rust ExEx Pseudocode

```rust
// ExEx binary inside reth node
struct PoolWhitelistExEx {
    whitelisted_pools: HashSet<Address>,
    nats_client: NatsClient,
    socket_stream: UnixStream,  // Unix socket to poolStateArena
}

impl PoolWhitelistExEx {
    async fn on_whitelist_update(&mut self, msg: WhitelistMessage) {
        // Update cached whitelist from NATS
        self.whitelisted_pools = msg.pools.into_iter().collect();
        info!("Updated whitelist cache: {} pools", self.whitelisted_pools.len());
    }

    async fn on_committed_block(&mut self, block: Block) {
        // Process all events in block
        for log in block.logs {
            let pool_address = log.address;

            // Filter: only process whitelisted pools
            if !self.whitelisted_pools.contains(&pool_address) {
                continue;
            }

            // Determine event type
            let event = match log.topics[0] {
                MINT_TOPIC => parse_mint_event(log),
                BURN_TOPIC => parse_burn_event(log),
                SWAP_TOPIC => parse_swap_event(log),
                _ => continue,
            };

            // Send to poolStateArena via Unix socket
            self.send_event_via_socket(event).await;
        }
    }

    async fn on_reverted_block(&mut self, block: Block) {
        // Send reorg notification via socket
        let reorg_event = ReorgEvent {
            block_number: block.number,
            block_hash: block.hash,
        };
        self.send_event_via_socket(reorg_event).await;
    }

    async fn send_event_via_socket(&mut self, event: impl Serialize) {
        // Serialize and send over Unix socket
        let bytes = bincode::serialize(&event).unwrap();
        self.socket_stream.write_all(&bytes).await.unwrap();
    }
}
```

### Communication Protocol

**ExEx → poolStateArena (Unix Socket)**

Message format (bincode serialized):
```rust
enum PoolEvent {
    Mint {
        pool_address: Address,
        block_number: u64,
        tick_lower: i32,
        tick_upper: i32,
        liquidity: u128,
    },
    Burn {
        pool_address: Address,
        block_number: u64,
        tick_lower: i32,
        tick_upper: i32,
        liquidity: u128,
    },
    Swap {
        pool_address: Address,
        block_number: u64,
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
    },
    Reorg {
        block_number: u64,
        block_hash: H256,
    },
}
```

## poolStateArena Implementation (Rust Service)

poolStateArena is a separate Rust service that:
1. Subscribes to NATS for whitelist updates
2. Receives filtered events from ExEx via Unix socket
3. Batch-scrapes pools with per-batch reference blocks (using reth DB directly)
4. Applies ExEx events to maintain real-time state

### State Manager

```rust
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::net::UnixStream;

struct PoolState {
    address: Address,
    reference_block: u64,
    tick_data: HashMap<i32, TickInfo>,
    sqrt_price_x96: U256,
    liquidity: u128,
    last_event_block: u64,
}

struct PoolStateArena {
    pools: HashMap<Address, PoolState>,
    reth_db_path: PathBuf,
    exex_socket: UnixStream,
    nats_client: NatsClient,
}

impl PoolStateArena {
    async fn start(&mut self) -> Result<()> {
        // Subscribe to NATS for whitelist updates
        self.setup_nats().await?;

        // Connect to ExEx Unix socket
        self.connect_exex_socket().await?;

        // Start event processing loop
        tokio::spawn(async move {
            self.process_exex_events().await;
        });

        Ok(())
    }

    async fn setup_nats(&mut self) -> Result<()> {
        // Subscribe to pool whitelist
        self.nats_client
            .subscribe("whitelist.pools.ethereum.full")
            .await?;

        info!("✓ Subscribed to NATS whitelist topic");
        Ok(())
    }

    async fn connect_exex_socket(&mut self) -> Result<()> {
        self.exex_socket = UnixStream::connect("/tmp/exex.sock").await?;
        info!("✓ Connected to ExEx socket");
        Ok(())
    }

    async fn on_whitelist_update(&mut self, msg: WhitelistMessage) {
        let pools = msg.pools;
        info!("📥 Received whitelist: {} pools", pools.len());

        // Start batch scraping
        self.batch_scrape_pools(pools).await;
    }

    async fn batch_scrape_pools(&mut self, pools: Vec<PoolMetadata>) {
        info!("🔄 Starting batch scraping for {} pools", pools.len());

        // Group by protocol
        let (v2_pools, v3_pools, v4_pools) = self.group_by_protocol(pools);

        // Batch scraping with per-batch reference blocks
        // Similar to dynamicWhitelist's BatchScraper but in Rust

        let web3 = Web3::new(Http::new("http://localhost:8545").unwrap());

        // Batch 1: V2 pools (fast - ~200 pools)
        if !v2_pools.is_empty() {
            let ref_block = web3.eth().block_number().await.unwrap();
            info!("📍 Batch 1 (V2): {} pools at block {}", v2_pools.len(), ref_block);

            for pool in v2_pools {
                let state = scrape_v2_pool(&self.reth_db_path, &pool).await;
                self.pools.insert(pool.address, PoolState {
                    address: pool.address,
                    reference_block: ref_block.as_u64(),
                    ...state,
                });
            }

            // Wait for next block
            self.wait_for_next_block(ref_block).await;
        }

        // Batch 2: V3 pools (moderate - ~30 pools)
        if !v3_pools.is_empty() {
            let ref_block = web3.eth().block_number().await.unwrap();
            info!("📍 Batch 2 (V3): {} pools at block {}", v3_pools.len(), ref_block);

            for pool in v3_pools {
                let state = scrape_v3_pool(&self.reth_db_path, &pool).await;
                self.pools.insert(pool.address, PoolState {
                    address: pool.address,
                    reference_block: ref_block.as_u64(),
                    ...state,
                });
            }

            self.wait_for_next_block(ref_block).await;
        }

        // Batch 3: V4 pools (slow - ~20 pools)
        if !v4_pools.is_empty() {
            let ref_block = web3.eth().block_number().await.unwrap();
            info!("📍 Batch 3 (V4): {} pools at block {}", v4_pools.len(), ref_block);

            for pool in v4_pools {
                let state = scrape_v4_pool(&self.reth_db_path, &pool).await;
                self.pools.insert(pool.address, PoolState {
                    address: pool.address,
                    reference_block: ref_block.as_u64(),
                    ...state,
                });
            }
        }

        info!("✅ Scraping complete: {} pools initialized", self.pools.len());
    }

    async fn wait_for_next_block(&self, current_block: U64) {
        let web3 = Web3::new(Http::new("http://localhost:8545").unwrap());
        let target_block = current_block + 1;

        info!("⏳ Waiting for block {}...", target_block);

        loop {
            let block = web3.eth().block_number().await.unwrap();
            if block >= target_block {
                info!("✓ Block {} arrived", block);
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn process_exex_events(&mut self) {
        // Read events from Unix socket
        let mut buf = vec![0u8; 4096];

        loop {
            match self.exex_socket.read(&mut buf).await {
                Ok(n) if n > 0 => {
                    // Deserialize event (bincode)
                    let event: PoolEvent = bincode::deserialize(&buf[..n]).unwrap();
                    self.apply_event(event).await;
                }
                Ok(_) => {
                    error!("ExEx socket closed");
                    break;
                }
                Err(e) => {
                    error!("Error reading from socket: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn apply_event(&mut self, event: PoolEvent) {
        let pool_addr = event.pool_address();
        let block_number = event.block_number();

        // Check if pool exists
        let Some(pool) = self.pools.get_mut(&pool_addr) else {
            return;
        };

        // Only apply events AFTER reference block
        if block_number <= pool.reference_block {
            return;
        }

        // Apply event
        match event {
            PoolEvent::Mint { tick_lower, tick_upper, liquidity, .. } => {
                self.handle_mint(pool, tick_lower, tick_upper, liquidity);
            }
            PoolEvent::Burn { tick_lower, tick_upper, liquidity, .. } => {
                self.handle_burn(pool, tick_lower, tick_upper, liquidity);
            }
            PoolEvent::Swap { sqrt_price_x96, tick, liquidity, .. } => {
                self.handle_swap(pool, sqrt_price_x96, tick, liquidity);
            }
            PoolEvent::Reorg { .. } => {
                warn!("Reorg detected for pool {:?}", pool_addr);
                // Handle reorg (may need to rescrape)
            }
        }

        pool.last_event_block = block_number;
    }

    fn handle_mint(&mut self, pool: &mut PoolState, tick_lower: i32, tick_upper: i32, liquidity: u128) {
        // Update tick data with new liquidity
        for tick in [tick_lower, tick_upper] {
            let tick_info = pool.tick_data.entry(tick).or_insert(TickInfo::default());
            tick_info.liquidity_gross += liquidity;
        }
    }

    fn handle_burn(&mut self, pool: &mut PoolState, tick_lower: i32, tick_upper: i32, liquidity: u128) {
        // Remove liquidity from ticks
        for tick in [tick_lower, tick_upper] {
            if let Some(tick_info) = pool.tick_data.get_mut(&tick) {
                tick_info.liquidity_gross = tick_info.liquidity_gross.saturating_sub(liquidity);
            }
        }
    }

    fn handle_swap(&mut self, pool: &mut PoolState, sqrt_price_x96: U256, tick: i32, liquidity: u128) {
        // Update current price and liquidity
        pool.sqrt_price_x96 = sqrt_price_x96;
        pool.liquidity = liquidity;
    }
}
```

### Python Example (For Reference)

If you prefer to prototype in Python first, here's equivalent code:

```python
class PoolStateArena:
    """
    Maintains real-time pool state using:
    1. Batch scraping with per-batch reference blocks
    2. ExEx events received via Unix socket
    """

    def __init__(self, reth_db_path: str, exex_socket_path: str = "/tmp/exex.sock"):
        self.pools = {}  # pool_address -> pool_state
        self.reth_db_path = reth_db_path
        self.exex_socket_path = exex_socket_path
        self.socket_reader = None

    async def start(self):
        """Start the pool state arena."""
        # Subscribe to NATS for whitelist updates
        await self.setup_nats()

        # Connect to ExEx Unix socket
        await self.connect_exex_socket()

        # Start event processing loop
        asyncio.create_task(self.process_exex_events())

    async def setup_nats(self):
        """Subscribe to NATS for pool whitelist."""
        self.nc = await nats.connect("nats://localhost:4222")

        await self.nc.subscribe(
            "whitelist.pools.ethereum.full",
            cb=self.on_whitelist_update
        )

    async def connect_exex_socket(self):
        """Connect to ExEx Unix socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.exex_socket_path)
        self.socket_reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(self.socket_reader)
        await asyncio.get_event_loop().connect_accepted_socket(
            lambda: protocol, sock
        )
        logger.info(f"✓ Connected to ExEx socket: {self.exex_socket_path}")

    async def on_whitelist_update(self, msg):
        """Handle whitelist update from NATS."""
        data = json.loads(msg.data.decode())
        pools = data['pools']

        logger.info(f"📥 Received whitelist: {len(pools)} pools")

        # Start batch scraping for new whitelist
        await self.batch_scrape_pools(pools)

    async def batch_scrape_pools(self, pools: List[dict]):
        """
        Batch scrape pools with per-batch reference blocks.

        Uses BatchScraper from dynamicWhitelist codebase.
        """
        from src.whitelist.batch_scraper import BatchScraper, BatchConfig
        from src.processors.pools.reth_snapshot_loader import RethSnapshotLoader
        from web3 import Web3

        # Initialize components
        web3 = Web3(Web3.HTTPProvider("http://localhost:8545"))
        reth_loader = RethSnapshotLoader(self.reth_db_path)

        # Configure batch scraping
        config = BatchConfig(
            v2_batch_size=200,
            v3_full_ticks_batch_size=30,
            v4_full_ticks_batch_size=20,
            wait_for_next_block=True  # Wait between batches
        )

        batch_scraper = BatchScraper(
            web3=web3,
            reth_loader=reth_loader,
            config=config
        )

        # Convert pools list to dict format
        pools_dict = {p['address']: p for p in pools}

        # Scrape all batches
        logger.info(f"🔄 Starting batch scraping for {len(pools_dict)} pools")

        results = await batch_scraper.scrape_all_batches(
            pools=pools_dict,
            batch_type="full_ticks",
            nats_publisher=None  # poolStateArena tracks its own ref blocks
        )

        # Store scraped data
        for batch in results['batches']:
            reference_block = batch['reference_block']
            logger.info(
                f"✓ Batch {batch['batch_number']}: {batch['pools_scraped']} pools "
                f"at block {reference_block}"
            )

        # Mark pools as initialized
        for pool_data in results['scraped_data']:
            pool_addr = pool_data['address'].lower()
            self.pools[pool_addr] = {
                **pool_data,
                'reference_block': pool_data.get('reference_block'),
                'initialized': True,
                'last_event_block': pool_data.get('reference_block', 0)
            }

        logger.info(f"✅ Scraping complete: {len(self.pools)} pools initialized")

    async def process_exex_events(self):
        """
        Process events from ExEx via Unix socket.

        Continuously reads events and applies them to pool state.
        """
        while True:
            try:
                # Read event from socket (bincode serialized)
                event_bytes = await self.socket_reader.readuntil(b'\n')
                event = self.deserialize_event(event_bytes)

                await self.apply_event(event)

            except Exception as e:
                logger.error(f"Error processing ExEx event: {e}")
                await asyncio.sleep(1)

    async def apply_event(self, event: dict):
        """
        Apply a single event from ExEx to pool state.

        Only applies events with block_number > pool's reference_block.
        """
        pool_addr = event['pool_address'].lower()
        block_number = event['block_number']

        # Check if pool exists
        if pool_addr not in self.pools:
            return

        pool = self.pools[pool_addr]

        # Only apply events AFTER the pool's reference block
        if block_number <= pool.get('reference_block', 0):
            return

        # Apply event based on type
        event_type = event['type']

        if event_type == 'Mint':
            await self.handle_mint(pool_addr, event)
        elif event_type == 'Burn':
            await self.handle_burn(pool_addr, event)
        elif event_type == 'Swap':
            await self.handle_swap(pool_addr, event)
        elif event_type == 'Reorg':
            await self.handle_reorg(pool_addr, event)

        # Update last event block
        pool['last_event_block'] = block_number

    async def apply_pool_update(self, pool_addr: str, event: dict, block_number: int):
        """
        Apply a pool state update from ExEx.

        Handles:
        - Mint events (add liquidity)
        - Burn events (remove liquidity)
        - Swap events (change price/tick)
        - Reorg events (revert state)
        """
        event_type = event.get('type')

        if event_type == 'Mint':
            # Add liquidity to tick range
            await self.handle_mint(pool_addr, event)

        elif event_type == 'Burn':
            # Remove liquidity from tick range
            await self.handle_burn(pool_addr, event)

        elif event_type == 'Swap':
            # Update price and current tick
            await self.handle_swap(pool_addr, event)

        elif event_type == 'Reorg':
            # Revert to previous state
            await self.handle_reorg(pool_addr, event)

        # Update last processed block
        self.pools[pool_addr]['last_update_block'] = block_number

    async def handle_mint(self, pool_addr: str, event: dict):
        """Handle liquidity addition event."""
        tick_lower = event['tickLower']
        tick_upper = event['tickUpper']
        liquidity = event['amount']

        pool = self.pools[pool_addr]

        # Update tick liquidity
        if 'tick_data' not in pool:
            pool['tick_data'] = {}

        for tick in [tick_lower, tick_upper]:
            if tick not in pool['tick_data']:
                pool['tick_data'][tick] = {
                    'liquidity_gross': 0,
                    'liquidity_net': 0
                }

            # Update tick (simplified - actual logic more complex)
            pool['tick_data'][tick]['liquidity_gross'] += liquidity

    async def handle_burn(self, pool_addr: str, event: dict):
        """Handle liquidity removal event."""
        # Similar to handle_mint but subtract liquidity
        pass

    async def handle_swap(self, pool_addr: str, event: dict):
        """Handle swap event."""
        pool = self.pools[pool_addr]

        # Update current state
        pool['sqrtPriceX96'] = event['sqrtPriceX96']
        pool['tick'] = event['tick']
        pool['liquidity'] = event['liquidity']

    async def handle_reorg(self, pool_addr: str, event: dict):
        """Handle chain reorganization."""
        # Revert to previous state
        # In practice, might need to rescrape affected pools
        logger.warning(f"Reorg detected for pool {pool_addr}, may need rescrape")
```

### NATS Subscription Setup

```python
async def setup_nats_subscriptions():
    """Set up NATS subscriptions for pool state management."""
    nc = await nats.connect("nats://localhost:4222")

    state_manager = PoolStateManager()

    # Subscribe to reference blocks (per batch)
    await nc.subscribe(
        "whitelist.snapshots.ethereum.reference_block",
        cb=lambda msg: state_manager.on_snapshot_reference_block(
            json.loads(msg.data.decode())
        )
    )

    # Subscribe to minimal pool list (for address tracking)
    await nc.subscribe(
        "whitelist.pools.ethereum.minimal",
        cb=lambda msg: state_manager.on_pool_whitelist_minimal(
            json.loads(msg.data.decode())
        )
    )

    # Subscribe to full pool list (for metadata)
    await nc.subscribe(
        "whitelist.pools.ethereum.full",
        cb=lambda msg: state_manager.on_pool_whitelist_full(
            json.loads(msg.data.decode())
        )
    )

    logger.info("✓ NATS subscriptions active")

    return state_manager
```

## Batch Scraper Configuration

The [BatchScraper](../src/whitelist/batch_scraper.py) class handles time-bounded batch scraping:

```python
from src.whitelist.batch_scraper import BatchScraper, BatchConfig

# Configure batch sizes (tune via profiling)
config = BatchConfig(
    block_time_seconds=12.0,
    safety_margin=0.8,  # Use 80% of block time
    v2_batch_size=200,
    v3_slot0_batch_size=150,
    v4_slot0_batch_size=100,
    v3_full_ticks_batch_size=30,
    v4_full_ticks_batch_size=20,
    wait_for_next_block=True  # Wait between batches
)

# Initialize scraper
batch_scraper = BatchScraper(
    web3=web3,
    reth_loader=reth_loader,
    config=config
)

# Scrape with per-batch reference blocks
results = await batch_scraper.scrape_all_batches(
    pools=pools_dict,
    batch_type="filtering",  # or "full_ticks"
    nats_publisher=nats_publisher
)
```

## Performance Tuning

### Profiling Script

Use [profile_scraping_performance.py](../src/tools/profile_scraping_performance.py) to determine optimal batch sizes:

```bash
# Set reth DB path
export RETH_DB_PATH=/path/to/reth/db

# Run profiling
uv run python src/tools/profile_scraping_performance.py
```

**Output:**
```
PERFORMANCE SUMMARY
Filtering Phase (slot0 only for V3/V4):
  V2: 45.2 pools/sec → Batch size: 434
  V3: 22.8 pools/sec → Batch size: 219
  V4: 18.3 pools/sec → Batch size: 176

Full Tick Collection Phase (post-filter):
  V3: 8.5 pools/sec → Batch size: 82
  V4: 6.2 pools/sec → Batch size: 60
```

### Tuning Recommendations

1. **Start Conservative**: Use 80% safety margin to ensure batches complete within block time
2. **Monitor Performance**: Track actual scraping times vs block times
3. **Adjust Gradually**: Increase batch sizes if consistently completing early
4. **Consider Network Conditions**: Adjust for mainnet vs testnet block variability

## Summary: How It All Works Together

### 1. **dynamicWhitelist** (This Repo)
- Builds token whitelist from cross-chain, hyperliquid, lighter, top transfers
- Finds pools containing whitelisted tokens
- Filters pools by liquidity thresholds
- Publishes to NATS:
  - `whitelist.pools.ethereum.minimal` → ExEx caches addresses
  - `whitelist.pools.ethereum.full` → poolStateArena gets metadata

### 2. **ExEx** (Rust Binary in Reth Node)
- Subscribes to NATS minimal whitelist
- Caches whitelisted pool addresses in HashSet
- Filters ALL blockchain events
- Forwards only whitelisted pool events via Unix socket to poolStateArena
- **No state management** - pure event filtering

### 3. **poolStateArena** (Rust Service)
- Subscribes to NATS full whitelist
- Receives whitelist → starts batch scraping
- Batch scraping with per-batch reference blocks:
  - Block N: Scrape batch 1 (V2 ~200 pools), capture ref block N
  - Wait for block N+1
  - Block N+1: Scrape batch 2 (V3 ~30 pools), capture ref block N+1
  - Wait for block N+2
  - Block N+2: Scrape batch 3 (V4 ~20 pools), capture ref block N+2
- Receives ExEx events via Unix socket (`/tmp/exex.sock`)
- Applies events only if `event.block > pool.reference_block`
- Maintains real-time, synchronized pool state
- **Written in Rust** for performance (can use reth DB directly)

### Key Benefits

✅ **Clear Separation**: Each service has one responsibility
✅ **No State Gaps**: Per-batch reference blocks ensure sync
✅ **Protocol-Optimized**: V2/V3/V4 batches sized appropriately
✅ **Unix Socket**: Fast IPC between ExEx and poolStateArena
✅ **Cached Whitelist**: ExEx filters efficiently with HashSet lookup
✅ **Real-Time**: State stays current through continuous ExEx events

## Failure Handling

### Batch Scraping Fails

If a batch fails:
1. BatchResult marks `success=False` with error message
2. Continue with remaining batches
3. Re-scrape failed batch after completion
4. ExEx continues processing, failed pools get updates when re-scraped

### NATS Publishing Fails

If reference block publishing fails:
1. Log error but continue scraping
2. ExEx may miss batch boundary (graceful degradation)
3. Final full snapshot still published
4. State converges once all pools scraped

### ExEx Connection Lost

If ExEx disconnects:
1. Continue scraping batches
2. Publish reference blocks to NATS (buffered)
3. When ExEx reconnects, processes buffered messages
4. May need to rescrape pools updated during downtime

## Future Enhancements

1. **Finalized Blocks**: Option to wait for finalized blocks before scraping
2. **Parallel Batch Scraping**: Scrape multiple protocols concurrently
3. **Adaptive Batch Sizing**: Dynamically adjust based on real-time performance
4. **Checkpoint/Resume**: Resume scraping from last completed batch on restart
5. **Multi-Chain Coordination**: Coordinate scraping across multiple chains

## Related Documentation

- [Snapshot Reference Block](./snapshot_reference_block.md)
- [Pool Whitelist NATS Publisher](../src/core/storage/pool_whitelist_publisher.py)
- [Batch Scraper](../src/whitelist/batch_scraper.py)
- [Whitelist Orchestrator](../src/whitelist/orchestrator.py)

## Testing

### Unit Tests

Test batch scraping logic:
```bash
uv run pytest src/whitelist/tests/test_batch_scraper.py -v
```

### Integration Test

Test full pipeline with ExEx simulation:
```bash
uv run python src/tools/test_exex_integration.py
```

## Monitoring Metrics

Key metrics to track:

| Metric | Target | Alert |
|--------|--------|-------|
| Batch completion time | < 9.6s (80% of 12s) | > 11s |
| Batches per scraping run | N/A | Failed batches > 5% |
| Reference block publishing success | 100% | < 95% |
| ExEx lag (blocks behind) | < 5 blocks | > 20 blocks |
| State convergence time | < 5 minutes | > 10 minutes |

---

**Last Updated:** 2025-10-31
**Version:** 1.0
**Author:** Dynamic Whitelist Team
