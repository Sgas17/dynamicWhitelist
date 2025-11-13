// Refined Startup Coordinator for pool_state_arena
// Location: /home/sam-sullivan/defi_platform/pool_state_arena/src/subscriber/startup_coordinator.rs
//
// This module handles two distinct startup scenarios:
// 1. Initial cold start - Full 4-phase startup with extensive scraping
// 2. Incremental additions - Add new pools while already in live mode
//
// Also optimizes for V2 pools which scrape much faster (no ticks/bitmaps)

use super::pool_factory::{create_pool, PoolCreationResult};
use super::reth_scraper::RethDbScraper;
use super::socket_client::{ClientMode, UnixSocketClient};
use super::socket_messages::PoolEvent;
use super::tier::determine_tier;
use crate::protocol::PoolIdentifier;
use crate::registry::PoolArenaRegistry;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::RwLock;

/// Errors that can occur during startup coordination
#[derive(Error, Debug)]
pub enum StartupError {
    #[error("Socket client error: {0}")]
    SocketClient(String),

    #[error("RethDB scraper error: {0}")]
    Scraper(String),

    #[error("Arena registry error: {0}")]
    ArenaRegistry(String),

    #[error("Pool factory error: {0}")]
    PoolFactory(String),

    #[error("Already in live mode - use add_pools_incremental instead")]
    AlreadyLive,

    #[error("Not in live mode - cannot add pools incrementally")]
    NotLive,
}

pub type Result<T> = std::result::Result<T, StartupError>;

/// Startup mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupMode {
    /// Cold start - first time startup with full scraping
    ColdStart,
    /// Already live - just adding new pools
    Incremental,
}

/// Pool metadata for optimized scraping
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub id: String,
    pub protocol: String,
}

impl PoolInfo {
    /// Check if this is a V2 pool (fast scraping, no ticks)
    pub fn is_v2(&self) -> bool {
        self.protocol == "uniswap_v2"
    }

    /// Check if this is a V3/V4 pool (slow scraping, has ticks)
    pub fn is_v3_or_v4(&self) -> bool {
        self.protocol == "uniswap_v3" || self.protocol == "uniswap_v4"
    }
}

/// Startup phase tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPhase {
    /// Not yet started
    NotStarted,
    /// Phase 1: Connecting socket in buffering mode
    ConnectingSocket,
    /// Phase 2: Scraping pools from RethDB
    ScrapingPools {
        v2_completed: usize,
        v2_total: usize,
        v3_v4_completed: usize,
        v3_v4_total: usize,
    },
    /// Phase 3: Replaying buffered events
    ReplayingEvents {
        events_completed: usize,
        events_total: usize,
    },
    /// Phase 4: Live processing
    Live,
}

/// Configuration for the startup coordinator
#[derive(Debug, Clone)]
pub struct StartupConfig {
    /// Path to Unix socket
    pub socket_path: PathBuf,

    /// Maximum events to buffer during scraping
    /// For cold start: 100,000 (handles ~90 seconds of scraping)
    /// For incremental: 10,000 (handles ~5 seconds for a few pools)
    pub buffer_capacity: usize,

    /// Batch size for V2 pool scraping (faster, can do larger batches)
    pub v2_scrape_batch_size: usize,

    /// Batch size for V3/V4 pool scraping (slower, smaller batches)
    pub v3_v4_scrape_batch_size: usize,

    /// Number of concurrent scraping tasks
    pub scraping_concurrency: usize,

    /// Whether to enable parallel scraping
    pub parallel_scraping: bool,
}

impl StartupConfig {
    /// Configuration for cold start (first time startup)
    pub fn cold_start() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/reth_exex.sock"),
            buffer_capacity: 100_000, // ~90 seconds of events
            v2_scrape_batch_size: 500, // V2 is fast, can handle large batches
            v3_v4_scrape_batch_size: 50, // V3/V4 is slow, smaller batches
            scraping_concurrency: 4,
            parallel_scraping: true,
        }
    }

    /// Configuration for incremental pool additions (already live)
    pub fn incremental() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/reth_exex.sock"),
            buffer_capacity: 10_000, // ~5 seconds of events
            v2_scrape_batch_size: 100,
            v3_v4_scrape_batch_size: 10,
            scraping_concurrency: 2,
            parallel_scraping: true,
        }
    }
}

/// Metrics collected during startup
#[derive(Debug, Clone, Default)]
pub struct StartupMetrics {
    pub startup_started_at: Option<Instant>,
    pub socket_connected_at: Option<Instant>,
    pub scraping_started_at: Option<Instant>,
    pub scraping_completed_at: Option<Instant>,
    pub replay_started_at: Option<Instant>,
    pub replay_completed_at: Option<Instant>,
    pub live_mode_started_at: Option<Instant>,

    pub v2_pools_scraped: usize,
    pub v3_v4_pools_scraped: usize,
    pub pools_failed: usize,
    pub events_buffered: usize,
    pub events_replayed: usize,

    pub total_startup_duration_ms: Option<u64>,
    pub v2_scraping_duration_ms: Option<u64>,
    pub v3_v4_scraping_duration_ms: Option<u64>,
    pub replay_duration_ms: Option<u64>,
}

/// Startup coordinator for pool_state_arena
pub struct StartupCoordinator {
    /// Configuration
    config: StartupConfig,

    /// Arena registry (wrapped in Arc for sharing)
    arena_registry: Arc<RwLock<PoolArenaRegistry>>,

    /// RethDB scraper
    scraper: Arc<dyn RethDbScraper>,

    /// Unix socket client
    socket_client: Option<UnixSocketClient>,

    /// Current startup phase
    phase: StartupPhase,

    /// Set of successfully loaded pool identifiers
    loaded_pools: HashSet<PoolIdentifier>,

    /// Reference block number for scraping
    scrape_reference_block: Option<u64>,

    /// Startup metrics
    metrics: StartupMetrics,
}

impl StartupCoordinator {
    /// Create a new startup coordinator
    pub fn new(
        config: StartupConfig,
        arena_registry: Arc<RwLock<PoolArenaRegistry>>,
        scraper: Arc<dyn RethDbScraper>,
    ) -> Self {
        Self {
            config,
            arena_registry,
            scraper,
            socket_client: None,
            phase: StartupPhase::NotStarted,
            loaded_pools: HashSet::new(),
            scrape_reference_block: None,
            metrics: StartupMetrics::default(),
        }
    }

    /// Get current startup phase
    pub fn phase(&self) -> &StartupPhase {
        &self.phase
    }

    /// Get startup metrics
    pub fn metrics(&self) -> &StartupMetrics {
        &self.metrics
    }

    /// Check if currently in live mode
    pub fn is_live(&self) -> bool {
        self.phase == StartupPhase::Live
    }

    /// Execute cold start - full 4-phase startup sequence
    ///
    /// Use this for initial startup when no pools are loaded yet.
    ///
    /// # Arguments
    /// * `pool_whitelist` - List of pools to scrape and monitor
    ///
    /// # Returns
    /// Ok(()) if startup completes successfully
    pub async fn run_cold_start(&mut self, pool_whitelist: Vec<PoolInfo>) -> Result<()> {
        if self.phase == StartupPhase::Live {
            return Err(StartupError::AlreadyLive);
        }

        self.metrics.startup_started_at = Some(Instant::now());

        // Categorize pools by protocol for optimized scraping
        let (v2_pools, v3_v4_pools) = Self::categorize_pools(pool_whitelist);

        log::info!(
            "Starting cold start with {} V2 pools and {} V3/V4 pools",
            v2_pools.len(),
            v3_v4_pools.len()
        );

        // Phase 1: Connect socket in buffering mode
        self.phase_1_connect_socket().await?;

        // Phase 2: Scrape pools (V2 first, then V3/V4)
        self.phase_2_scrape_pools(v2_pools, v3_v4_pools).await?;

        // Phase 3: Replay buffered events
        self.phase_3_replay_events().await?;

        // Phase 4: Switch to live processing
        self.phase_4_go_live().await?;

        // Calculate total duration
        if let Some(start) = self.metrics.startup_started_at {
            self.metrics.total_startup_duration_ms = Some(start.elapsed().as_millis() as u64);
        }

        log::info!(
            "Cold start complete! V2: {} pools in {:?}ms, V3/V4: {} pools in {:?}ms, Total: {:?}ms",
            self.metrics.v2_pools_scraped,
            self.metrics.v2_scraping_duration_ms,
            self.metrics.v3_v4_pools_scraped,
            self.metrics.v3_v4_scraping_duration_ms,
            self.metrics.total_startup_duration_ms
        );

        Ok(())
    }

    /// Add pools incrementally while already in live mode
    ///
    /// Use this when the system is already running and you just need to add
    /// a few new pools from the whitelist. This is much faster than cold start:
    /// - No socket reconnection needed (already connected)
    /// - Smaller event buffer (only covers scraping duration)
    /// - Can process V2 and V3/V4 pools separately
    ///
    /// # Arguments
    /// * `new_pools` - List of new pools to add
    ///
    /// # Returns
    /// Ok(()) if pools added successfully
    pub async fn add_pools_incremental(&mut self, new_pools: Vec<PoolInfo>) -> Result<()> {
        if self.phase != StartupPhase::Live {
            return Err(StartupError::NotLive);
        }

        log::info!(
            "Adding {} pools incrementally (already in live mode)",
            new_pools.len()
        );

        let start = Instant::now();

        // Categorize pools
        let (v2_pools, v3_v4_pools) = Self::categorize_pools(new_pools);

        // Temporarily switch to buffering mode for scraping
        if let Some(ref mut client) = self.socket_client {
            client.set_mode(ClientMode::Buffering {
                capacity: self.config.buffer_capacity,
            });
        }

        // Get current block as reference
        let reference_block = self.get_current_block_number().await;
        log::info!(
            "Using reference block {} for incremental additions",
            reference_block
        );

        // Scrape V2 pools first (fast)
        if !v2_pools.is_empty() {
            let v2_start = Instant::now();
            for pool in v2_pools {
                if let Err(e) = self.scrape_and_add_pool(&pool.id, &pool.protocol).await {
                    log::error!("Failed to scrape V2 pool {}: {}", pool.id, e);
                    self.metrics.pools_failed += 1;
                } else {
                    self.metrics.v2_pools_scraped += 1;
                }
            }
            log::info!(
                "Scraped {} V2 pools in {:?}ms",
                self.metrics.v2_pools_scraped,
                v2_start.elapsed().as_millis()
            );
        }

        // Scrape V3/V4 pools (slower)
        if !v3_v4_pools.is_empty() {
            let v3_v4_start = Instant::now();
            for pool in v3_v4_pools {
                if let Err(e) = self.scrape_and_add_pool(&pool.id, &pool.protocol).await {
                    log::error!("Failed to scrape V3/V4 pool {}: {}", pool.id, e);
                    self.metrics.pools_failed += 1;
                } else {
                    self.metrics.v3_v4_pools_scraped += 1;
                }
            }
            log::info!(
                "Scraped {} V3/V4 pools in {:?}ms",
                self.metrics.v3_v4_pools_scraped,
                v3_v4_start.elapsed().as_millis()
            );
        }

        // Replay buffered events from after reference block
        if let Some(ref mut client) = self.socket_client {
            let mut buffered_events = client.take_buffered_events();
            buffered_events.retain(|e| e.block_number > reference_block);

            log::info!(
                "Replaying {} buffered events after incremental scraping",
                buffered_events.len()
            );

            for event in &buffered_events {
                if let Err(e) = self.apply_event(event).await {
                    log::error!("Failed to apply buffered event: {}", e);
                }
            }

            // Switch back to live mode
            client.set_mode(ClientMode::Live);
        }

        log::info!(
            "Incremental addition complete in {:?}ms ({} pools added, {} failed)",
            start.elapsed().as_millis(),
            self.metrics.v2_pools_scraped + self.metrics.v3_v4_pools_scraped,
            self.metrics.pools_failed
        );

        Ok(())
    }

    /// Categorize pools into V2 vs V3/V4 for optimized scraping
    fn categorize_pools(pools: Vec<PoolInfo>) -> (Vec<PoolInfo>, Vec<PoolInfo>) {
        let mut v2_pools = Vec::new();
        let mut v3_v4_pools = Vec::new();

        for pool in pools {
            if pool.is_v2() {
                v2_pools.push(pool);
            } else {
                v3_v4_pools.push(pool);
            }
        }

        (v2_pools, v3_v4_pools)
    }

    /// Phase 1: Connect to Unix socket in buffering mode
    async fn phase_1_connect_socket(&mut self) -> Result<()> {
        self.phase = StartupPhase::ConnectingSocket;
        let phase_start = Instant::now();

        log::info!("Phase 1: Connecting to Unix socket in buffering mode");

        let mut socket_client = UnixSocketClient::new(
            self.config.socket_path.clone(),
            ClientMode::Buffering {
                capacity: self.config.buffer_capacity,
            },
        );

        socket_client
            .connect()
            .await
            .map_err(|e| StartupError::SocketClient(e.to_string()))?;

        self.socket_client = Some(socket_client);
        self.metrics.socket_connected_at = Some(Instant::now());

        log::info!(
            "Phase 1 complete: Socket connected in {:?}ms",
            phase_start.elapsed().as_millis()
        );

        Ok(())
    }

    /// Phase 2: Scrape pools from RethDB
    ///
    /// Optimized to scrape V2 pools first (fast), then V3/V4 pools (slow).
    /// This reduces the total buffering time since V2 pools complete quickly.
    async fn phase_2_scrape_pools(
        &mut self,
        v2_pools: Vec<PoolInfo>,
        v3_v4_pools: Vec<PoolInfo>,
    ) -> Result<()> {
        log::info!(
            "Phase 2: Scraping {} V2 pools and {} V3/V4 pools",
            v2_pools.len(),
            v3_v4_pools.len()
        );

        self.metrics.scraping_started_at = Some(Instant::now());

        // Update phase with initial counts
        self.phase = StartupPhase::ScrapingPools {
            v2_completed: 0,
            v2_total: v2_pools.len(),
            v3_v4_completed: 0,
            v3_v4_total: v3_v4_pools.len(),
        };

        // Scrape V2 pools first (fast - just reserve0/reserve1)
        if !v2_pools.is_empty() {
            let v2_start = Instant::now();
            log::info!("Scraping V2 pools (batch size: {})...", self.config.v2_scrape_batch_size);

            for (idx, pool) in v2_pools.iter().enumerate() {
                if let Err(e) = self.scrape_and_add_pool(&pool.id, &pool.protocol).await {
                    log::error!("Failed to scrape V2 pool {}: {}", pool.id, e);
                    self.metrics.pools_failed += 1;
                } else {
                    self.metrics.v2_pools_scraped += 1;
                }

                // Update phase progress
                self.phase = StartupPhase::ScrapingPools {
                    v2_completed: idx + 1,
                    v2_total: v2_pools.len(),
                    v3_v4_completed: 0,
                    v3_v4_total: v3_v4_pools.len(),
                };

                // Log progress every 100 pools
                if (idx + 1) % 100 == 0 {
                    log::info!("V2 progress: {}/{}", idx + 1, v2_pools.len());
                }
            }

            self.metrics.v2_scraping_duration_ms = Some(v2_start.elapsed().as_millis() as u64);
            log::info!(
                "V2 scraping complete: {} pools in {:?}ms ({:.1} pools/sec)",
                self.metrics.v2_pools_scraped,
                self.metrics.v2_scraping_duration_ms,
                (self.metrics.v2_pools_scraped as f64)
                    / (v2_start.elapsed().as_secs_f64())
            );

            // Get reference block after V2 scraping (before slow V3/V4 scraping)
            self.scrape_reference_block = Some(self.get_current_block_number().await);
        }

        // Scrape V3/V4 pools (slow - ticks + bitmaps)
        if !v3_v4_pools.is_empty() {
            let v3_v4_start = Instant::now();
            log::info!(
                "Scraping V3/V4 pools (batch size: {})...",
                self.config.v3_v4_scrape_batch_size
            );

            for (idx, pool) in v3_v4_pools.iter().enumerate() {
                if let Err(e) = self.scrape_and_add_pool(&pool.id, &pool.protocol).await {
                    log::error!("Failed to scrape V3/V4 pool {}: {}", pool.id, e);
                    self.metrics.pools_failed += 1;
                } else {
                    self.metrics.v3_v4_pools_scraped += 1;
                }

                // Update phase progress
                self.phase = StartupPhase::ScrapingPools {
                    v2_completed: v2_pools.len(),
                    v2_total: v2_pools.len(),
                    v3_v4_completed: idx + 1,
                    v3_v4_total: v3_v4_pools.len(),
                };

                // Log progress every 50 pools
                if (idx + 1) % 50 == 0 {
                    log::info!("V3/V4 progress: {}/{}", idx + 1, v3_v4_pools.len());
                }
            }

            self.metrics.v3_v4_scraping_duration_ms =
                Some(v3_v4_start.elapsed().as_millis() as u64);
            log::info!(
                "V3/V4 scraping complete: {} pools in {:?}ms ({:.1} pools/sec)",
                self.metrics.v3_v4_pools_scraped,
                self.metrics.v3_v4_scraping_duration_ms,
                (self.metrics.v3_v4_pools_scraped as f64)
                    / (v3_v4_start.elapsed().as_secs_f64())
            );

            // Update reference block if not set (no V2 pools)
            if self.scrape_reference_block.is_none() {
                self.scrape_reference_block = Some(self.get_current_block_number().await);
            }
        }

        self.metrics.scraping_completed_at = Some(Instant::now());

        // Log socket buffer status
        if let Some(ref client) = self.socket_client {
            let stats = client.buffer_stats();
            log::info!(
                "Socket buffer: {} events buffered (blocks {:?})",
                stats.buffered_count,
                stats.buffered_block_range
            );
            self.metrics.events_buffered = stats.buffered_count;
        }

        log::info!(
            "Phase 2 complete: Total {} pools scraped ({} V2, {} V3/V4, {} failed)",
            self.metrics.v2_pools_scraped + self.metrics.v3_v4_pools_scraped,
            self.metrics.v2_pools_scraped,
            self.metrics.v3_v4_pools_scraped,
            self.metrics.pools_failed
        );

        Ok(())
    }

    /// Phase 3: Replay buffered events
    async fn phase_3_replay_events(&mut self) -> Result<()> {
        log::info!("Phase 3: Replaying buffered events");
        self.metrics.replay_started_at = Some(Instant::now());

        let client = self
            .socket_client
            .as_mut()
            .ok_or_else(|| StartupError::SocketClient("No socket client".to_string()))?;

        // Take buffered events (automatically sorted)
        let mut buffered_events = client.take_buffered_events();
        self.metrics.events_buffered = buffered_events.len();

        log::info!(
            "Retrieved {} buffered events from socket",
            buffered_events.len()
        );

        // Filter to only events after scrape reference block
        let reference_block = self.scrape_reference_block.unwrap_or(0);
        buffered_events.retain(|e| e.block_number > reference_block);

        log::info!(
            "Filtered to {} events after reference block {}",
            buffered_events.len(),
            reference_block
        );

        // Replay events
        self.phase = StartupPhase::ReplayingEvents {
            events_completed: 0,
            events_total: buffered_events.len(),
        };

        for (idx, event) in buffered_events.iter().enumerate() {
            if let Err(e) = self.apply_event(event).await {
                log::error!("Failed to apply buffered event: {}", e);
            } else {
                self.metrics.events_replayed += 1;
            }

            // Update progress every 1000 events
            if (idx + 1) % 1000 == 0 {
                self.phase = StartupPhase::ReplayingEvents {
                    events_completed: idx + 1,
                    events_total: buffered_events.len(),
                };
            }
        }

        self.metrics.replay_completed_at = Some(Instant::now());
        if let Some(start) = self.metrics.replay_started_at {
            self.metrics.replay_duration_ms = Some(start.elapsed().as_millis() as u64);
        }

        log::info!(
            "Phase 3 complete: Replayed {} events in {:?}ms",
            self.metrics.events_replayed,
            self.metrics.replay_duration_ms
        );

        Ok(())
    }

    /// Phase 4: Switch to live processing
    async fn phase_4_go_live(&mut self) -> Result<()> {
        log::info!("Phase 4: Switching to live processing mode");

        // Switch socket to live mode
        if let Some(ref mut client) = self.socket_client {
            client.set_mode(ClientMode::Live);
        }

        self.phase = StartupPhase::Live;
        self.metrics.live_mode_started_at = Some(Instant::now());

        log::info!("Phase 4 complete: Now in live processing mode");

        Ok(())
    }

    /// Scrape a single pool and add to arena
    async fn scrape_and_add_pool(&mut self, pool_id: &str, protocol: &str) -> Result<()> {
        // Parse identifier
        let identifier = self.parse_pool_identifier(pool_id, protocol)?;

        // Scrape from RethDB
        let raw_state = self
            .scraper
            .scrape_pool(&identifier, protocol)
            .await
            .map_err(|e| StartupError::Scraper(e.to_string()))?;

        // Determine tier
        let tick_count = raw_state.ticks.len();
        let bitmap_count = raw_state.tick_bitmaps.len();
        let tier = determine_tier(tick_count, bitmap_count);

        // Create pool
        let pool_result = create_pool(raw_state, tier)
            .map_err(|e| StartupError::PoolFactory(e.to_string()))?;

        // Add to registry
        let mut registry = self.arena_registry.write().await;
        self.add_pool_to_registry(&mut registry, pool_result)?;

        // Track loaded pool
        self.loaded_pools.insert(identifier);

        Ok(())
    }

    /// Add pool to registry based on type
    fn add_pool_to_registry(
        &self,
        registry: &mut PoolArenaRegistry,
        pool_result: PoolCreationResult,
    ) -> Result<()> {
        match pool_result {
            PoolCreationResult::V2(pool) => registry
                .add_uniswap_v2_pool(
                    pool.pool_id(),
                    pool.reserve0,
                    pool.reserve1,
                    pool.token0,
                    pool.token1,
                )
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V3Low(pool) => registry
                .add_uniswap_v3_low_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V3Active(pool) => registry
                .add_uniswap_v3_active_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V3Popular(pool) => registry
                .add_uniswap_v3_popular_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V3Major(pool) => registry
                .add_uniswap_v3_major_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V4Low(pool) => registry
                .add_uniswap_v4_low_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V4Active(pool) => registry
                .add_uniswap_v4_active_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V4Popular(pool) => registry
                .add_uniswap_v4_popular_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
            PoolCreationResult::V4Major(pool) => registry
                .add_uniswap_v4_major_pool(pool)
                .map_err(|e| StartupError::ArenaRegistry(e.to_string())),
        }
    }

    /// Apply a pool event to arena (placeholder)
    async fn apply_event(&self, event: &PoolEvent) -> Result<()> {
        // TODO: Implement event application
        // This will update pool state based on event type
        log::trace!(
            "Applying event: block={}, tx={}, log={}, type={}",
            event.block_number,
            event.transaction_index,
            event.log_index,
            event.event_type
        );
        Ok(())
    }

    /// Get current block number (placeholder)
    async fn get_current_block_number(&self) -> u64 {
        // TODO: Query from RethDB
        0
    }

    /// Parse pool identifier from string
    fn parse_pool_identifier(&self, id_str: &str, protocol: &str) -> Result<PoolIdentifier> {
        let hex_str = id_str.strip_prefix("0x").unwrap_or(id_str);

        match protocol {
            "uniswap_v2" | "uniswap_v3" => {
                if hex_str.len() != 40 {
                    return Err(StartupError::PoolFactory(format!(
                        "Invalid address length: {}",
                        hex_str.len()
                    )));
                }
                let mut address = [0u8; 20];
                hex::decode_to_slice(hex_str, &mut address)
                    .map_err(|_| StartupError::PoolFactory("Invalid hex".to_string()))?;
                Ok(PoolIdentifier::Address(address))
            }
            "uniswap_v4" => {
                if hex_str.len() != 64 {
                    return Err(StartupError::PoolFactory(format!(
                        "Invalid pool ID length: {}",
                        hex_str.len()
                    )));
                }
                let mut pool_id = [0u8; 32];
                hex::decode_to_slice(hex_str, &mut pool_id)
                    .map_err(|_| StartupError::PoolFactory("Invalid hex".to_string()))?;
                Ok(PoolIdentifier::PoolId(pool_id))
            }
            _ => Err(StartupError::PoolFactory(format!(
                "Unknown protocol: {}",
                protocol
            ))),
        }
    }

    /// Get socket client for live event processing
    pub fn socket_client_mut(&mut self) -> Option<&mut UnixSocketClient> {
        if self.phase == StartupPhase::Live {
            self.socket_client.as_mut()
        } else {
            None
        }
    }
}
