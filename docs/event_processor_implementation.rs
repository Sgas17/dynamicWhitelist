// Event Processor for pool_state_arena
// Location: /home/sam-sullivan/defi_platform/pool_state_arena/src/subscriber/event_processor.rs
//
// This module handles applying pool events to update pool state in the arena.
// Supports V2, V3, and V4 pool events including Swap, Mint, Burn, and ModifyLiquidity.

use super::socket_messages::PoolEvent;
use crate::protocol::{PoolData, PoolIdentifier};
use crate::registry::PoolArenaRegistry;
use alloy_primitives::U256;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Errors that can occur during event processing
#[derive(Error, Debug)]
pub enum EventProcessorError {
    #[error("Pool not found: {0:?}")]
    PoolNotFound(PoolIdentifier),

    #[error("Invalid event data: {0}")]
    InvalidEventData(String),

    #[error("Unsupported protocol: {0}")]
    UnsupportedProtocol(String),

    #[error("Unsupported event type: {0}")]
    UnsupportedEventType(String),

    #[error("Arena registry error: {0}")]
    ArenaRegistry(String),

    #[error("Arithmetic overflow: {0}")]
    ArithmeticOverflow(String),
}

pub type Result<T> = std::result::Result<T, EventProcessorError>;

/// Event processor for applying pool state updates
pub struct EventProcessor {
    /// Arena registry (shared, read-write access)
    arena_registry: Arc<RwLock<PoolArenaRegistry>>,

    /// Statistics
    stats: EventProcessorStats,
}

/// Statistics for event processing
#[derive(Debug, Clone, Default)]
pub struct EventProcessorStats {
    pub v2_swaps_processed: u64,
    pub v2_mints_processed: u64,
    pub v2_burns_processed: u64,

    pub v3_swaps_processed: u64,
    pub v3_mints_processed: u64,
    pub v3_burns_processed: u64,

    pub v4_swaps_processed: u64,
    pub v4_mints_processed: u64,
    pub v4_burns_processed: u64,
    pub v4_modify_liquidity_processed: u64,

    pub reverts_processed: u64,
    pub errors: u64,
}

impl EventProcessor {
    /// Create a new event processor
    pub fn new(arena_registry: Arc<RwLock<PoolArenaRegistry>>) -> Self {
        Self {
            arena_registry,
            stats: EventProcessorStats::default(),
        }
    }

    /// Get processing statistics
    pub fn stats(&self) -> &EventProcessorStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = EventProcessorStats::default();
    }

    /// Process a single pool event
    ///
    /// Routes the event to the appropriate handler based on protocol and event type.
    ///
    /// # Arguments
    /// * `event` - Pool event to process
    ///
    /// # Returns
    /// Ok(()) if event processed successfully
    pub async fn process_event(&mut self, event: &PoolEvent) -> Result<()> {
        // Handle revert events first
        if event.is_revert {
            log::warn!(
                "Processing revert event for block {} - reverting pool state changes",
                event.block_number
            );
            self.stats.reverts_processed += 1;
            // TODO: Implement revert logic (store block snapshots for rollback)
            return Ok(());
        }

        // Route based on protocol
        match event.protocol.as_str() {
            "uniswap_v2" => self.process_v2_event(event).await,
            "uniswap_v3" => self.process_v3_event(event).await,
            "uniswap_v4" => self.process_v4_event(event).await,
            protocol => {
                self.stats.errors += 1;
                Err(EventProcessorError::UnsupportedProtocol(
                    protocol.to_string(),
                ))
            }
        }
    }

    /// Process batch of events
    ///
    /// Processes events in order and collects errors without stopping.
    ///
    /// # Returns
    /// Vec of errors that occurred, empty if all succeeded
    pub async fn process_batch(&mut self, events: &[PoolEvent]) -> Vec<EventProcessorError> {
        let mut errors = Vec::new();

        for event in events {
            if let Err(e) = self.process_event(event).await {
                log::error!(
                    "Error processing event (block={}, tx={}, log={}): {}",
                    event.block_number,
                    event.transaction_index,
                    event.log_index,
                    e
                );
                errors.push(e);
            }
        }

        errors
    }

    /// Process Uniswap V2 event
    async fn process_v2_event(&mut self, event: &PoolEvent) -> Result<()> {
        match event.event_type.as_str() {
            "Swap" => self.process_v2_swap(event).await,
            "Mint" => self.process_v2_mint(event).await,
            "Burn" => self.process_v2_burn(event).await,
            _ => {
                self.stats.errors += 1;
                Err(EventProcessorError::UnsupportedEventType(format!(
                    "V2 {}",
                    event.event_type
                )))
            }
        }
    }

    /// Process V2 Swap event
    ///
    /// Updates reserve0 and reserve1 based on swap amounts.
    async fn process_v2_swap(&mut self, event: &PoolEvent) -> Result<()> {
        log::trace!(
            "Processing V2 Swap: pool={:?}, amount0={}, amount1={}",
            hex::encode(&event.pool_address),
            event.amount0,
            event.amount1
        );

        // For V2, we need to update the reserves based on the swap
        // amount0 < 0 means token0 out, amount0 > 0 means token0 in
        // amount1 < 0 means token1 out, amount1 > 0 means token1 in

        // Get current pool state
        let registry = self.arena_registry.read().await;
        let pool_location = registry
            .get_v2_pool_location(event.pool_address)
            .ok_or(EventProcessorError::PoolNotFound(
                PoolIdentifier::Address(event.pool_address),
            ))?;

        drop(registry); // Release read lock

        // Calculate new reserves
        // This is a simplified version - actual implementation would need
        // to read current reserves, apply deltas, and validate

        // TODO: Implement actual reserve update logic
        // For now, just track the stat
        self.stats.v2_swaps_processed += 1;

        Ok(())
    }

    /// Process V2 Mint event
    async fn process_v2_mint(&mut self, event: &PoolEvent) -> Result<()> {
        log::trace!(
            "Processing V2 Mint: pool={:?}, amount0={}, amount1={}",
            hex::encode(&event.pool_address),
            event.amount0,
            event.amount1
        );

        // V2 Mint adds liquidity, increasing reserves
        // reserve0 += amount0
        // reserve1 += amount1

        // TODO: Implement reserve update logic

        self.stats.v2_mints_processed += 1;
        Ok(())
    }

    /// Process V2 Burn event
    async fn process_v2_burn(&mut self, event: &PoolEvent) -> Result<()> {
        log::trace!(
            "Processing V2 Burn: pool={:?}, amount0={}, amount1={}",
            hex::encode(&event.pool_address),
            event.amount0,
            event.amount1
        );

        // V2 Burn removes liquidity, decreasing reserves
        // reserve0 -= amount0
        // reserve1 -= amount1

        // TODO: Implement reserve update logic

        self.stats.v2_burns_processed += 1;
        Ok(())
    }

    /// Process Uniswap V3 event
    async fn process_v3_event(&mut self, event: &PoolEvent) -> Result<()> {
        match event.event_type.as_str() {
            "Swap" => self.process_v3_swap(event).await,
            "Mint" => self.process_v3_mint(event).await,
            "Burn" => self.process_v3_burn(event).await,
            _ => {
                self.stats.errors += 1;
                Err(EventProcessorError::UnsupportedEventType(format!(
                    "V3 {}",
                    event.event_type
                )))
            }
        }
    }

    /// Process V3 Swap event
    ///
    /// Updates sqrt_price_x96, current_tick, and liquidity.
    async fn process_v3_swap(&mut self, event: &PoolEvent) -> Result<()> {
        let sqrt_price = event.sqrt_price_x96.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing sqrt_price_x96".to_string())
        })?;

        let tick = event.tick.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick".to_string())
        })?;

        let liquidity = event.liquidity.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing liquidity".to_string())
        })?;

        log::trace!(
            "Processing V3 Swap: pool={:?}, sqrt_price={}, tick={}, liquidity={}",
            hex::encode(&event.pool_address),
            sqrt_price,
            tick,
            liquidity
        );

        // Find pool in registry
        let registry = self.arena_registry.read().await;
        let pool_location = registry
            .get_v3_pool_location(event.pool_address)
            .ok_or(EventProcessorError::PoolNotFound(
                PoolIdentifier::Address(event.pool_address),
            ))?;

        drop(registry); // Release read lock

        // TODO: Update pool state
        // This requires:
        // 1. Get mutable reference to pool based on pool_location (tier + index)
        // 2. Update pool.sqrt_price_x96 = sqrt_price
        // 3. Update pool.current_tick = tick
        // 4. Update pool.liquidity = liquidity
        //
        // This is tricky because we need mutable access to the specific tier arena
        // which requires careful lock management and type-safe arena access

        self.stats.v3_swaps_processed += 1;
        Ok(())
    }

    /// Process V3 Mint event
    ///
    /// Updates tick liquidity in the specified tick range.
    async fn process_v3_mint(&mut self, event: &PoolEvent) -> Result<()> {
        let tick_lower = event.tick_lower.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_lower".to_string())
        })?;

        let tick_upper = event.tick_upper.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_upper".to_string())
        })?;

        let liquidity_delta = event.liquidity_delta.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing liquidity_delta".to_string())
        })?;

        log::trace!(
            "Processing V3 Mint: pool={:?}, tick_lower={}, tick_upper={}, liquidity_delta={}",
            hex::encode(&event.pool_address),
            tick_lower,
            tick_upper,
            liquidity_delta
        );

        // TODO: Update tick liquidity
        // For V3 Mint, we need to:
        // 1. Add liquidity_delta to tick_lower
        // 2. Subtract liquidity_delta from tick_upper
        // 3. Update overall pool liquidity if current tick is in range
        //
        // This requires mutable access to the pool's tick array

        self.stats.v3_mints_processed += 1;
        Ok(())
    }

    /// Process V3 Burn event
    ///
    /// Updates tick liquidity in the specified tick range (opposite of Mint).
    async fn process_v3_burn(&mut self, event: &PoolEvent) -> Result<()> {
        let tick_lower = event.tick_lower.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_lower".to_string())
        })?;

        let tick_upper = event.tick_upper.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_upper".to_string())
        })?;

        let liquidity_delta = event.liquidity_delta.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing liquidity_delta".to_string())
        })?;

        log::trace!(
            "Processing V3 Burn: pool={:?}, tick_lower={}, tick_upper={}, liquidity_delta={}",
            hex::encode(&event.pool_address),
            tick_lower,
            tick_upper,
            liquidity_delta
        );

        // TODO: Update tick liquidity (opposite of Mint)
        // For V3 Burn, we need to:
        // 1. Subtract liquidity_delta from tick_lower
        // 2. Add liquidity_delta to tick_upper
        // 3. Update overall pool liquidity if current tick is in range

        self.stats.v3_burns_processed += 1;
        Ok(())
    }

    /// Process Uniswap V4 event
    async fn process_v4_event(&mut self, event: &PoolEvent) -> Result<()> {
        match event.event_type.as_str() {
            "Swap" => self.process_v4_swap(event).await,
            "ModifyLiquidity" => self.process_v4_modify_liquidity(event).await,
            _ => {
                self.stats.errors += 1;
                Err(EventProcessorError::UnsupportedEventType(format!(
                    "V4 {}",
                    event.event_type
                )))
            }
        }
    }

    /// Process V4 Swap event
    ///
    /// Similar to V3, updates sqrt_price_x96, tick, and liquidity.
    async fn process_v4_swap(&mut self, event: &PoolEvent) -> Result<()> {
        let pool_id = event.pool_id.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing pool_id for V4".to_string())
        })?;

        let sqrt_price = event.sqrt_price_x96.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing sqrt_price_x96".to_string())
        })?;

        let tick = event.tick.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick".to_string())
        })?;

        let liquidity = event.liquidity.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing liquidity".to_string())
        })?;

        log::trace!(
            "Processing V4 Swap: pool_id={:?}, sqrt_price={}, tick={}, liquidity={}",
            hex::encode(&pool_id),
            sqrt_price,
            tick,
            liquidity
        );

        // Find pool in registry
        let registry = self.arena_registry.read().await;
        let pool_location = registry
            .get_v4_pool_location(pool_id)
            .ok_or(EventProcessorError::PoolNotFound(
                PoolIdentifier::PoolId(pool_id),
            ))?;

        drop(registry); // Release read lock

        // TODO: Update V4 pool state (same as V3)

        self.stats.v4_swaps_processed += 1;
        Ok(())
    }

    /// Process V4 ModifyLiquidity event
    ///
    /// V4 combines Mint/Burn into a single ModifyLiquidity event.
    /// Positive liquidity_delta = add liquidity (Mint)
    /// Negative liquidity_delta = remove liquidity (Burn)
    async fn process_v4_modify_liquidity(&mut self, event: &PoolEvent) -> Result<()> {
        let pool_id = event.pool_id.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing pool_id for V4".to_string())
        })?;

        let tick_lower = event.tick_lower.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_lower".to_string())
        })?;

        let tick_upper = event.tick_upper.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing tick_upper".to_string())
        })?;

        let liquidity_delta = event.liquidity_delta.ok_or_else(|| {
            EventProcessorError::InvalidEventData("Missing liquidity_delta".to_string())
        })?;

        log::trace!(
            "Processing V4 ModifyLiquidity: pool_id={:?}, tick_lower={}, tick_upper={}, liquidity_delta={}",
            hex::encode(&pool_id),
            tick_lower,
            tick_upper,
            liquidity_delta
        );

        // TODO: Update tick liquidity
        // For V4 ModifyLiquidity:
        // If liquidity_delta > 0 (adding liquidity):
        //   - Add liquidity_delta to tick_lower
        //   - Subtract liquidity_delta from tick_upper
        // If liquidity_delta < 0 (removing liquidity):
        //   - Subtract |liquidity_delta| from tick_lower
        //   - Add |liquidity_delta| to tick_upper
        //
        // This matches V3 Mint/Burn logic

        self.stats.v4_modify_liquidity_processed += 1;
        Ok(())
    }
}

/// Helper functions for V3/V4 tick calculations
mod tick_math {
    use super::*;

    /// Apply liquidity delta to a tick
    ///
    /// # Arguments
    /// * `current_liquidity` - Current liquidity at the tick
    /// * `delta` - Liquidity change (positive = add, negative = remove)
    ///
    /// # Returns
    /// New liquidity value, or error if overflow/underflow
    pub fn apply_liquidity_delta(current_liquidity: i128, delta: i128) -> Result<i128> {
        current_liquidity
            .checked_add(delta)
            .ok_or_else(|| EventProcessorError::ArithmeticOverflow(
                format!("Tick liquidity overflow: {} + {}", current_liquidity, delta)
            ))
    }

    /// Check if current tick is within a range
    pub fn tick_in_range(current_tick: i32, tick_lower: i32, tick_upper: i32) -> bool {
        current_tick >= tick_lower && current_tick < tick_upper
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_math_apply_liquidity_delta() {
        // Adding liquidity
        assert_eq!(tick_math::apply_liquidity_delta(1000, 500).unwrap(), 1500);

        // Removing liquidity
        assert_eq!(tick_math::apply_liquidity_delta(1000, -200).unwrap(), 800);

        // Overflow
        assert!(tick_math::apply_liquidity_delta(i128::MAX, 1).is_err());

        // Underflow
        assert!(tick_math::apply_liquidity_delta(i128::MIN, -1).is_err());
    }

    #[test]
    fn test_tick_in_range() {
        assert!(tick_math::tick_in_range(100, 50, 150));
        assert!(tick_math::tick_in_range(50, 50, 150)); // Lower bound inclusive
        assert!(!tick_math::tick_in_range(150, 50, 150)); // Upper bound exclusive
        assert!(!tick_math::tick_in_range(200, 50, 150));
        assert!(!tick_math::tick_in_range(0, 50, 150));
    }

    #[test]
    fn test_event_processor_stats() {
        let registry = Arc::new(RwLock::new(PoolArenaRegistry::new()));
        let mut processor = EventProcessor::new(registry);

        assert_eq!(processor.stats().v3_swaps_processed, 0);

        processor.stats.v3_swaps_processed = 10;
        assert_eq!(processor.stats().v3_swaps_processed, 10);

        processor.reset_stats();
        assert_eq!(processor.stats().v3_swaps_processed, 0);
    }
}
