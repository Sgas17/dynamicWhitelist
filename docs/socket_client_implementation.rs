// Unix Socket Client for pool_state_arena
// Location: /home/sam-sullivan/defi_platform/pool_state_arena/src/subscriber/socket_client.rs
//
// This module implements Unix socket communication with the Reth ExEx,
// adapting patterns from eventCaptureService/capture/liquidity_capture.py

use super::socket_messages::{PoolEvent, SocketMessage};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::sleep;

/// Errors that can occur in the socket client
#[derive(Error, Debug)]
pub enum SocketClientError {
    #[error("Socket connection error: {0}")]
    Connection(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::Error),

    #[error("Invalid message format: {0}")]
    InvalidMessage(String),

    #[error("Buffer overflow: capacity {capacity}, attempted {attempted}")]
    BufferOverflow { capacity: usize, attempted: usize },
}

pub type Result<T> = std::result::Result<T, SocketClientError>;

/// Operating mode for the socket client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMode {
    /// Buffer all events in memory for later replay
    Buffering {
        /// Maximum number of events to buffer before dropping old ones
        capacity: usize,
    },
    /// Process events immediately as they arrive
    Live,
}

/// Unix socket client for receiving pool events from Reth ExEx
pub struct UnixSocketClient {
    /// Path to the Unix socket
    socket_path: PathBuf,

    /// Active socket connection
    stream: Option<UnixStream>,

    /// Current operating mode
    mode: ClientMode,

    /// Buffered events (only used in Buffering mode)
    event_buffer: Vec<PoolEvent>,

    /// Track the earliest buffered block number
    earliest_buffered_block: Option<u64>,

    /// Track the latest buffered block number
    latest_buffered_block: Option<u64>,

    /// Current block being processed (between BeginBlock and EndBlock)
    current_block_number: Option<u64>,

    /// Pending updates for current block
    pending_updates: Vec<PoolEvent>,

    /// Reconnection configuration
    max_reconnect_attempts: u32,
    reconnect_delay: Duration,

    /// Internal read buffer for incomplete messages
    read_buffer: Vec<u8>,
}

impl UnixSocketClient {
    /// Create a new Unix socket client
    ///
    /// # Arguments
    /// * `socket_path` - Path to the Unix socket
    /// * `mode` - Initial operating mode (Buffering or Live)
    ///
    /// # Example
    /// ```
    /// let client = UnixSocketClient::new(
    ///     "/tmp/reth_exex.sock",
    ///     ClientMode::Buffering { capacity: 100_000 }
    /// );
    /// ```
    pub fn new(socket_path: impl Into<PathBuf>, mode: ClientMode) -> Self {
        Self {
            socket_path: socket_path.into(),
            stream: None,
            mode,
            event_buffer: Vec::new(),
            earliest_buffered_block: None,
            latest_buffered_block: None,
            current_block_number: None,
            pending_updates: Vec::new(),
            max_reconnect_attempts: 10,
            reconnect_delay: Duration::from_millis(100),
            read_buffer: Vec::with_capacity(8192),
        }
    }

    /// Connect to the Unix socket with retry logic
    ///
    /// Uses exponential backoff for reconnection attempts.
    ///
    /// # Returns
    /// Ok(()) if connection successful, error otherwise
    pub async fn connect(&mut self) -> Result<()> {
        let mut attempts = 0;
        let mut delay = self.reconnect_delay;

        loop {
            match UnixStream::connect(&self.socket_path).await {
                Ok(stream) => {
                    log::info!(
                        "Connected to Unix socket: {}",
                        self.socket_path.display()
                    );
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.max_reconnect_attempts {
                        return Err(SocketClientError::Connection(format!(
                            "Failed to connect after {} attempts: {}",
                            attempts, e
                        )));
                    }

                    log::warn!(
                        "Connection attempt {} failed: {}. Retrying in {:?}...",
                        attempts,
                        e,
                        delay
                    );

                    sleep(delay).await;
                    // Exponential backoff (cap at 10 seconds)
                    delay = std::cmp::min(delay * 2, Duration::from_secs(10));
                }
            }
        }
    }

    /// Check if currently connected
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Set the operating mode
    ///
    /// When switching from Buffering to Live, the buffer is NOT automatically cleared.
    /// Call `take_buffered_events()` first if you want to process them.
    pub fn set_mode(&mut self, mode: ClientMode) {
        log::info!("Switching socket client mode: {:?} -> {:?}", self.mode, mode);
        self.mode = mode;
    }

    /// Get current operating mode
    pub fn mode(&self) -> ClientMode {
        self.mode
    }

    /// Take ownership of buffered events and clear the buffer
    ///
    /// Returns events sorted chronologically by (block_number, transaction_index, log_index).
    ///
    /// # Returns
    /// Sorted vector of buffered events
    pub fn take_buffered_events(&mut self) -> Vec<PoolEvent> {
        let mut events = std::mem::take(&mut self.event_buffer);
        events.sort(); // PoolEvent implements Ord for chronological sorting
        self.earliest_buffered_block = None;
        self.latest_buffered_block = None;
        events
    }

    /// Get the block number range of buffered events without taking ownership
    pub fn buffered_block_range(&self) -> Option<(u64, u64)> {
        match (self.earliest_buffered_block, self.latest_buffered_block) {
            (Some(earliest), Some(latest)) => Some((earliest, latest)),
            _ => None,
        }
    }

    /// Get count of buffered events
    pub fn buffered_count(&self) -> usize {
        self.event_buffer.len()
    }

    /// Read and process the next message from the socket
    ///
    /// This is the main event loop method. It:
    /// 1. Reads length-prefixed bincode messages from the socket
    /// 2. Handles BeginBlock → PoolUpdate → EndBlock batching
    /// 3. Either buffers or processes events based on current mode
    ///
    /// # Returns
    /// - Ok(Some(events)) - Complete block processed (only in Live mode)
    /// - Ok(None) - Message processed but block not yet complete
    /// - Err - Connection or parsing error
    pub async fn read_and_process(&mut self) -> Result<Option<Vec<PoolEvent>>> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| SocketClientError::Connection("Not connected".to_string()))?;

        // Read data from socket into our buffer
        let mut temp_buf = [0u8; 8192];
        let n = stream.read(&mut temp_buf).await?;

        if n == 0 {
            return Err(SocketClientError::Connection(
                "Socket closed by remote".to_string(),
            ));
        }

        self.read_buffer.extend_from_slice(&temp_buf[..n]);

        // Try to parse complete messages from buffer
        let mut result = None;

        loop {
            // Need at least 4 bytes for length prefix
            if self.read_buffer.len() < 4 {
                break;
            }

            // Read message length (u32 little-endian)
            let length = u32::from_le_bytes([
                self.read_buffer[0],
                self.read_buffer[1],
                self.read_buffer[2],
                self.read_buffer[3],
            ]) as usize;

            // Check if we have the complete message
            if self.read_buffer.len() < 4 + length {
                break; // Wait for more data
            }

            // Extract message payload
            let message_data = &self.read_buffer[4..4 + length];

            // Deserialize the message
            let message: SocketMessage = bincode::deserialize(message_data)?;

            // Remove processed message from buffer
            self.read_buffer.drain(..4 + length);

            // Handle the message
            if let Some(events) = self.handle_message(message)? {
                result = Some(events);
            }
        }

        Ok(result)
    }

    /// Handle a parsed socket message
    ///
    /// Implements the BeginBlock → PoolUpdate → EndBlock state machine.
    ///
    /// # Returns
    /// - Ok(Some(events)) - Complete block processed
    /// - Ok(None) - Message handled but block not complete
    /// - Err - Invalid message sequence
    fn handle_message(&mut self, message: SocketMessage) -> Result<Option<Vec<PoolEvent>>> {
        match message {
            SocketMessage::BeginBlock {
                block_number,
                is_revert,
            } => {
                // Start new block
                if self.current_block_number.is_some() {
                    log::warn!(
                        "Received BeginBlock while already processing block {}",
                        self.current_block_number.unwrap()
                    );
                }

                log::debug!(
                    "BeginBlock: {} (revert: {})",
                    block_number,
                    is_revert
                );

                self.current_block_number = Some(block_number);
                self.pending_updates.clear();

                Ok(None)
            }

            SocketMessage::PoolUpdate(mut event) => {
                // Add event to current block
                if self.current_block_number.is_none() {
                    return Err(SocketClientError::InvalidMessage(
                        "Received PoolUpdate without BeginBlock".to_string(),
                    ));
                }

                // Verify event block number matches current block
                if event.block_number != self.current_block_number.unwrap() {
                    return Err(SocketClientError::InvalidMessage(format!(
                        "PoolUpdate block {} doesn't match current block {}",
                        event.block_number,
                        self.current_block_number.unwrap()
                    )));
                }

                log::trace!(
                    "PoolUpdate: block={}, tx={}, log={}, pool={:?}",
                    event.block_number,
                    event.transaction_index,
                    event.log_index,
                    hex::encode(&event.pool_address)
                );

                self.pending_updates.push(event);

                Ok(None)
            }

            SocketMessage::EndBlock {
                block_number,
                num_updates,
            } => {
                // Complete current block
                if self.current_block_number.is_none() {
                    return Err(SocketClientError::InvalidMessage(
                        "Received EndBlock without BeginBlock".to_string(),
                    ));
                }

                if block_number != self.current_block_number.unwrap() {
                    return Err(SocketClientError::InvalidMessage(format!(
                        "EndBlock block {} doesn't match current block {}",
                        block_number,
                        self.current_block_number.unwrap()
                    )));
                }

                if self.pending_updates.len() != num_updates {
                    log::warn!(
                        "EndBlock update count mismatch: expected {}, got {}",
                        num_updates,
                        self.pending_updates.len()
                    );
                }

                log::debug!(
                    "EndBlock: {} with {} updates",
                    block_number,
                    self.pending_updates.len()
                );

                // Take ownership of pending updates
                let events = std::mem::take(&mut self.pending_updates);
                self.current_block_number = None;

                // Handle based on mode
                match self.mode {
                    ClientMode::Buffering { capacity } => {
                        // Buffer events for later replay
                        if self.event_buffer.len() + events.len() > capacity {
                            return Err(SocketClientError::BufferOverflow {
                                capacity,
                                attempted: self.event_buffer.len() + events.len(),
                            });
                        }

                        // Track block range
                        if self.earliest_buffered_block.is_none() {
                            self.earliest_buffered_block = Some(block_number);
                        }
                        self.latest_buffered_block = Some(block_number);

                        self.event_buffer.extend(events);

                        log::info!(
                            "Buffered {} events from block {} (total buffered: {})",
                            num_updates,
                            block_number,
                            self.event_buffer.len()
                        );

                        Ok(None)
                    }
                    ClientMode::Live => {
                        // Return events for immediate processing
                        log::debug!(
                            "Live processing {} events from block {}",
                            events.len(),
                            block_number
                        );
                        Ok(Some(events))
                    }
                }
            }
        }
    }

    /// Disconnect from the socket
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
            log::info!("Disconnected from Unix socket");
        }
        Ok(())
    }

    /// Get buffer statistics
    pub fn buffer_stats(&self) -> BufferStats {
        BufferStats {
            mode: self.mode,
            buffered_count: self.event_buffer.len(),
            buffered_block_range: self.buffered_block_range(),
            current_block: self.current_block_number,
            pending_updates_count: self.pending_updates.len(),
        }
    }
}

/// Buffer statistics for monitoring
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub mode: ClientMode,
    pub buffered_count: usize,
    pub buffered_block_range: Option<(u64, u64)>,
    pub current_block: Option<u64>,
    pub pending_updates_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = UnixSocketClient::new(
            "/tmp/test.sock",
            ClientMode::Buffering { capacity: 1000 },
        );

        assert_eq!(client.mode(), ClientMode::Buffering { capacity: 1000 });
        assert!(!client.is_connected());
        assert_eq!(client.buffered_count(), 0);
    }

    #[test]
    fn test_mode_switching() {
        let mut client = UnixSocketClient::new(
            "/tmp/test.sock",
            ClientMode::Buffering { capacity: 1000 },
        );

        client.set_mode(ClientMode::Live);
        assert_eq!(client.mode(), ClientMode::Live);

        client.set_mode(ClientMode::Buffering { capacity: 5000 });
        assert_eq!(client.mode(), ClientMode::Buffering { capacity: 5000 });
    }

    #[test]
    fn test_buffer_stats() {
        let client = UnixSocketClient::new(
            "/tmp/test.sock",
            ClientMode::Buffering { capacity: 1000 },
        );

        let stats = client.buffer_stats();
        assert_eq!(stats.buffered_count, 0);
        assert_eq!(stats.buffered_block_range, None);
        assert_eq!(stats.current_block, None);
        assert_eq!(stats.pending_updates_count, 0);
    }

    // Note: Integration tests with actual Unix socket would require
    // a running ExEx instance, so we test the client logic separately
}
