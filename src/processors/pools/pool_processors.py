"""
Modular pool processors for Uniswap V3 and V4.

KISS: Extract core logic from existing scripts into reusable components.
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import eth_abi.abi as eth_abi
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes
import ujson

from ..base import BaseProcessor, ProcessorResult


class UniswapV3PoolProcessor(BaseProcessor):
    """
    Process Uniswap V3 PoolCreated events into structured pool data.
    
    Uses database storage instead of JSON files.
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize Uniswap V3 pool processor."""
        super().__init__(chain, "uniswap_v3")
        
        # Use centralized protocol config for factory addresses
        self.factory_addresses = self.config.protocols.get_factory_addresses("uniswap_v3", chain)
        self._pool_manager_addresses = [HexBytes(addr) for addr in self.factory_addresses]
        self.lp_type = "UniswapV3"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        if not self.factory_addresses:
            self.logger.error(f"No factory addresses for chain: {self.chain}")
            return False
        return True
    
    async def process(
        self, 
        start_block: int = 0,
        events_path: Optional[str] = None
    ) -> ProcessorResult:
        """
        Process Uniswap V3 pool events and store to database.
        
        Args:
            start_block: Block to start processing from
            events_path: Path to parquet events (optional)
            
        Returns:
            ProcessorResult: Processing results
        """
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Setup paths
            data_dir = Path(self.config.base.DATA_DIR)
            
            if not events_path:
                events_path = data_dir / self.chain / f"{self.protocol}_poolcreated_events"
            else:
                events_path = Path(events_path)
            
            # Get last processed block from database
            last_processed_block = await self._get_last_processed_block()
            start_block = max(start_block, last_processed_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Store to database
            stored_count = await self._store_pools_to_database(pools)
            
            # Get final stats
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            return ProcessorResult(
                success=True,
                data=pools,
                processed_count=stored_count,
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "stored_pools": stored_count
                }
            )
            
        except Exception as e:
            error_msg = f"V3 pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _process_events(self, events_path: Path, start_block: int) -> List[Dict[str, Any]]:
        """Process events from parquet files."""
        # Read events
        events_df = (
            pl.read_parquet(str(events_path / "*.parquet"))
            .filter(pl.col("block_number") >= start_block)
            .sort(pl.col("block_number"))
        )
        
        if events_df.is_empty():
            self.logger.info("No new events to process")
            return []
        
        # Process each event
        pools = []
        for event in events_df.rows(named=True):
            pool_data = self._process_single_event(event)
            if pool_data:
                pools.append(pool_data)
        
        self.logger.info(f"Processed {len(pools)} pools from {len(events_df)} events")
        return pools
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single PoolCreated event."""
        try:
            # Check factory address
            factory_address = event["address"]
            if factory_address not in self._pool_manager_addresses:
                return None
            
            # Decode event data
            currency0 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic1"])[0]
            )
            currency1 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic2"])[0]
            )
            fee = eth_abi.decode(types=["uint24"], data=event["topic3"])[0]
            
            tick_spacing, pool_address = eth_abi.decode(
                types=["int24", "address"], data=event["data"]
            )
            pool_address = to_checksum_address(pool_address)
            
            return {
                "address": self._to_serializable(pool_address),
                "fee": fee,
                "tick_spacing": tick_spacing,
                "asset0": self._to_serializable(currency0),
                "asset1": self._to_serializable(currency1),
                "type": self.lp_type,
                "creation_block": event["block_number"],
                "factory": self._to_serializable(factory_address),
                "chain": self.chain
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process event: {e}")
            return None


class UniswapV4PoolProcessor(BaseProcessor):
    """
    Process Uniswap V4 Initialize events into structured pool data.
    
    Uses database storage instead of JSON files.
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize Uniswap V4 pool processor."""
        super().__init__(chain, "uniswap_v4")
        
        # Use centralized protocol config for pool manager addresses
        config_data = self.config.protocols.get_protocol_config("uniswap_v4", chain)
        self.pool_manager_addresses = config_data.get("pool_manager", [])
        if isinstance(self.pool_manager_addresses, str):
            self.pool_manager_addresses = [self.pool_manager_addresses]
        
        self._pool_manager_addresses = [HexBytes(addr) for addr in self.pool_manager_addresses]
        self.lp_type = "UniswapV4"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        if not self.pool_manager_addresses:
            self.logger.warning(f"No pool manager addresses configured for chain: {self.chain}")
            return False
        
        self.logger.info(f"Using {len(self.pool_manager_addresses)} pool manager addresses for V4")
        return True
    
    async def process(
        self, 
        start_block: int = 0,
        events_path: Optional[str] = None
    ) -> ProcessorResult:
        """Process Uniswap V4 Initialize events and store to database."""
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Setup paths
            data_dir = Path(self.config.base.DATA_DIR)
            
            if not events_path:
                events_path = data_dir / self.chain / f"{self.protocol}_initialize_events"
            else:
                events_path = Path(events_path)
            
            # Get last processed block from database
            last_processed_block = await self._get_last_processed_block()
            start_block = max(start_block, last_processed_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Store to database
            stored_count = await self._store_pools_to_database(pools)
            
            # Get final stats
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            return ProcessorResult(
                success=True,
                data=pools,
                processed_count=stored_count,
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "stored_pools": stored_count
                }
            )
            
        except Exception as e:
            error_msg = f"V4 pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _process_events(self, events_path: Path, start_block: int) -> List[Dict[str, Any]]:
        """Process events from parquet files."""
        events_df = (
            pl.read_parquet(str(events_path / "*.parquet"))
            .filter(pl.col("block_number") >= start_block)
            .sort(pl.col("block_number"))
        )
        
        if events_df.is_empty():
            self.logger.info("No new events to process")
            return []
        
        pools = []
        for event in events_df.rows(named=True):
            pool_data = self._process_single_event(event)
            if pool_data:
                pools.append(pool_data)
        
        self.logger.info(f"Processed {len(pools)} pools from {len(events_df)} events")
        return pools
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single V4 Initialize event."""
        try:
            # Check pool manager address
            pool_manager_address = event["address"]
            if pool_manager_address not in self._pool_manager_addresses:
                return None
            
            # Decode V4 Initialize event
            # Topics: pool_id, currency0, currency1 (all indexed)
            pool_id = event["topic1"]  # bytes32 pool ID
            currency0 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic2"])[0]
            )
            currency1 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic3"])[0]
            )

            # Data: fee, tickSpacing, hooks (non-indexed)
            # Event signature: Initialize(PoolId indexed id, Currency indexed currency0, Currency indexed currency1, uint24 fee, int24 tickSpacing, IHooks hooks)
            fee, tick_spacing, hooks_address = eth_abi.decode(
                types=["uint24", "int24", "address"], data=event["data"]
            )
            hooks_address = to_checksum_address(hooks_address)
            
            # V4 doesn't have individual pool addresses - pools are identified by ID
            pool_address = pool_id.hex()

            # Store hooks_address in additional_data - critical for V4 pool behavior
            # Hooks determine fee structure, LP actions, and other pool-specific logic
            return {
                "address": self._to_serializable(pool_address),
                "fee": fee,
                "tick_spacing": tick_spacing,
                "asset0": self._to_serializable(currency0),
                "asset1": self._to_serializable(currency1),
                "type": self.lp_type,
                "creation_block": event["block_number"],
                "factory": self._to_serializable(pool_manager_address),
                "chain": self.chain,
                "additional_data": {
                    "hooks_address": self._to_serializable(hooks_address)
                }
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process V4 event: {e}")
            return None


class UniswapV2PoolProcessor(BaseProcessor):
    """
    Process Uniswap V2 PairCreated events into structured pool data.
    
    Uses database storage instead of JSON files.
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize Uniswap V2 pool processor."""
        super().__init__(chain, "uniswap_v2")
        
        # Get factory addresses from centralized config
        self.factory_addresses = self.config.protocols.get_factory_addresses("uniswap_v2", chain)
        self._factory_addresses = [HexBytes(addr) for addr in self.factory_addresses]
        self.lp_type = "UniswapV2"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        if not self.factory_addresses:
            self.logger.error(f"No factory addresses for chain: {self.chain}")
            return False
        
        self.logger.info(f"Using {len(self.factory_addresses)} factory addresses for V2")
        return True
    
    async def process(
        self, 
        start_block: int = 0,
        events_path: Optional[str] = None
    ) -> ProcessorResult:
        """Process Uniswap V2 pair creation events and store to database."""
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Setup paths
            data_dir = Path(self.config.base.DATA_DIR)
            
            if not events_path:
                events_path = data_dir / self.chain / f"{self.protocol}_paircreated_events"
            else:
                events_path = Path(events_path)
            
            # Get last processed block from database
            last_processed_block = await self._get_last_processed_block()
            start_block = max(start_block, last_processed_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Store to database
            stored_count = await self._store_pools_to_database(pools)
            
            # Get final stats
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            return ProcessorResult(
                success=True,
                data=pools,
                processed_count=stored_count,
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "stored_pools": stored_count
                }
            )
            
        except Exception as e:
            error_msg = f"V2 pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _process_events(self, events_path: Path, start_block: int) -> List[Dict[str, Any]]:
        """Process events from parquet files."""
        events_df = (
            pl.read_parquet(str(events_path / "*.parquet"))
            .filter(pl.col("block_number") >= start_block)
            .sort(pl.col("block_number"))
        )
        
        if events_df.is_empty():
            self.logger.info("No new events to process")
            return []
        
        pools = []
        for event in events_df.rows(named=True):
            pool_data = self._process_single_event(event)
            if pool_data:
                pools.append(pool_data)
        
        self.logger.info(f"Processed {len(pools)} pools from {len(events_df)} events")
        return pools
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single PairCreated event."""
        try:
            # Check factory address
            factory_address = event["address"]
            if factory_address not in self._factory_addresses:
                return None
            
            # Decode V2 PairCreated event data
            token0 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic1"])[0]
            )
            token1 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic2"])[0]
            )
            
            pair_address, pair_index = eth_abi.decode(
                types=["address", "uint256"], data=event["data"]
            )
            pair_address = to_checksum_address(pair_address)
            
            # pair_index not needed - filter by factory address instead
            # V2 doesn't have tick spacing (that's a V3/V4 concept) - leave as NULL
            return {
                "address": self._to_serializable(pair_address),
                "fee": 3000,  # V2 has fixed 0.3% fee
                "asset0": self._to_serializable(token0),
                "asset1": self._to_serializable(token1),
                "type": self.lp_type,
                "creation_block": event["block_number"],
                "factory": self._to_serializable(factory_address),
                "chain": self.chain,
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process V2 event: {e}")
            return None


class AerodromeV2PoolProcessor(BaseProcessor):
    """
    Process Aerodrome V2 PairCreated events for Base chain.
    
    Uses database storage instead of JSON files.
    """
    
    def __init__(self, chain: str = "base"):
        """Initialize Aerodrome V2 pool processor."""
        super().__init__(chain, "aerodrome_v2")
        
        # Get factory address from centralized config
        config_data = self.config.protocols.get_protocol_config("aerodrome_v2", chain)
        self.factory_address = config_data.get("factory")
        self._factory_address = HexBytes(self.factory_address) if self.factory_address else None
        self.lp_type = "AerodromeV2"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        if not self.factory_address:
            self.logger.error(f"No factory address for Aerodrome V2 on chain: {self.chain}")
            return False
        
        self.logger.info(f"Using factory address: {self.factory_address}")
        return True
    
    async def process(
        self, 
        start_block: int = 0,
        events_path: Optional[str] = None
    ) -> ProcessorResult:
        """Process Aerodrome V2 pair creation events and store to database."""
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Setup paths
            data_dir = Path(self.config.base.DATA_DIR)
            
            if not events_path:
                events_path = data_dir / self.chain / f"{self.protocol}_paircreated_events"
            else:
                events_path = Path(events_path)
            
            # Get last processed block from database
            last_processed_block = await self._get_last_processed_block()
            start_block = max(start_block, last_processed_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Store to database
            stored_count = await self._store_pools_to_database(pools)
            
            # Get final stats
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            return ProcessorResult(
                success=True,
                data=pools,
                processed_count=stored_count,
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "stored_pools": stored_count
                }
            )
            
        except Exception as e:
            error_msg = f"Aerodrome V2 pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _process_events(self, events_path: Path, start_block: int) -> List[Dict[str, Any]]:
        """Process events from parquet files."""
        events_df = (
            pl.read_parquet(str(events_path / "*.parquet"))
            .filter(pl.col("block_number") >= start_block)
            .sort(pl.col("block_number"))
        )
        
        if events_df.is_empty():
            self.logger.info("No new events to process")
            return []
        
        pools = []
        for event in events_df.rows(named=True):
            pool_data = self._process_single_event(event)
            if pool_data:
                pools.append(pool_data)
        
        self.logger.info(f"Processed {len(pools)} pools from {len(events_df)} events")
        return pools
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single Aerodrome V2 PairCreated event."""
        try:
            # Check factory address
            if event["address"] != self._factory_address:
                return None
            
            # Decode Aerodrome V2 PairCreated event
            token0 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic1"])[0]
            )
            token1 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic2"])[0]
            )
            
            # Aerodrome has a "stable" parameter in topic3
            stable = eth_abi.decode(types=["bool"], data=event["topic3"])[0]
            
            pair_address, pair_index = eth_abi.decode(
                types=["address", "uint256"], data=event["data"]
            )
            pair_address = to_checksum_address(pair_address)
            
            return {
                "address": self._to_serializable(pair_address),
                "fee": 500 if stable else 3000,  # Stable pairs have lower fees
                "asset0": self._to_serializable(token0),
                "asset1": self._to_serializable(token1),
                "type": self.lp_type,
                "creation_block": event["block_number"],
                "factory": self._to_serializable(event["address"]),
                "chain": self.chain,
                "additional_data": {
                    "pair_index": pair_index,
                    "stable": stable
                }
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process Aerodrome V2 event: {e}")
            return None


class AerodromeV3PoolProcessor(BaseProcessor):
    """
    Process Aerodrome V3 PoolCreated events for Base chain.
    
    Uses database storage instead of JSON files.
    """
    
    def __init__(self, chain: str = "base"):
        """Initialize Aerodrome V3 pool processor."""
        super().__init__(chain, "aerodrome_v3")
        
        # Get factory address from centralized config
        config_data = self.config.protocols.get_protocol_config("aerodrome_v3", chain)
        self.factory_address = config_data.get("factory")
        self._factory_address = HexBytes(self.factory_address) if self.factory_address else None
        self.lp_type = "AerodromeV3"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        if not self.factory_address:
            self.logger.error(f"No factory address for Aerodrome V3 on chain: {self.chain}")
            return False
        
        self.logger.info(f"Using factory address: {self.factory_address}")
        return True
    
    async def process(
        self, 
        start_block: int = 0,
        events_path: Optional[str] = None
    ) -> ProcessorResult:
        """Process Aerodrome V3 pool creation events and store to database."""
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Setup paths
            data_dir = Path(self.config.base.DATA_DIR)
            
            if not events_path:
                events_path = data_dir / self.chain / f"{self.protocol}_poolcreated_events"
            else:
                events_path = Path(events_path)
            
            # Get last processed block from database
            last_processed_block = await self._get_last_processed_block()
            start_block = max(start_block, last_processed_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Store to database
            stored_count = await self._store_pools_to_database(pools)
            
            # Get final stats
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            return ProcessorResult(
                success=True,
                data=pools,
                processed_count=stored_count,
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "stored_pools": stored_count
                }
            )
            
        except Exception as e:
            error_msg = f"Aerodrome V3 pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _process_events(self, events_path: Path, start_block: int) -> List[Dict[str, Any]]:
        """Process events from parquet files."""
        events_df = (
            pl.read_parquet(str(events_path / "*.parquet"))
            .filter(pl.col("block_number") >= start_block)
            .sort(pl.col("block_number"))
        )
        
        if events_df.is_empty():
            self.logger.info("No new events to process")
            return []
        
        pools = []
        for event in events_df.rows(named=True):
            pool_data = self._process_single_event(event)
            if pool_data:
                pools.append(pool_data)
        
        self.logger.info(f"Processed {len(pools)} pools from {len(events_df)} events")
        return pools
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single Aerodrome V3 PoolCreated event."""
        try:
            # Check factory address
            if event["address"] != self._factory_address:
                return None
            
            # Decode Aerodrome V3 PoolCreated event - similar to Uniswap V3
            token0 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic1"])[0]
            )
            token1 = to_checksum_address(
                eth_abi.decode(types=["address"], data=event["topic2"])[0]
            )
            fee = eth_abi.decode(types=["uint24"], data=event["topic3"])[0]
            
            tick_spacing, pool_address = eth_abi.decode(
                types=["int24", "address"], data=event["data"]
            )
            pool_address = to_checksum_address(pool_address)
            
            return {
                "address": self._to_serializable(pool_address),
                "fee": fee,
                "tick_spacing": tick_spacing,
                "asset0": self._to_serializable(token0),
                "asset1": self._to_serializable(token1),
                "type": self.lp_type,
                "creation_block": event["block_number"],
                "factory": self._to_serializable(event["address"]),
                "chain": self.chain
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to process Aerodrome V3 event: {e}")
            return None
