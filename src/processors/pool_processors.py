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

from .base import BaseProcessor, ProcessorResult, ProcessorError
from ..config.manager import ConfigManager


class UniswapV3PoolProcessor(BaseProcessor):
    """
    Process Uniswap V3 PoolCreated events into structured pool data.
    
    Refactored from uniswap_v3_pool_fetcher_parquet.py
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize Uniswap V3 pool processor."""
        super().__init__(chain, "uniswap_v3")
        self.config = ConfigManager()
        
        # Factory addresses by chain
        self.factory_addresses = {
            "ethereum": [
                "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
                "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
            ]
        }.get(chain, [])
        
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
        events_path: Optional[str] = None,
        output_file: Optional[str] = None
    ) -> ProcessorResult:
        """
        Process Uniswap V3 pool events.
        
        Args:
            start_block: Block to start processing from
            events_path: Path to parquet events (optional)
            output_file: JSON output file path (optional)
            
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
            
            if not output_file:
                output_file = data_dir / f"{self.chain}_lps_{self.protocol}.json"
            else:
                output_file = Path(output_file)
            
            # Load existing data if available
            existing_data, last_block = self._load_existing_data(output_file)
            start_block = max(start_block, last_block)
            
            # Read and process events
            pools = await self._process_events(events_path, start_block)
            
            if not pools:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No new events to process"}
                )
            
            # Combine with existing data and deduplicate
            all_pools = existing_data + pools
            unique_pools = self._deduplicate_pools(all_pools)
            
            # Get last event block
            events_df = pl.read_parquet(str(events_path / "*.parquet"))
            last_event_block = events_df.select(pl.col("block_number")).max().item()
            
            # Add metadata
            final_data = unique_pools + [{
                "block_number": last_event_block,
                "number_of_pools": len(unique_pools)
            }]
            
            # Save to file
            self._save_data(final_data, output_file)
            
            return ProcessorResult(
                success=True,
                data=unique_pools,
                processed_count=len(pools),
                metadata={
                    "start_block": start_block,
                    "end_block": last_event_block,
                    "total_pools": len(unique_pools),
                    "output_file": str(output_file)
                }
            )
            
        except Exception as e:
            error_msg = f"Pool processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    def _load_existing_data(self, output_file: Path) -> tuple[List[Dict], int]:
        """Load existing pool data from JSON file."""
        try:
            with open(output_file, "r") as f:
                data = ujson.load(f)
            
            # Remove metadata (last item) and get last block
            metadata = data.pop(-1) if data else {}
            last_block = metadata.get("block_number", 0)
            
            self.logger.info(f"Loaded {len(data)} existing pools up to block {last_block}")
            return data, last_block
            
        except FileNotFoundError:
            self.logger.info("No existing pool data found")
            return [], 0
    
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
    
    def _deduplicate_pools(self, pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate pools by address."""
        seen = set()
        unique = []
        duplicates = 0
        
        for pool in pools:
            if isinstance(pool, dict) and 'address' in pool:
                addr = pool['address']
                if addr not in seen:
                    seen.add(addr)
                    unique.append(pool)
                else:
                    duplicates += 1
        
        if duplicates > 0:
            self.logger.warning(f"Removed {duplicates} duplicate pools")
        
        return unique
    
    def _save_data(self, data: List[Dict[str, Any]], output_file: Path) -> None:
        """Save data to JSON file."""
        # Make all values serializable
        serializable_data = []
        for entry in data:
            if isinstance(entry, dict):
                serializable_data.append({k: self._to_serializable(v) for k, v in entry.items()})
            else:
                serializable_data.append(entry)
        
        with open(output_file, "w") as f:
            ujson.dump(serializable_data, f, indent=2)
        
        self.logger.info(f"Saved data to {output_file}")
    
    def _to_serializable(self, value: Any) -> Any:
        """Convert value to serializable format."""
        if value is None:
            return None
        if isinstance(value, (bytes, HexBytes)):
            return '0x' + value.hex()
        return str(value) if not isinstance(value, (int, float, bool, str)) else value


class UniswapV4PoolProcessor(BaseProcessor):
    """
    Process Uniswap V4 PoolCreated events.
    
    Refactored from uniswap_v4_pool_fetcher_parquet.py
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize Uniswap V4 pool processor."""
        super().__init__(chain, "uniswap_v4")
        self.config = ConfigManager()
        
        # V4 factory addresses (update when available)
        self.factory_addresses = {
            "ethereum": [
                # Add V4 factory addresses when deployed
            ]
        }.get(chain, [])
        
        self.lp_type = "UniswapV4"
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        # For now, allow empty factory addresses as V4 might not be deployed yet
        return True
    
    async def process(self, **kwargs) -> ProcessorResult:
        """
        Process Uniswap V4 pool events.
        
        Note: Implementation pending V4 deployment and final event structure.
        """
        return ProcessorResult(
            success=True,
            data=[],
            processed_count=0,
            metadata={"message": "V4 processor placeholder - implementation pending deployment"}
        )