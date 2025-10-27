"""
Base processor classes for data pipeline components.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
import ujson
from hexbytes import HexBytes

logger = logging.getLogger(__name__)


class ProcessorError(Exception):
    """Base exception for processor errors."""
    pass


@dataclass
class ProcessorResult:
    """Result from processor execution."""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    processed_count: int = 0
    
    @property
    def failed(self) -> bool:
        """Check if processing failed."""
        return not self.success


class BaseProcessor(ABC):
    """
    Abstract base class for pool processors.
    
    KISS principle: Each processor handles one specific protocol's pool creation events.
    """
    
    def __init__(self, chain: str, protocol: str):
        """
        Initialize processor.
        
        Args:
            chain: Blockchain chain name (e.g., 'ethereum', 'base')
            protocol: Protocol name (e.g., 'uniswap_v2', 'aerodrome_v2')
        """
        self.chain = chain
        self.protocol = protocol
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        
        # Load configuration
        from src.config import ConfigManager
        from src.core.storage.postgres import PostgresStorage
        
        self.config = ConfigManager()
        
        # Convert database config to dictionary format expected by PostgresStorage
        db_config = {
            'host': self.config.database.POSTGRES_HOST,
            'port': self.config.database.POSTGRES_PORT,
            'user': self.config.database.POSTGRES_USER,
            'password': self.config.database.POSTGRES_PASSWORD,
            'database': self.config.database.POSTGRES_DB,
            'pool_size': self.config.database.MAX_CONNECTIONS,
            'pool_timeout': self.config.database.CONNECTION_TIMEOUT
        }
        self.storage = PostgresStorage(db_config)

    def _setup_blacklist(self, blacklist_file: Optional[str] = None, auto_update: bool = False):
        """
        Setup token blacklist manager (optional feature).
        
        Args:
            blacklist_file: Path to blacklist JSON file
            auto_update: Whether to auto-update blacklist from Etherscan
        """
        try:
            from src.utils.token_blacklist_manager import TokenBlacklistManager
            
            if not blacklist_file:
                blacklist_file = f"data/token_blacklist_{self.chain}.json"
            
            self.blacklist_manager = TokenBlacklistManager(
                blacklist_file=blacklist_file,
                cache_file=f"data/etherscan_cache/etherscan_labels_{self.chain}.json",
                auto_update=auto_update,
            )
            count = len(self.blacklist_manager.blacklist)
            self.logger.info(f"Blacklist initialized: {count} entries for {self.chain}")
            return True
        except Exception as e:
            self.logger.warning(f"Failed to setup blacklist: {e}")
            self.blacklist_manager = None
            return False
    
    def _filter_blacklisted_pools(self, pools: List[Dict[str, Any]]) -> tuple[List[Dict], List[Dict]]:
        """
        Filter out pools containing blacklisted tokens (optional feature).
        
        Requires _setup_blacklist() to be called first.
        
        Args:
            pools: List of pool dictionaries
            
        Returns:
            (clean_pools, filtered_pools) - Pools without and with blacklisted tokens
        """
        if not hasattr(self, 'blacklist_manager') or self.blacklist_manager is None:
            return pools, []
        
        clean_pools = []
        filtered_pools = []
        
        for pool in pools:
            # Check if any token is blacklisted
            blacklisted_tokens = []
            for asset_key in ["asset0", "asset1", "asset2", "asset3"]:
                token_address = pool.get(asset_key)
                if token_address and self.blacklist_manager.is_blacklisted(token_address):
                    blacklisted_tokens.append(token_address)
            
            if blacklisted_tokens:
                pool["is_blacklisted"] = True
                pool["blacklist_reason"] = f"Contains blacklisted tokens: {', '.join(blacklisted_tokens)}"
                filtered_pools.append(pool)
                self.logger.info(f"Filtered pool {pool['address']}: {pool['blacklist_reason']}")
            else:
                clean_pools.append(pool)
        
        return clean_pools, filtered_pools
    
    def _cleanup_old_parquet_files(self, parquet_path: Path, keep_latest: bool = True):
        """
        Clean up old parquet files after processing (optional feature).
        
        Args:
            parquet_path: Path to parquet directory
            keep_latest: If True, keep the latest parquet file for reference
        """
        try:
            if not parquet_path.exists():
                return
            
            parquet_files = sorted(parquet_path.glob("*.parquet"), key=lambda p: p.stat().st_mtime)
            
            if not parquet_files:
                return
            
            if keep_latest:
                files_to_delete = parquet_files[:-1]
            else:
                files_to_delete = parquet_files
            
            for file in files_to_delete:
                file.unlink()
                self.logger.info(f"Deleted old parquet file: {file.name}")
            
            self.logger.info(f"Cleaned up {len(files_to_delete)} old parquet files")
            
        except Exception as e:
            self.logger.warning(f"Error cleaning up parquet files: {e}")
    
    @abstractmethod
    async def process(self, **kwargs) -> ProcessorResult:
        """
        Process pool creation events from parquet files.
        
        Args:
            start_block: Block to start processing from (optional)
            events_path: Path to parquet events (optional)
            
        Returns:
            ProcessorResult: Processing results with pools data
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate processor configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        pass
    
    @abstractmethod
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single pool creation event into standardized pool data.
        
        Args:
            event: Raw event data from parquet
            
        Returns:
            Pool data dict or None if event should be skipped
        """
        pass
    
    def get_identifier(self) -> str:
        """Get unique identifier for this processor."""
        return f"{self.chain}_{self.protocol}_processor"
    
    def log_result(self, result: ProcessorResult) -> None:
        """Log processing result."""
        if result.success:
            self.logger.info(
                f"Processing completed: {result.processed_count} items processed"
            )
        else:
            self.logger.error(f"Processing failed: {result.error}")
    
    async def _get_last_processed_block(self) -> int:
        """Get the last processed block from database."""
        try:
            if not self.storage.pool:
                await self.storage.connect()
            
            # Query the highest creation_block for this chain/protocol
            table_name = self.storage._get_pool_table_name(self.chain)
            
            async with self.storage.pool.acquire() as conn:
                query = f"""
                    SELECT MAX(creation_block) as max_block 
                    FROM {table_name}
                    WHERE additional_data->>'protocol' = $1
                """
                result = await conn.fetchrow(query, self.protocol)
                last_block = result['max_block'] if result and result['max_block'] else 0
                
                self.logger.info(f"Last processed block for {self.protocol}: {last_block}")
                return last_block
                
        except Exception as e:
            self.logger.warning(f"Could not get last processed block: {e}")
            return 0
    
    async def _store_pools_to_database(self, pools: List[Dict[str, Any]]) -> int:
        """Store pools to database using the storage layer."""
        if not pools:
            return 0

        try:
            if not self.storage.pool:
                await self.storage.connect()

            # Store pools directly without modifying additional_data
            # Protocol filtering should be done by factory address, not additional_data
            count = await self.storage.store_pools_batch(pools, self.chain, self.protocol)
            self.logger.info(f"Stored {count} pools to database for {self.chain}/{self.protocol}")
            return count

        except Exception as e:
            self.logger.error(f"Failed to store pools to database: {e}")
            raise
    
    def _to_serializable(self, value: Any) -> Any:
        """Convert value to serializable format."""
        if value is None:
            return None
        if isinstance(value, (bytes, HexBytes)):
            return '0x' + value.hex()
        return str(value) if not isinstance(value, (int, float, bool, str)) else value
    
    # Legacy methods for backward compatibility - now deprecated
    def _load_existing_data(self, output_file: Path) -> tuple[List[Dict], int]:
        """
        DEPRECATED: Load existing pool data from JSON file.
        Use _get_last_processed_block() instead.
        """
        self.logger.warning("_load_existing_data is deprecated, use database storage")
        return [], 0
    
    def _deduplicate_pools(self, pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        DEPRECATED: Remove duplicate pools by address.
        Database handles deduplication via ON CONFLICT.
        """
        self.logger.warning("_deduplicate_pools is deprecated, database handles deduplication")
        return pools
    
    def _save_data(self, data: List[Dict[str, Any]], output_file: Path) -> None:
        """
        DEPRECATED: Save data to JSON file.
        Use _store_pools_to_database() instead.
        """
        self.logger.warning("_save_data is deprecated, use database storage")