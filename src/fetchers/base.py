"""
Base classes for blockchain data fetchers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Base exception for fetch-related errors."""
    pass


@dataclass
class FetchResult:
    """Result from fetch execution."""
    success: bool
    data_path: Optional[str] = None
    fetched_blocks: int = 0
    start_block: Optional[int] = None
    end_block: Optional[int] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def failed(self) -> bool:
        """Check if fetch failed."""
        return not self.success


class BaseFetcher(ABC):
    """
    Abstract base class for blockchain data fetchers.
    
    KISS principle: Each fetcher handles one specific chain's data collection.
    """
    
    def __init__(self, chain: str, rpc_url: str):
        """
        Initialize fetcher.
        
        Args:
            chain: Blockchain chain name (e.g., 'ethereum')
            rpc_url: RPC endpoint URL
        """
        self.chain = chain
        self.rpc_url = rpc_url
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    async def fetch_logs(
        self,
        start_block: int,
        end_block: int,
        contracts: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
        output_dir: Optional[str] = None
    ) -> FetchResult:
        """
        Fetch blockchain logs/events.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number  
            contracts: List of contract addresses to filter
            events: List of event signatures to filter
            output_dir: Directory to save fetched data
            
        Returns:
            FetchResult: Result of fetch operation
        """
        pass
    
    @abstractmethod
    async def get_latest_block(self) -> int:
        """
        Get the latest finalized block number.
        
        Returns:
            int: Latest block number
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate fetcher configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        pass
    
    def get_identifier(self) -> str:
        """Get unique identifier for this fetcher."""
        return f"{self.chain}_fetcher"
    
    def log_result(self, result: FetchResult) -> None:
        """Log fetch result."""
        if result.success:
            self.logger.info(
                f"Fetch completed: {result.fetched_blocks} blocks "
                f"({result.start_block}-{result.end_block})"
            )
        else:
            self.logger.error(f"Fetch failed: {result.error}")
    
    async def calculate_block_range(self, hours_back: int, blocks_per_minute: int = 5) -> tuple[int, int]:
        """
        Calculate block range for time period.
        
        Args:
            hours_back: Hours to look back
            blocks_per_minute: Blocks per minute for this chain
            
        Returns:
            tuple: (start_block, end_block)
        """
        minutes = hours_back * 60
        blocks_to_fetch = minutes * blocks_per_minute
        
        # Get actual latest block from chain
        end_block = await self.get_latest_block()
        start_block = end_block - blocks_to_fetch
        
        return start_block, end_block