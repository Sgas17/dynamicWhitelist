"""
Base processor classes for data pipeline components.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import logging

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
    Abstract base class for data processors.
    
    KISS principle: Each processor handles one specific data source or transformation.
    """
    
    def __init__(self, chain: str, protocol: str):
        """
        Initialize processor.
        
        Args:
            chain: Blockchain chain name (e.g., 'ethereum')
            protocol: Protocol name (e.g., 'uniswap_v3')
        """
        self.chain = chain
        self.protocol = protocol
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    async def process(self, **kwargs) -> ProcessorResult:
        """
        Process data from the specified source.
        
        Returns:
            ProcessorResult: Result of processing operation
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