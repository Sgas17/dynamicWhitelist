"""
Base configuration management for dynamicWhitelist.
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Exception raised for configuration-related errors."""
    pass


@dataclass
class BaseConfig:
    """Base configuration class with environment variable management."""
    
    # Project paths
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "data"
    
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "local")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    def __post_init__(self):
        """Initialize configuration after dataclass creation."""
        self._setup_logging()
        self._validate_config()
        self._ensure_directories()
    
    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, self.LOG_LEVEL.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    def _validate_config(self):
        """Validate configuration values."""
        if self.ENVIRONMENT not in ["local", "dev", "staging", "production"]:
            raise ConfigError(f"Invalid environment: {self.ENVIRONMENT}")
    
    def _ensure_directories(self):
        """Ensure required directories exist."""
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data directory: {self.DATA_DIR}")
    
    @staticmethod
    def get_env(key: str, default: Optional[str] = None, required: bool = False) -> str:
        """
        Get environment variable with validation.
        
        Args:
            key: Environment variable name
            default: Default value if not found
            required: Whether the variable is required
            
        Returns:
            Environment variable value
            
        Raises:
            ConfigError: If required variable is missing
        """
        value = os.getenv(key, default)
        if required and value is None:
            raise ConfigError(f"Required environment variable '{key}' is not set")
        return value
    
    @staticmethod
    def get_env_int(key: str, default: Optional[int] = None, required: bool = False) -> int:
        """Get environment variable as integer."""
        value = BaseConfig.get_env(key, str(default) if default is not None else None, required)
        try:
            return int(value)
        except (ValueError, TypeError):
            raise ConfigError(f"Environment variable '{key}' must be an integer, got: {value}")
    
    @staticmethod
    def get_env_float(key: str, default: Optional[float] = None, required: bool = False) -> float:
        """Get environment variable as float."""
        value = BaseConfig.get_env(key, str(default) if default is not None else None, required)
        try:
            return float(value)
        except (ValueError, TypeError):
            raise ConfigError(f"Environment variable '{key}' must be a float, got: {value}")

    @staticmethod
    def get_env_bool(key: str, default: bool = False) -> bool:
        """Get environment variable as boolean."""
        value = BaseConfig.get_env(key, str(default))
        return value.lower() in ("true", "1", "yes", "on")

    @staticmethod
    def get_env_list(key: str, default: Optional[List[str]] = None, separator: str = ",") -> List[str]:
        """Get environment variable as list."""
        value = BaseConfig.get_env(key, separator.join(default) if default else "")
        return [item.strip() for item in value.split(separator) if item.strip()] if value else []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            field: getattr(self, field) 
            for field in self.__dataclass_fields__ 
            if not field.startswith('_')
        }