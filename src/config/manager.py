"""
Configuration manager for dynamicWhitelist.

This module provides a centralized way to access all configuration settings
across the application. It combines all configuration classes into a single
easy-to-use interface.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Any
from .base import BaseConfig, ConfigError
from .database import DatabaseConfig
from .chains import ChainConfig
from .protocols import ProtocolConfig
from .nats_config import NatsConfig

logger = logging.getLogger(__name__)


@dataclass
class ConfigManager:
    """
    Centralized configuration manager that combines all configuration classes.
    
    This class provides easy access to all configuration settings and ensures
    that configurations are properly initialized and validated.
    """
    
    def __init__(self, environment: str = None):
        """
        Initialize the configuration manager.
        
        Args:
            environment: Override the environment (local, dev, staging, production)
        """
        self._environment = environment
        self._base_config = None
        self._database_config = None
        self._chain_config = None
        self._protocol_config = None
        self._nats_config = None
        self._initialize_configs()
    
    def _initialize_configs(self):
        """Initialize all configuration objects."""
        try:
            # Initialize base configuration first
            self._base_config = BaseConfig()
            if self._environment:
                self._base_config.ENVIRONMENT = self._environment
            
            # Initialize all other configurations
            self._database_config = DatabaseConfig()
            self._chain_config = ChainConfig()
            self._protocol_config = ProtocolConfig()
            self._nats_config = NatsConfig()
            
            logger.info(f"Configuration initialized for environment: {self.environment}")
            
        except Exception as e:
            logger.error(f"Failed to initialize configuration: {e}")
            raise ConfigError(f"Configuration initialization failed: {e}")
    
    @property
    def environment(self) -> str:
        """Get current environment."""
        return self._base_config.ENVIRONMENT
    
    @property
    def base(self) -> BaseConfig:
        """Get base configuration."""
        return self._base_config
    
    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration.""" 
        return self._database_config
    
    @property
    def chains(self) -> ChainConfig:
        """Get chain configuration."""
        return self._chain_config
    
    @property
    def protocols(self) -> ProtocolConfig:
        """Get protocol configuration."""
        return self._protocol_config
    
    @property
    def nats(self) -> NatsConfig:
        """Get NATS configuration."""
        return self._nats_config
    
    def get_chain_database_config(self, chain_name: str) -> Dict[str, Any]:
        """
        Get combined database and chain configuration for a specific chain.
        
        Args:
            chain_name: Name of the blockchain (ethereum, base, arbitrum)
            
        Returns:
            Combined configuration dictionary
        """
        chain_config = self.chains.get_chain_config(chain_name)
        return {
            "chain_name": chain_name,
            "chain_id": chain_config["chain_id"],
            "rpc_url": chain_config["rpc_url"],
            "postgres_url": self.database.postgres_url,
            "redis_config": self.database.get_redis_connection_kwargs(),
            "table_name": self.database.pools_table_name,
            "data_directory": self.chains.get_data_directory(chain_name)
        }
    
    def get_protocol_chain_config(self, protocol: str, chain: str) -> Dict[str, Any]:
        """
        Get combined protocol and chain configuration.
        
        Args:
            protocol: Protocol name (uniswap_v3, sushiswap_v2, etc.)
            chain: Chain name (ethereum, base, arbitrum)
            
        Returns:
            Combined configuration dictionary
        """
        protocol_config = self.protocols.get_protocol_config(protocol, chain)
        chain_config = self.chains.get_chain_config(chain)
        
        return {
            "protocol": protocol,
            "chain": chain,
            "rpc_url": chain_config["rpc_url"],
            "factory_addresses": self.protocols.get_factory_addresses(protocol, chain),
            "deployment_block": self.protocols.get_deployment_block(protocol, chain),
            "blocks_per_minute": chain_config["blocks_per_minute"],
            "data_directory": self.chains.get_data_directory(chain),
            **protocol_config
        }
    
    def get_nats_publishing_config(self, chain: str = None, protocol: str = None) -> Dict[str, Any]:
        """
        Get NATS publishing configuration for specific chain or protocol.
        
        Args:
            chain: Chain name for token subjects
            protocol: Protocol name for pool subjects
            
        Returns:
            NATS configuration dictionary
        """
        config = {
            "enabled": self.nats.NATS_ENABLED,
            "url": self.nats.get_nats_url(),
            "stream_name": self.nats.STREAM_NAME,
            "connection_params": self.nats.connection_params,
            "jetstream_config": self.nats.jetstream_config
        }
        
        if chain:
            config["token_subject"] = self.nats.get_token_subject(chain)
        
        if protocol:
            config["pool_subject"] = self.nats.get_pool_subject(protocol)
            
        return config
    
    def validate_configuration(self) -> bool:
        """
        Validate all configuration settings.
        
        Returns:
            True if all configurations are valid
            
        Raises:
            ConfigError: If any configuration is invalid
        """
        try:
            # Validate database connection can be established
            if not self.database.postgres_url:
                raise ConfigError("PostgreSQL URL not configured")
            
            # Validate at least one chain is properly configured
            supported_chains = self.chains.supported_chains
            if not supported_chains:
                raise ConfigError("No chains configured")
            
            # Validate protocols have factory addresses
            for protocol in self.protocols.supported_protocols:
                for chain in supported_chains:
                    try:
                        addresses = self.protocols.get_factory_addresses(protocol, chain)
                        if not addresses:
                            logger.warning(f"No factory addresses for {protocol} on {chain}")
                    except ValueError:
                        # Protocol not supported on this chain - that's okay
                        continue
            
            logger.info("Configuration validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise ConfigError(f"Configuration validation failed: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert all configurations to dictionary format."""
        return {
            "environment": self.environment,
            "base": self.base.to_dict() if self.base else {},
            "database": self.database.to_dict() if self.database else {},
            "chains": self.chains.to_dict() if self.chains else {},
            "protocols": self.protocols.to_dict() if self.protocols else {},
            "nats": self.nats.to_dict() if self.nats else {}
        }
    
    def __repr__(self) -> str:
        """String representation of the configuration manager."""
        return f"ConfigManager(environment={self.environment})"


# Global configuration manager instance
_config_manager = None


def get_config(environment: str = None, force_reload: bool = False) -> ConfigManager:
    """
    Get the global configuration manager instance.
    
    Args:
        environment: Override environment
        force_reload: Force reload of configuration
        
    Returns:
        ConfigManager instance
    """
    global _config_manager
    
    if _config_manager is None or force_reload:
        _config_manager = ConfigManager(environment=environment)
        _config_manager.validate_configuration()
    
    return _config_manager


def reload_config(environment: str = None) -> ConfigManager:
    """
    Reload the global configuration manager.
    
    Args:
        environment: Override environment
        
    Returns:
        New ConfigManager instance
    """
    return get_config(environment=environment, force_reload=True)