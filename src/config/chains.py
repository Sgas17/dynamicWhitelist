"""
Chain-specific configuration for dynamicWhitelist.
"""

from dataclasses import dataclass
from typing import Dict, Optional
from .base import BaseConfig


@dataclass
class ChainConfig(BaseConfig):
    """Chain-specific configuration for different blockchains."""
    
    # Default chain settings
    DEFAULT_CHAIN: str = BaseConfig.get_env("DEFAULT_CHAIN", "ethereum")
    
    # Chain-specific RPC URLs
    ETHEREUM_RPC_URL: str = BaseConfig.get_env("ETHEREUM_RPC_URL", "http://100.104.193.35:8545")
    BASE_RPC_URL: str = BaseConfig.get_env("BASE_RPC_URL", "https://mainnet.base.org")
    ARBITRUM_RPC_URL: str = BaseConfig.get_env("ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc")
    
    # Chain IDs
    ETHEREUM_CHAIN_ID: int = 1
    BASE_CHAIN_ID: int = 8453
    ARBITRUM_CHAIN_ID: int = 42161
    
    # Block settings
    BLOCKS_PER_MINUTE_ETHEREUM: int = 5  # ~12s block time
    BLOCKS_PER_MINUTE_BASE: int = 30     # ~2s block time
    BLOCKS_PER_MINUTE_ARBITRUM: int = 4  # ~15s block time
    
    # Batch processing settings
    DEFAULT_BLOCKS_PER_REQUEST: int = BaseConfig.get_env_int("BLOCKS_PER_REQUEST", 10000)
    MAX_RETRY_ATTEMPTS: int = BaseConfig.get_env_int("MAX_RETRY_ATTEMPTS", 3)
    RETRY_DELAY_SECONDS: int = BaseConfig.get_env_int("RETRY_DELAY_SECONDS", 5)
    
    # Data fetching settings
    CHUNK_SIZE: int = BaseConfig.get_env_int("CHUNK_SIZE", 500)
    SMALL_CHUNK_SIZE: int = BaseConfig.get_env_int("SMALL_CHUNK_SIZE", 10)
    
    @property
    def supported_chains(self) -> Dict[str, Dict]:
        """Get configuration for all supported chains."""
        return {
            "ethereum": {
                "chain_id": self.ETHEREUM_CHAIN_ID,
                "rpc_url": self.ETHEREUM_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_ETHEREUM,
                "native_token": "ETH",
                "explorer_url": "https://etherscan.io"
            },
            "base": {
                "chain_id": self.BASE_CHAIN_ID,
                "rpc_url": self.BASE_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_BASE,
                "native_token": "ETH",
                "explorer_url": "https://basescan.org"
            },
            "arbitrum": {
                "chain_id": self.ARBITRUM_CHAIN_ID,
                "rpc_url": self.ARBITRUM_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_ARBITRUM,
                "native_token": "ETH",
                "explorer_url": "https://arbiscan.io"
            }
        }
    
    def get_chain_config(self, chain_name: str) -> Dict:
        """Get configuration for a specific chain."""
        if chain_name not in self.supported_chains:
            raise ValueError(f"Unsupported chain: {chain_name}")
        return self.supported_chains[chain_name]
    
    def get_rpc_url(self, chain_name: str) -> str:
        """Get RPC URL for a specific chain."""
        return self.get_chain_config(chain_name)["rpc_url"]
    
    def get_chain_id(self, chain_name: str) -> int:
        """Get chain ID for a specific chain."""
        return self.get_chain_config(chain_name)["chain_id"]
    
    def get_blocks_per_minute(self, chain_name: str) -> int:
        """Get blocks per minute for a specific chain."""
        return self.get_chain_config(chain_name)["blocks_per_minute"]
    
    def calculate_blocks_for_time_range(self, chain_name: str, minutes: int) -> int:
        """Calculate number of blocks for a given time range in minutes."""
        blocks_per_minute = self.get_blocks_per_minute(chain_name)
        return minutes * blocks_per_minute
    
    def get_data_directory(self, chain_name: str) -> str:
        """Get data directory path for a specific chain."""
        return str(self.DATA_DIR / chain_name)