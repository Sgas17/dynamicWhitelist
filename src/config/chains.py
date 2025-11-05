"""
Chain-specific configuration for dynamicWhitelist.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .base import BaseConfig


@dataclass
class ChainConfig(BaseConfig):
    """Chain-specific configuration for different blockchains."""

    # Default chain settings
    DEFAULT_CHAIN: str = BaseConfig.get_env("DEFAULT_CHAIN", "ethereum")

    # Chain-specific RPC URLs
    ETHEREUM_RPC_URL: str = BaseConfig.get_env(
        "ETHEREUM_RPC_URL", "http://100.104.193.35:8545"
    )
    BASE_RPC_URL: str = BaseConfig.get_env("BASE_RPC_URL", "https://mainnet.base.org")
    ARBITRUM_RPC_URL: str = BaseConfig.get_env(
        "ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc"
    )

    # Chain IDs
    ETHEREUM_CHAIN_ID: int = 1
    BASE_CHAIN_ID: int = 8453
    ARBITRUM_CHAIN_ID: int = 42161

    # Block settings
    BLOCKS_PER_MINUTE_ETHEREUM: int = 5  # ~12s block time
    BLOCKS_PER_MINUTE_BASE: int = 30  # ~2s block time
    BLOCKS_PER_MINUTE_ARBITRUM: int = 4  # ~15s block time

    # Batch processing settings
    DEFAULT_BLOCKS_PER_REQUEST: int = BaseConfig.get_env_int(
        "BLOCKS_PER_REQUEST", 10000
    )
    MAX_RETRY_ATTEMPTS: int = BaseConfig.get_env_int("MAX_RETRY_ATTEMPTS", 3)
    RETRY_DELAY_SECONDS: int = BaseConfig.get_env_int("RETRY_DELAY_SECONDS", 5)

    # Data fetching settings
    CHUNK_SIZE: int = BaseConfig.get_env_int("CHUNK_SIZE", 500)
    SMALL_CHUNK_SIZE: int = BaseConfig.get_env_int("SMALL_CHUNK_SIZE", 10)

    # Liquidity thresholds (in USD) for pool filtering
    MIN_LIQUIDITY_V2: float = BaseConfig.get_env_float(
        "MIN_LIQUIDITY_V2", 2000.0
    )  # $2k
    MIN_LIQUIDITY_V3: float = BaseConfig.get_env_float(
        "MIN_LIQUIDITY_V3", 2000.0
    )  # $2k
    MIN_LIQUIDITY_V4: float = BaseConfig.get_env_float(
        "MIN_LIQUIDITY_V4", 1000.0
    )  # $1k

    TRUSTED_TOKENS: List[str] = field(
        default_factory=lambda: ["weth", "usdc", "usdt", "dai", "wbtc", "usde"]
    )

    @property
    def supported_chains(self) -> Dict[str, Dict]:
        """Get configuration for all supported chains."""
        return {
            "ethereum": {
                "chain_id": self.ETHEREUM_CHAIN_ID,
                "rpc_url": self.ETHEREUM_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_ETHEREUM,
                "native_token": "ETH",
                "explorer_url": "https://etherscan.io",
            },
            "base": {
                "chain_id": self.BASE_CHAIN_ID,
                "rpc_url": self.BASE_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_BASE,
                "native_token": "ETH",
                "explorer_url": "https://basescan.org",
            },
            "arbitrum": {
                "chain_id": self.ARBITRUM_CHAIN_ID,
                "rpc_url": self.ARBITRUM_RPC_URL,
                "blocks_per_minute": self.BLOCKS_PER_MINUTE_ARBITRUM,
                "native_token": "ETH",
                "explorer_url": "https://arbiscan.io",
            },
        }

    def get_trusted_tokens_for_chain(
        self,
    ) -> Dict[str, Dict[str, str]]:
        """Get trusted token for a specific chain."""
        return {
            "ethereum": {
                "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                "usde": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
            },
            "base": {
                "weth": "0x4200000000000000000000000000000000000006",
                "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "usdt": "0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2",
                "dai": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                "wbtc": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
                "usde": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
            },
            "arbitrum": {
                "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "usdt": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620",
                "dai": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                "usde": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
            },
        }

    def get_chain_trusted_tokens(self, chain_name: str) -> Dict[str, hex]:
        """Get trusted tokens for a specific chain."""
        if chain_name not in self.supported_chains:
            raise ValueError(f"Unsupported chain: {chain_name}")
        return [
            token
            for token in self.TRUSTED_TOKENS
            if token in self.get_trusted_tokens_for_chain()[chain_name]
        ]

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
