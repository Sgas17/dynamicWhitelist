"""
Protocol-specific configuration for dynamicWhitelist.
"""

from dataclasses import dataclass
from typing import Dict, List
from .base import BaseConfig


@dataclass
class ProtocolConfig(BaseConfig):
    """Configuration for different DeFi protocols."""
    
    # Event Hashes (these are standard across chains)
    ERC20_TRANSFER_EVENT: str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    UNISWAP_V3_POOL_CREATED_EVENT: str = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
    UNISWAP_V3_MINT_EVENT: str = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
    UNISWAP_V3_BURN_EVENT: str = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    
    @property 
    def uniswap_v2_config(self) -> Dict[str, Dict]:
        """Uniswap V2 configuration by chain."""
        return {
            "ethereum": {
                "factory_addresses": ["0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"],
                "router_address": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
                "deployment_block": 10000835,
                "fee": 3000  # 0.3%
            },
            "base": {
                "factory_addresses": ["0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"],
                "router_address": "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24",
                "deployment_block": 6601915,
                "fee": 3000
            },
            "arbitrum": {
                "factory_addresses": ["0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9"],
                "router_address": "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24",
                "deployment_block": 150442611,
                "fee": 3000
            }
        }
    
    @property
    def uniswap_v3_config(self) -> Dict[str, Dict]:
        """Uniswap V3 configuration by chain."""
        return {
            "ethereum": {
                "factory_addresses": [
                    "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # Main factory
                    "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"   # PancakeSwap V3 factory
                ],
                "deployment_block": 12369621,
                "pool_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                "quoter": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6"
            },
            "base": {
                "factory_addresses": [
                    "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
                    "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
                ],
                "deployment_block": 1371680,
                "pool_manager": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
                "quoter": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a"
            },
            "arbitrum": {
                "factory_addresses": [
                    "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
                ],
                "deployment_block": 165,
                "pool_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                "quoter": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6"
            }
        }
    
    @property
    def sushiswap_config(self) -> Dict[str, Dict]:
        """Sushiswap configuration by chain."""
        return {
            "ethereum": {
                "v2_factory": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
                "v3_factory": "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
                "deployment_block": 10794229,
                "router": "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F"
            },
            "base": {
                "v2_factory": "0x71524B4f93c58fcbF659783284E38825f0622859",
                "v3_factory": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
                "deployment_block": 1371680,
                "router": "0x6BDED42c6DA8FBf0d2bA55B2fa120C5e0c8D7891"
            },
            "arbitrum": {
                "v2_factory": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
                "v3_factory": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
                "deployment_block": 70,
                "router": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506"
            }
        }
    
    @property
    def supported_protocols(self) -> List[str]:
        """Get list of supported protocols."""
        return [
            "uniswap_v2",
            "uniswap_v3", 
            "uniswap_v4",
            "sushiswap_v2",
            "sushiswap_v3",
            "pancakeswap_v2",
            "pancakeswap_v3",
            "aerodrome_v2",
            "aerodrome_v3"
        ]
    
    def get_protocol_config(self, protocol: str, chain: str) -> Dict:
        """Get configuration for a specific protocol on a specific chain."""
        if protocol.startswith("uniswap_v2"):
            return self.uniswap_v2_config.get(chain, {})
        elif protocol.startswith("uniswap_v3"):
            return self.uniswap_v3_config.get(chain, {})
        elif protocol.startswith("sushiswap"):
            return self.sushiswap_config.get(chain, {})
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")
    
    def get_factory_addresses(self, protocol: str, chain: str) -> List[str]:
        """Get factory addresses for a protocol on a specific chain."""
        config = self.get_protocol_config(protocol, chain)
        
        # Handle different config structures
        if "factory_addresses" in config:
            return config["factory_addresses"]
        elif "v2_factory" in config and "v3_factory" in config:
            return [config["v2_factory"], config["v3_factory"]]
        elif "factory" in config:
            return [config["factory"]]
        else:
            return []
    
    def get_deployment_block(self, protocol: str, chain: str) -> int:
        """Get deployment block for a protocol on a specific chain."""
        config = self.get_protocol_config(protocol, chain)
        return config.get("deployment_block", 0)