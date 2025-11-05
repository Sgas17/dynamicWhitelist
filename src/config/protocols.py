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
    ERC20_TRANSFER_EVENT: str = (
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )
    UNISWAP_V2_PAIR_CREATED_EVENT: str = (
        "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
    )
    UNISWAP_V3_POOL_CREATED_EVENT: str = (
        "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
    )
    UNISWAP_V3_MINT_EVENT: str = (
        "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
    )
    UNISWAP_V3_BURN_EVENT: str = (
        "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    )
    UNISWAP_V4_INITIALIZED_EVENT: str = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"  # Confirmed on-chain
    UNISWAP_V4_MODIFY_LIQUIDITY_EVENT: str = (
        "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
    )
    AERODROME_V2_POOL_CREATED_EVENT: str = (
        "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e"
    )
    AERODROME_V3_POOL_CREATED_EVENT: str = (
        "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2"
    )

    @property
    def uniswap_v2_config(self) -> Dict[str, Dict]:
        """Uniswap V2 configuration by chain."""
        return {
            "ethereum": {
                "factory_addresses": [
                    "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",  # Uniswap V2
                    "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",  # Sushiswap V2
                    "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362",  # PancakeSwap V2
                ],
                "router_address": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
                "deployment_block": 10000835,  # Earliest deployment (Uniswap V2)
                "fee": 3000,  # 0.3%
            },
            "base": {
                "factory_addresses": [],
                "router_address": "",
                "deployment_block": 6601915,
                "fee": 3000,
            },
            "arbitrum": {
                "factory_addresses": [
                    "0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9",  # Uniswap V2
                    "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",  # Sushiswap V2
                    "0x02a84c5b5cd8d987671d4ff0e17ff5d862e4c0a2",  # PancakeSwap V2
                ],
                "router_address": "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24",
                "deployment_block": 150442611,
                "fee": 3000,
            },
        }

    @property
    def uniswap_v3_config(self) -> Dict[str, Dict]:
        """Uniswap V3 configuration by chain."""
        return {
            "ethereum": {
                "factory_addresses": [
                    "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
                    "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
                ],
                "deployment_block": 12369621,
                "pool_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                "quoter": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
            },
            "base": {
                "factory_addresses": [
                    "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
                    "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
                ],
                "deployment_block": 1371680,
                "pool_manager": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
                "quoter": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
            },
            "arbitrum": {
                "factory_addresses": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
                "deployment_block": 165,
                "pool_manager": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
                "quoter": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
            },
        }

    @property
    def uniswap_v4_config(self) -> Dict[str, Dict]:
        """Uniswap V4 configuration by chain."""
        return {
            "ethereum": {
                "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
                "deployment_block": 21688329,  # Update with actual block when deployed
            },
            "base": {
                "pool_manager": "0x498581fF718922c3f8e6A244956aF099B2652b2b",
                "deployment_block": 25350988,  # Update with actual block when deployed
            },
            "arbitrum": {
                "pool_manager": "0x360E68faCcca8cA495c1B759Fd9EEe466db9FB32",
                "deployment_block": 297842872,  # Update with actual block when deployed
            },
        }

    @property
    def aerodrome_v2_config(self) -> Dict[str, Dict]:
        """Aerodrome V2 configuration by chain."""
        return {
            "base": {
                "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
                "deployment_block": 3200559,
                "router": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
            }
        }

    @property
    def aerodrome_v3_config(self) -> Dict[str, Dict]:
        """Aerodrome configuration by chain (Base-specific)."""
        return {
            "base": {
                "factory": "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
                "deployment_block": 13843704,
                "router": "0x6Cb442acF35158D5eDa88fe602221b67B400Be3E",
            }
        }

    @property
    def supported_protocols(self) -> List[str]:
        """Get list of supported protocols."""
        return [
            "uniswap_v2",  # Includes all V2 forks (Sushiswap V2, PancakeSwap V2)
            "uniswap_v3",  # Includes all V3 forks (Sushiswap V3, PancakeSwap V3)
            "uniswap_v4",
            "aerodrome_v2",
            "aerodrome_v3",  # Base-specific protocol with different architecture
        ]

    def get_protocol_config(self, protocol: str, chain: str) -> Dict:
        """Get configuration for a specific protocol on a specific chain."""
        # All V2 forks use the same architecture and events
        if protocol.startswith("uniswap_v2"):
            return self.uniswap_v2_config.get(chain, {})
        # All V3 forks use the same architecture and events
        elif protocol.startswith("uniswap_v3"):
            return self.uniswap_v3_config.get(chain, {})
        elif protocol.startswith("uniswap_v4"):
            return self.uniswap_v4_config.get(chain, {})
        elif protocol.startswith("aerodrome_v2"):
            return self.aerodrome_v2_config.get(chain, {})
        elif protocol.startswith("aerodrome_v3"):
            return self.aerodrome_v3_config.get(chain, {})
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

    def get_factory_addresses(self, protocol: str, chain: str) -> List[str]:
        """Get factory addresses for a protocol on a specific chain."""
        config = self.get_protocol_config(protocol, chain)

        # Handle different config structures
        if "factory_addresses" in config:
            return config["factory_addresses"]
        elif "factory" in config:
            return [config["factory"]]
        elif "pool_manager" in config:
            return [config["pool_manager"]]
        else:
            return []

    def get_event_hash(self, event_type: str) -> str:
        """Get event hash for a specific event type."""
        event_map = {
            "erc20_transfer": self.ERC20_TRANSFER_EVENT,
            "uniswap_v2_pair_created": self.UNISWAP_V2_PAIR_CREATED_EVENT,
            "uniswap_v3_pool_created": self.UNISWAP_V3_POOL_CREATED_EVENT,
            "uniswap_v3_mint": self.UNISWAP_V3_MINT_EVENT,
            "uniswap_v3_burn": self.UNISWAP_V3_BURN_EVENT,
            "uniswap_v4_initialized": self.UNISWAP_V4_INITIALIZED_EVENT,
            "uniswap_v4_modify_liquidity": self.UNISWAP_V4_MODIFY_LIQUIDITY_EVENT,
            # V4 uses single ModifyLiquidity event (not separate mint/burn)
            "uniswap_v4_mint": self.UNISWAP_V4_MODIFY_LIQUIDITY_EVENT,
            "uniswap_v4_burn": self.UNISWAP_V4_MODIFY_LIQUIDITY_EVENT,
            "aerodrome_v2_pool_created": self.AERODROME_V2_POOL_CREATED_EVENT,
            "aerodrome_v3_pool_created": self.AERODROME_V3_POOL_CREATED_EVENT,
            "aerodrome_v3_mint": self.UNISWAP_V3_MINT_EVENT,  # aerodrome v3 uses the same mint event as uniswap v3
            "aerodrome_v3_burn": self.UNISWAP_V3_BURN_EVENT,  # aerodrome v3 uses the same burn event as uniswap v3
        }
        if event_type not in event_map:
            raise ValueError(f"Unknown event type: {event_type}")
        return event_map[event_type]

    def get_deployment_block(self, protocol: str, chain: str) -> int:
        """Get deployment block for a protocol on a specific chain."""
        config = self.get_protocol_config(protocol, chain)
        return config.get("deployment_block", 0)
