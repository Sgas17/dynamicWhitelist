"""
Liquidity calculator for DEX pools.

Calculates slippage and liquidity for UniswapV2, V3, and V4 pools.
Integrates with batch calling for efficient on-chain data fetching.
"""

import asyncio
from typing import Dict, Optional, Tuple, List, Any
from decimal import Decimal
from web3 import Web3
from eth_utils import to_checksum_address
from eth_abi.abi import encode, decode
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class BatchLiquidityFetcher:
    """Batch fetch liquidity data for multiple pools efficiently."""

    def __init__(self, w3: Web3, chain_id: int):
        self.w3 = w3
        self.chain_id = chain_id
        self.contracts_dir = Path(f"itrc_chain_data/itrc_contracts/{chain_id}")

    def load_bytecode(self, contract_name: str) -> str:
        """Load contract bytecode from JSON file."""
        json_path = self.contracts_dir / f"{contract_name}.json"
        if json_path.exists():
            with open(json_path, "r") as f:
                data = json.loads(f.read())
                return data['bytecode']['object']
        else:
            # Fallback to default location
            return ""

    async def batch_fetch_v2_reserves(
        self,
        pool_addresses: List[str]
    ) -> Dict[str, Tuple[int, int]]:
        """
        Batch fetch reserves for multiple V2 pools.

        Returns:
            Dict mapping pool address to (reserve0, reserve1) tuple
        """
        try:
            # Load batch getter bytecode
            bytecode = self.load_bytecode("UniswapReservesGetter")
            if not bytecode:
                # Fallback to individual calls
                return await self._fetch_v2_reserves_individual(pool_addresses)

            # Encode pool addresses
            input_data = encode(["address[]"], [pool_addresses])
            call_data = bytecode + input_data.hex()

            # Make eth_call
            result = self.w3.eth.call({
                "data": "0x" + call_data
            }, "latest")

            # Decode results
            decoded = decode(
                ["uint256[]", "uint256[]", "uint256[]"],
                result
            )

            reserves = {}
            for i, addr in enumerate(pool_addresses):
                reserves[addr.lower()] = (decoded[0][i], decoded[1][i])

            return reserves

        except Exception as e:
            logger.warning(f"Batch fetch failed: {e}, falling back to individual calls")
            return await self._fetch_v2_reserves_individual(pool_addresses)

    async def _fetch_v2_reserves_individual(
        self,
        pool_addresses: List[str]
    ) -> Dict[str, Tuple[int, int]]:
        """Fallback to fetch reserves individually."""
        reserves = {}

        # Minimal ABI for getReserves
        abi = [{
            "constant": True,
            "inputs": [],
            "name": "getReserves",
            "outputs": [
                {"name": "reserve0", "type": "uint112"},
                {"name": "reserve1", "type": "uint112"},
                {"name": "blockTimestampLast", "type": "uint32"}
            ],
            "type": "function"
        }]

        for addr in pool_addresses:
            try:
                addr = to_checksum_address(addr)
                contract = self.w3.eth.contract(address=addr, abi=abi)
                result = contract.functions.getReserves().call()
                reserves[addr.lower()] = (result[0], result[1])
            except Exception as e:
                logger.error(f"Failed to get reserves for {addr}: {e}")
                reserves[addr.lower()] = (0, 0)

        return reserves


class UniswapV2LiquidityCalculator:
    """Calculate liquidity and slippage for Uniswap V2 pools."""

    def __init__(self, w3: Web3, chain_id: int):
        self.w3 = w3
        self.chain_id = chain_id
        self.batch_fetcher = BatchLiquidityFetcher(w3, chain_id)

    async def calculate_slippage_batch(
        self,
        pools_data: List[Dict[str, Any]],
        trade_amount_usd: Decimal = Decimal("1000"),
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate slippage for multiple V2 pools efficiently.

        Args:
            pools_data: List of pool data dicts with address, token0, token1
            trade_amount_usd: Trade size in USD
            token_prices: Token price mapping

        Returns:
            List of slippage calculation results
        """
        pool_addresses = [p["address"] for p in pools_data]

        # Batch fetch reserves
        reserves_map = await self.batch_fetcher.batch_fetch_v2_reserves(pool_addresses)

        results = []
        for pool in pools_data:
            addr = pool["address"].lower()
            if addr not in reserves_map:
                results.append({
                    "pool_address": pool["address"],
                    "error": "Failed to fetch reserves"
                })
                continue

            reserve0, reserve1 = reserves_map[addr]

            # Calculate slippage
            result = self._calculate_v2_slippage(
                pool["address"],
                pool.get("token0"),
                pool.get("token1"),
                reserve0,
                reserve1,
                pool.get("decimals0", 18),
                pool.get("decimals1", 18),
                trade_amount_usd,
                token_prices
            )
            results.append(result)

        return results

    def _calculate_v2_slippage(
        self,
        pool_address: str,
        token0: str,
        token1: str,
        reserve0: int,
        reserve1: int,
        decimals0: int,
        decimals1: int,
        trade_amount_usd: Decimal,
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> Dict[str, Any]:
        """Calculate V2 slippage from reserves."""
        try:
            # Convert reserves to decimal
            reserve0_decimal = Decimal(reserve0) / Decimal(10 ** decimals0)
            reserve1_decimal = Decimal(reserve1) / Decimal(10 ** decimals1)

            # Calculate liquidity and slippage if prices available
            liquidity_usd = None
            slippage = None

            if token_prices and token0 and token1:
                price0 = token_prices.get(token0.lower(), Decimal(0))
                price1 = token_prices.get(token1.lower(), Decimal(0))

                if price0 > 0 and price1 > 0:
                    # Total liquidity in USD
                    liquidity_usd = (reserve0_decimal * price0) + (reserve1_decimal * price1)

                    # Calculate trade amount in token0
                    trade_amount_token0 = trade_amount_usd / price0

                    if reserve0_decimal > 0:
                        # V2 AMM formula with 0.3% fee
                        dx = trade_amount_token0
                        x = reserve0_decimal
                        y = reserve1_decimal

                        # Amount out with fee
                        dy = (y * dx * Decimal("997")) / (x * Decimal("1000") + dx * Decimal("997"))

                        # Ideal amount out (no slippage)
                        ideal_dy = (y * dx) / x

                        # Calculate slippage
                        if ideal_dy > 0:
                            slippage = (ideal_dy - dy) / ideal_dy * Decimal(100)

            return {
                "pool_address": pool_address,
                "token0": token0,
                "token1": token1,
                "reserve0": float(reserve0_decimal),
                "reserve1": float(reserve1_decimal),
                "liquidity_usd": float(liquidity_usd) if liquidity_usd else None,
                "slippage_percent": float(slippage) if slippage else None,
                "trade_amount_usd": float(trade_amount_usd)
            }

        except Exception as e:
            logger.error(f"Failed to calculate V2 slippage: {e}")
            return {
                "pool_address": pool_address,
                "error": str(e)
            }


class UniswapV3LiquidityCalculator:
    """Calculate liquidity and slippage for Uniswap V3 pools."""

    def __init__(self, w3: Web3, chain_id: int):
        self.w3 = w3
        self.chain_id = chain_id
        self.batch_fetcher = BatchLiquidityFetcher(w3, chain_id)

    async def fetch_slot0_batch(self, pool_addresses: List[str]) -> Dict[str, Dict]:
        """Batch fetch slot0 data for V3 pools."""
        slot0_data = {}

        # ABI for slot0
        abi = [{
            "inputs": [],
            "name": "slot0",
            "outputs": [
                {"name": "sqrtPriceX96", "type": "uint160"},
                {"name": "tick", "type": "int24"},
                {"name": "observationIndex", "type": "uint16"},
                {"name": "observationCardinality", "type": "uint16"},
                {"name": "observationCardinalityNext", "type": "uint16"},
                {"name": "feeProtocol", "type": "uint8"},
                {"name": "unlocked", "type": "bool"}
            ],
            "stateMutability": "view",
            "type": "function"
        }]

        for addr in pool_addresses:
            try:
                addr = to_checksum_address(addr)
                contract = self.w3.eth.contract(address=addr, abi=abi)
                result = contract.functions.slot0().call()

                slot0_data[addr.lower()] = {
                    "sqrtPriceX96": result[0],
                    "tick": result[1],
                    "observationIndex": result[2]
                }
            except Exception as e:
                logger.error(f"Failed to get slot0 for {addr}: {e}")
                slot0_data[addr.lower()] = None

        return slot0_data

    async def fetch_liquidity_batch(self, pool_addresses: List[str]) -> Dict[str, int]:
        """Batch fetch liquidity for V3 pools."""
        liquidity_data = {}

        # ABI for liquidity
        abi = [{
            "inputs": [],
            "name": "liquidity",
            "outputs": [{"name": "", "type": "uint128"}],
            "stateMutability": "view",
            "type": "function"
        }]

        for addr in pool_addresses:
            try:
                addr = to_checksum_address(addr)
                contract = self.w3.eth.contract(address=addr, abi=abi)
                liquidity = contract.functions.liquidity().call()
                liquidity_data[addr.lower()] = liquidity
            except Exception as e:
                logger.error(f"Failed to get liquidity for {addr}: {e}")
                liquidity_data[addr.lower()] = 0

        return liquidity_data

    async def calculate_slippage_batch(
        self,
        pools_data: List[Dict[str, Any]],
        trade_amount_usd: Decimal = Decimal("1000"),
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate slippage for multiple V3 pools.

        Args:
            pools_data: List of pool data with address, token0, token1, fee
            trade_amount_usd: Trade size in USD
            token_prices: Token price mapping

        Returns:
            List of slippage calculation results
        """
        pool_addresses = [p["address"] for p in pools_data]

        # Batch fetch slot0 and liquidity
        slot0_data = await self.fetch_slot0_batch(pool_addresses)
        liquidity_data = await self.fetch_liquidity_batch(pool_addresses)

        results = []
        for pool in pools_data:
            addr = pool["address"].lower()

            if addr not in slot0_data or slot0_data[addr] is None:
                results.append({
                    "pool_address": pool["address"],
                    "error": "Failed to fetch slot0 data"
                })
                continue

            result = self._calculate_v3_slippage(
                pool["address"],
                pool.get("token0"),
                pool.get("token1"),
                slot0_data[addr],
                liquidity_data.get(addr, 0),
                pool.get("decimals0", 18),
                pool.get("decimals1", 18),
                pool.get("fee", 3000),
                trade_amount_usd,
                token_prices
            )
            results.append(result)

        return results

    def _calculate_v3_slippage(
        self,
        pool_address: str,
        token0: str,
        token1: str,
        slot0: Dict,
        liquidity: int,
        decimals0: int,
        decimals1: int,
        fee: int,
        trade_amount_usd: Decimal,
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> Dict[str, Any]:
        """Calculate V3 slippage from slot0 and liquidity."""
        try:
            sqrt_price_x96 = slot0["sqrtPriceX96"]
            current_tick = slot0["tick"]

            # Calculate current price
            sqrt_price = Decimal(sqrt_price_x96) / Decimal(2**96)
            price = sqrt_price ** 2 * Decimal(10 ** (decimals0 - decimals1))

            # Simplified slippage calculation
            liquidity_decimal = Decimal(liquidity)
            liquidity_usd = None
            slippage = None

            if token_prices and token0 and token1 and liquidity > 0:
                price0 = token_prices.get(token0.lower())
                price1 = token_prices.get(token1.lower())

                if price0 and price1:
                    # Estimate amounts at current price (simplified)
                    amount0 = liquidity_decimal / sqrt_price / Decimal(10**decimals0)
                    amount1 = liquidity_decimal * sqrt_price / Decimal(10**decimals1)

                    liquidity_usd = (amount0 * price0) + (amount1 * price1)

                    # Rough slippage estimate
                    if liquidity_usd > 0:
                        slippage = trade_amount_usd / (Decimal(2) * liquidity_usd) * Decimal(100)

            return {
                "pool_address": pool_address,
                "token0": token0,
                "token1": token1,
                "current_tick": current_tick,
                "sqrt_price_x96": sqrt_price_x96,
                "liquidity": liquidity,
                "liquidity_usd": float(liquidity_usd) if liquidity_usd else None,
                "slippage_percent": float(slippage) if slippage else None,
                "trade_amount_usd": float(trade_amount_usd),
                "fee_bps": fee / 100
            }

        except Exception as e:
            logger.error(f"Failed to calculate V3 slippage: {e}")
            return {
                "pool_address": pool_address,
                "error": str(e)
            }


class ProtocolLiquidityManager:
    """Manage liquidity calculations across protocols."""

    def __init__(self, w3: Web3, chain_id: int):
        self.w3 = w3
        self.chain_id = chain_id
        self.v2_calculator = UniswapV2LiquidityCalculator(w3, chain_id)
        self.v3_calculator = UniswapV3LiquidityCalculator(w3, chain_id)

    async def check_pools_liquidity_threshold(
        self,
        pools_data: List[Dict[str, Any]],
        max_slippage_percent: Decimal = Decimal("5"),
        trade_amount_usd: Decimal = Decimal("1000"),
        token_prices: Optional[Dict[str, Decimal]] = None
    ) -> List[Dict[str, Any]]:
        """
        Check multiple pools against liquidity threshold.

        Args:
            pools_data: List of pool data with protocol info
            max_slippage_percent: Maximum acceptable slippage
            trade_amount_usd: Trade size for slippage calc
            token_prices: Token prices for USD calculations

        Returns:
            List of results with pass/fail status
        """
        # Separate pools by protocol
        v2_pools = []
        v3_pools = []

        for pool in pools_data:
            protocol = pool.get("protocol", "").lower()
            if "v2" in protocol or "aerodrome" in protocol:
                v2_pools.append(pool)
            elif "v3" in protocol:
                v3_pools.append(pool)

        results = []

        # Process V2 pools
        if v2_pools:
            v2_results = await self.v2_calculator.calculate_slippage_batch(
                v2_pools, trade_amount_usd, token_prices
            )
            for result in v2_results:
                if "error" not in result and result.get("slippage_percent") is not None:
                    result["passes_threshold"] = result["slippage_percent"] <= float(max_slippage_percent)
                else:
                    result["passes_threshold"] = False
                results.append(result)

        # Process V3 pools
        if v3_pools:
            v3_results = await self.v3_calculator.calculate_slippage_batch(
                v3_pools, trade_amount_usd, token_prices
            )
            for result in v3_results:
                if "error" not in result and result.get("slippage_percent") is not None:
                    result["passes_threshold"] = result["slippage_percent"] <= float(max_slippage_percent)
                else:
                    result["passes_threshold"] = False
                results.append(result)

        return results