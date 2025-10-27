"""
Main orchestrator for the complete whitelist and pool filtering pipeline.

Pipeline stages:
1. Build token whitelist from multiple sources
2. Filter Stage 1 pools: whitelisted + trusted tokens
3. Calculate token prices from Stage 1
4. Filter Stage 2 pools: whitelisted + any token with trust path
5. Publish whitelist to Redis, NATS, and JSON storage
"""

import asyncio
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, UTC

# Add project root to Python path for direct script execution
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.core.storage.whitelist_publisher import WhitelistPublisher
from src.whitelist.builder import TokenWhitelistBuilder
from src.whitelist.pool_filter import PoolFilter, PoolInfo, TokenPrice
from src.whitelist.liquidity_filter import PoolLiquidityFilter
from web3 import Web3

logger = logging.getLogger(__name__)


class WhitelistOrchestrator:
    """Orchestrate the complete whitelist and pool filtering pipeline."""

    def __init__(
        self,
        storage: PostgresStorage,
        config: ConfigManager,
        output_dir: Optional[Path] = None
    ):
        """
        Initialize orchestrator.

        Args:
            storage: Database storage instance
            config: Configuration manager
            output_dir: Directory for output files (default: project_root/data)
        """
        self.storage = storage
        self.config = config
        self.output_dir = output_dir or Path(__file__).parent.parent.parent / "data"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger

    async def run_pipeline(
        self,
        chain: str = "ethereum",
        top_transfers: int = 100,
        stage1_liquidity: Decimal = Decimal("10000"),  # $10k
        stage2_liquidity: Decimal = Decimal("50000"),  # $50k
        protocols: List[str] = ["uniswap_v2", "uniswap_v3", "uniswap_v4"]
    ) -> Dict:
        """
        Run the complete pipeline.

        Args:
            chain: Blockchain identifier
            top_transfers: Number of top transferred tokens to include in whitelist
            stage1_liquidity: Liquidity threshold for Stage 1 (whitelisted + trusted)
            stage2_liquidity: Liquidity threshold for Stage 2 (whitelisted + any)
            protocols: DEX protocols to query

        Returns:
            Dictionary with complete pipeline results
        """
        self.logger.info("="*80)
        self.logger.info("DYNAMIC WHITELIST & POOL FILTERING PIPELINE")
        self.logger.info("="*80)

        # Step 1: Build whitelist
        self.logger.info("STEP 1: BUILD TOKEN WHITELIST")

        whitelist_builder = TokenWhitelistBuilder(self.storage)
        whitelist_result = await whitelist_builder.build_whitelist(
            top_transfers=top_transfers
        )

        # Extract whitelisted token addresses
        whitelisted_tokens = set(whitelist_result['tokens'])

        # Extract token metadata from whitelist
        token_info = whitelist_result.get('token_info', {})
        token_symbols = {addr: info.get('symbol', '') for addr, info in token_info.items() if 'symbol' in info}
        token_decimals = {addr: info.get('decimals', 18) for addr, info in token_info.items() if 'decimals' in info}

        # Get trusted tokens from config for the specified chain
        all_trusted_tokens = self.config.chains.get_trusted_tokens_for_chain()
        trusted_tokens = all_trusted_tokens.get(chain, {})
        trusted_token_addresses = set(trusted_tokens.keys())

        # Step 2: Query pools from database
        self.logger.info("STEP 2: QUERY POOLS FROM DATABASE")
        self.logger.info(f"Whitelisted tokens: {len(whitelisted_tokens)}")
        self.logger.info(f"Trusted tokens: {list(trusted_tokens.keys())}")
        self.logger.info(f"Liquidity threshold: ${stage1_liquidity:,}")

        # Query pools containing whitelisted or trusted tokens
        all_tokens = whitelisted_tokens | trusted_token_addresses

        # Get factory addresses for each protocol
        v2_factories = [f.lower() for f in self.config.protocols.get_factory_addresses("uniswap_v2", chain)]
        v3_factories = [f.lower() for f in self.config.protocols.get_factory_addresses("uniswap_v3", chain)]
        v4_factories = [f.lower() for f in self.config.protocols.get_factory_addresses("uniswap_v4", chain)]

        # Query pools from database - get ALL pools where BOTH tokens are whitelisted
        # This includes Stage 1 (whitelisted+trusted) and Stage 2 (whitelisted+whitelisted)
        all_tokens_for_query = whitelisted_tokens | trusted_token_addresses

        # Query pools with tick_spacing for V3/V4 pools
        query = """
        SELECT DISTINCT
            p.address,
            LOWER(p.asset0) as token0,
            LOWER(p.asset1) as token1,
            LOWER(p.factory) as factory,
            COALESCE(v3.tick_spacing, v4.tick_spacing) as tick_spacing
        FROM (
            SELECT address, asset0, asset1, factory FROM network_1__dex_pools
            UNION
            SELECT address, asset0, asset1, factory FROM network_1_dex_pools_cryo
        ) p
        LEFT JOIN network_1_uniswap_v3_pool_creation v3
            ON LOWER(p.address) = LOWER(v3.pool)
        LEFT JOIN network_1_uniswap_v4_pool_creation v4
            ON LOWER(p.address) = LOWER(v4.pool_id)
        WHERE (
            LOWER(p.asset0) = ANY($1) AND LOWER(p.asset1) = ANY($1)
        )
        """

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query, list(all_tokens_for_query))

        # Group pools by address and format
        pools = {}
        for row in results:
            pool_addr = row['address'].lower()
            factory = row['factory'].lower()
            token0 = row['token0']
            token1 = row['token1']
            tick_spacing = row['tick_spacing']

            # Identify protocol
            if factory in v2_factories:
                protocol = 'v2'
            elif factory in v3_factories:
                protocol = 'v3'
            elif factory in v4_factories:
                protocol = 'v4'
            else:
                continue  # Skip unknown protocols

            pools[pool_addr] = {
                'address': pool_addr,
                'token0': {'address': token0},
                'token1': {'address': token1},
                'factory': factory,
                'protocol': protocol,
                'tick_spacing': tick_spacing  # Include for V3/V4 (will be None for V2)
            }

        self.logger.info(f"✅ Found {len(pools)} pools")

        # Debug: Save V4 pool addresses being queried
        v4_pools_queried = [addr for addr, p in pools.items() if p.get('protocol') == 'v4']
        import json
        with open('v4_pools_queried.json', 'w') as f:
            json.dump({'count': len(v4_pools_queried), 'addresses': v4_pools_queried}, f, indent=2)
        self.logger.info(f"💾 DEBUG: Saved {len(v4_pools_queried)} V4 pool addresses to v4_pools_queried.json")

        # Step 3: Filter pools with comprehensive price discovery
        self.logger.info("STEP 3: FILTER POOLS WITH PRICE DISCOVERY")

        # Initialize Web3 and PoolLiquidityFilter
        rpc_url = self.config.chains.get_rpc_url(chain)
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Use protocol-specific thresholds from config
        liquidity_filter = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_v2_usd=self.config.chains.MIN_LIQUIDITY_V2,
            min_liquidity_v3_usd=self.config.chains.MIN_LIQUIDITY_V3,
            min_liquidity_v4_usd=self.config.chains.MIN_LIQUIDITY_V4,
            chain=chain
        )

        # Run the unified filtering with price discovery
        result = await liquidity_filter.filter_pools_with_price_discovery(
            storage=self.storage,
            all_tokens=all_tokens,
            token_symbols=token_symbols,
            token_decimals=token_decimals,
            v2_factories=v2_factories,
            v3_factories=v3_factories,
            v4_factories=v4_factories,
            pools=pools
        )

        filtered_pools_dict = result['filtered_pools']
        discovered_prices = result['discovered_prices']

        # Convert filtered pools to PoolInfo objects for compatibility
        filtered_pools = []
        for pool_addr, pool_data in filtered_pools_dict.items():
            pool_info = PoolInfo(
                pool_address=pool_addr,
                token0=pool_data['token0']['address'],
                token1=pool_data['token1']['address'],
                protocol=pool_data['protocol'],
                liquidity=pool_data.get('liquidity_usd'),
                fee=None,
                tick_spacing=None,
                factory=pool_data['factory'],
                block_number=None
            )
            filtered_pools.append(pool_info)

        # Convert discovered prices to TokenPrice objects for compatibility
        token_prices = {}
        for token_addr, price in discovered_prices.items():
            # Find a pool that contains this token for the pool_address field
            pool_addr = None
            for p_addr, p_data in filtered_pools_dict.items():
                if token_addr in [p_data['token0']['address'], p_data['token1']['address']]:
                    pool_addr = p_addr
                    break

            token_prices[token_addr] = TokenPrice(
                token_address=token_addr,
                price_in_trusted=price,
                trusted_token='USD',  # Prices are in USD
                pool_address=pool_addr or '',
                liquidity=Decimal("0")  # Not tracked individually
            )

        # For compatibility with existing code, treat all filtered pools as stage1
        stage1_pools = filtered_pools
        stage2_pools = []

        # Step 4: Prepare whitelist for publishing
        self.logger.info("STEP 4: PREPARE WHITELIST FOR PUBLISHING")

        # Format whitelist for publishing
        whitelist_for_publishing = []
        for token in whitelisted_tokens:
            token_data = {
                "address": token,
                "sources": whitelist_result.get('token_sources', {}).get(token, []),
                "info": whitelist_result.get('token_info', {}).get(token, {})
            }

            # Add price if available
            if token in token_prices:
                price_info = token_prices[token]
                token_data["price"] = {
                    "value": str(price_info.price_in_trusted),
                    "trusted_token": price_info.trusted_token,
                    "pool_address": price_info.pool_address,
                    "liquidity": str(price_info.liquidity)
                }

            whitelist_for_publishing.append(token_data)

        # Metadata for publishing
        publish_metadata = {
            "chain": chain,
            "token_count": len(whitelisted_tokens),
            "generated_at": datetime.now(UTC).isoformat(),
            "sources_breakdown": whitelist_result.get('breakdown', {}),
            "stage1_pools_count": len(stage1_pools),
            "stage2_pools_count": len(stage2_pools),
            "total_pools": len(stage1_pools) + len(stage2_pools),
            "config": {
                "top_transfers": top_transfers,
                "stage1_liquidity": str(stage1_liquidity),
                "stage2_liquidity": str(stage2_liquidity),
                "protocols": protocols
            }
        }

        # Step 5: Publish whitelist to Redis, NATS, and JSON
        self.logger.info("STEP 5: PUBLISH WHITELIST")

        async with WhitelistPublisher(self.config) as publisher:
            publish_results = await publisher.publish_whitelist(
                chain=chain,
                whitelist=whitelist_for_publishing,
                metadata=publish_metadata
            )

            self.logger.info(f"Publishing results: {publish_results}")

        # Step 6: Save detailed results locally
        self.logger.info("STEP 6: SAVE DETAILED RESULTS")

        # Save whitelist by stage for debugging
        whitelist_stages_path = self.output_dir / f"whitelist_by_stage_{chain}.json"
        with open(whitelist_stages_path, 'w') as f:
            json.dump({
                "metadata": {
                    "chain": chain,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "total_tokens": len(whitelisted_tokens)
                },
                "breakdown": whitelist_result.get('breakdown', {}),
                "cross_chain_tokens": [addr for addr, sources in whitelist_result.get('token_sources', {}).items() if 'cross_chain' in sources],
                "hyperliquid_tokens": [addr for addr, sources in whitelist_result.get('token_sources', {}).items() if 'hyperliquid' in sources],
                "lighter_tokens": [addr for addr, sources in whitelist_result.get('token_sources', {}).items() if 'lighter' in sources],
                "top_transferred_tokens": [addr for addr, sources in whitelist_result.get('token_sources', {}).items() if 'top_transferred' in sources],
                "unmapped_hyperliquid": whitelist_result.get('unmapped_hyperliquid', {}),
                "unmapped_lighter": whitelist_result.get('unmapped_lighter', {})
            }, f, indent=2)
        self.logger.info(f"💾 Saved whitelist by stage to {whitelist_stages_path}")

        results = {
            "whitelist": {
                "total_tokens": len(whitelisted_tokens),
                "tokens": sorted(list(whitelisted_tokens)),
                "sources": whitelist_result.get('breakdown', {}),
                "token_details": whitelist_for_publishing,
                "unmapped_hyperliquid": whitelist_result.get('unmapped_hyperliquid', {}),
                "unmapped_lighter": whitelist_result.get('unmapped_lighter', {})
            },
            "stage1_pools": {
                "count": len(stage1_pools),
                "liquidity_threshold": str(stage1_liquidity),
                "pools": [self._pool_to_dict(p) for p in stage1_pools]
            },
            "stage2_pools": {
                "count": len(stage2_pools),
                "liquidity_threshold": str(stage2_liquidity),
                "pools": [self._pool_to_dict(p) for p in stage2_pools]
            },
            "token_prices": {
                addr: {
                    "price_in_trusted": str(price.price_in_trusted),
                    "trusted_token": price.trusted_token,
                    "pool_address": price.pool_address,
                    "liquidity": str(price.liquidity)
                }
                for addr, price in token_prices.items()
            },
            "metadata": publish_metadata,
            "publishing": publish_results
        }

        # Save complete results
        results_path = self.output_dir / f"pipeline_results_{chain}.json"
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        self.logger.info(f"Saved complete results to {results_path}")

        # Save pools separately for easy access
        pools_path = self.output_dir / f"filtered_pools_{chain}.json"
        pools_data = {
            "metadata": {
                "chain": chain,
                "generated_at": datetime.now(UTC).isoformat(),
                "stage1_count": len(stage1_pools),
                "stage2_count": len(stage2_pools)
            },
            "stage1": [self._pool_to_dict(p) for p in stage1_pools],
            "stage2": [self._pool_to_dict(p) for p in stage2_pools]
        }
        with open(pools_path, 'w') as f:
            json.dump(pools_data, f, indent=2)
        self.logger.info(f"Saved filtered pools to {pools_path}")

        self.logger.info("="*80)
        self.logger.info("PIPELINE COMPLETE")
        self.logger.info("="*80)
        self.logger.info(f"Summary:")
        self.logger.info(f"  Whitelisted tokens: {len(whitelisted_tokens)}")
        self.logger.info(f"  Stage 1 pools: {len(stage1_pools)}")
        self.logger.info(f"  Stage 2 pools: {len(stage2_pools)}")
        self.logger.info(f"  Token prices calculated: {len(token_prices)}")
        self.logger.info(f"  Total arbitrage-ready pools: {len(stage1_pools) + len(stage2_pools)}")
        self.logger.info(f"  Published to: {', '.join([k for k, v in publish_results.items() if v])}")
        self.logger.info("="*80)

        return results

    def _pool_to_dict(self, pool: PoolInfo) -> Dict:
        """Convert PoolInfo to dictionary for JSON serialization."""
        return {
            "pool_address": pool.pool_address,
            "token0": pool.token0,
            "token1": pool.token1,
            "protocol": pool.protocol,
            "liquidity": str(pool.liquidity) if pool.liquidity else None,
            "fee": pool.fee,
            "tick_spacing": pool.tick_spacing,
            "factory": pool.factory,
            "block_number": pool.block_number
        }


async def main():
    """Main execution function."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Initialize config and storage
    config = ConfigManager()

    db_config = {
        'host': config.database.POSTGRES_HOST,
        'port': config.database.POSTGRES_PORT,
        'user': config.database.POSTGRES_USER,
        'password': config.database.POSTGRES_PASSWORD,
        'database': config.database.POSTGRES_DB,
        'pool_size': 10,
        'pool_timeout': 10
    }

    storage = PostgresStorage(config=db_config)
    await storage.connect()

    try:
        orchestrator = WhitelistOrchestrator(storage, config)

        # Run pipeline with configurable parameters
        await orchestrator.run_pipeline(
            chain="ethereum",
            top_transfers=100,
            stage1_liquidity=Decimal("10000"),   # $10k minimum for Stage 1
            stage2_liquidity=Decimal("50000"),   # $50k minimum for Stage 2
            protocols=["uniswap_v2", "uniswap_v3", "uniswap_v4"]
        )

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
