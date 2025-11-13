"""
Main orchestrator for the complete whitelist and pool filtering pipeline.

Pipeline stages:
1. Build token whitelist from multiple sources (CEX, cross-chain, top transfers)
2. Query pools from database (V2, V3, V4 pools with whitelisted/trusted tokens)
3. Filter pools with price discovery (fetch liquidity and prices via RPC/RETH)
4. Prepare whitelist for publishing (format token and pool data)
5. Publish whitelist (NATS, JSON, Redis if available)
6. Save detailed results (pipeline results and filtered pools to JSON)
"""

import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to Python path for direct script execution
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

from web3 import Web3


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that converts Decimal to string."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

from src.config import ConfigManager
from src.core.storage.postgres import PostgresStorage
from src.core.storage.token_whitelist_publisher import TokenWhitelistNatsPublisher
from src.core.storage.whitelist_publisher import WhitelistPublisher
from src.core.whitelist_manager import WhitelistManager
from src.whitelist.builder import TokenWhitelistBuilder
from src.whitelist.liquidity_filter import PoolLiquidityFilter
from src.whitelist.pool_types import PoolInfo, TokenPrice

logger = logging.getLogger(__name__)


class WhitelistOrchestrator:
    """Orchestrate the complete whitelist and pool filtering pipeline."""

    def __init__(
        self,
        storage: PostgresStorage,
        config: ConfigManager,
        output_dir: Optional[Path] = None,
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
        self, chain: str = "ethereum", top_transfers: int = 100
    ) -> Dict:
        """
        Run the complete pipeline.

        Args:
            chain: Blockchain identifier
            top_transfers: Number of top transferred tokens to include in whitelist

        Returns:
            Dictionary with complete pipeline results

        Note:
            Liquidity thresholds are configured per-protocol in ChainConfig:
            - MIN_LIQUIDITY_V2: V2 pool minimum liquidity (default: $2k)
            - MIN_LIQUIDITY_V3: V3 pool minimum liquidity (default: $2k)
            - MIN_LIQUIDITY_V4: V4 pool minimum liquidity (default: $1k)

            Full tick data collection and reference block publishing are handled by
            poolStateArena, not dynamicWhitelist. This pipeline only filters pools
            by liquidity using slot0 data and publishes the whitelist to NATS.
        """
        self.logger.info("=" * 80)
        self.logger.info("DYNAMIC WHITELIST & POOL FILTERING PIPELINE")
        self.logger.info("=" * 80)

        # Step 1: Build whitelist
        self.logger.info("STEP 1: BUILD TOKEN WHITELIST")

        whitelist_builder = TokenWhitelistBuilder(self.storage)
        whitelist_result = await whitelist_builder.build_whitelist(
            top_transfers=top_transfers
        )

        # Extract whitelisted token addresses
        whitelisted_tokens = set(whitelist_result["tokens"])

        # Extract token metadata from whitelist
        token_info = whitelist_result.get("token_info", {})
        token_symbols = {
            addr: info.get("symbol", "")
            for addr, info in token_info.items()
            if "symbol" in info
        }
        token_decimals = {
            addr: info.get("decimals", 18)
            for addr, info in token_info.items()
            if "decimals" in info
        }

        # Get trusted tokens from config for the specified chain
        all_trusted_tokens = self.config.chains.get_trusted_tokens_for_chain()
        trusted_tokens = all_trusted_tokens.get(chain, {})
        # Use values (addresses) not keys (symbols), and lowercase them for DB comparison
        trusted_token_addresses = set(addr.lower() for addr in trusted_tokens.values())

        # Step 2: Query pools from database
        self.logger.info("STEP 2: QUERY POOLS FROM DATABASE")
        self.logger.info(f"Whitelisted tokens: {len(whitelisted_tokens)}")
        self.logger.info(f"Trusted tokens: {list(trusted_tokens.keys())}")
        self.logger.info(
            f"Liquidity thresholds: V2=${self.config.chains.MIN_LIQUIDITY_V2:,.0f}, "
            f"V3=${self.config.chains.MIN_LIQUIDITY_V3:,.0f}, "
            f"V4=${self.config.chains.MIN_LIQUIDITY_V4:,.0f}"
        )

        # Query pools containing whitelisted or trusted tokens
        all_tokens = whitelisted_tokens | trusted_token_addresses

        # Get factory addresses for each protocol
        v2_factories = [
            f.lower()
            for f in self.config.protocols.get_factory_addresses("uniswap_v2", chain)
        ]
        v3_factories = [
            f.lower()
            for f in self.config.protocols.get_factory_addresses("uniswap_v3", chain)
        ]
        v4_factories = [
            f.lower()
            for f in self.config.protocols.get_factory_addresses("uniswap_v4", chain)
        ]

        # Query pools from database - get ALL pools where BOTH tokens are whitelisted
        # This includes Stage 1 (whitelisted+trusted) and Stage 2 (whitelisted+whitelisted)
        all_tokens_for_query = whitelisted_tokens | trusted_token_addresses

        # Query pools from network_1_dex_pools_cryo (includes tick_spacing and additional_data)
        query = """
        SELECT DISTINCT
            address,
            LOWER(asset0) as token0,
            LOWER(asset1) as token1,
            LOWER(factory) as factory,
            tick_spacing,
            additional_data
        FROM network_1_dex_pools_cryo
        WHERE (
            LOWER(asset0) = ANY($1) AND LOWER(asset1) = ANY($1)
        )
        """

        async with self.storage.pool.acquire() as conn:
            results = await conn.fetch(query, list(all_tokens_for_query))

        # Group pools by address and format
        pools = {}
        v4_pools_with_hooks_filtered = 0

        for row in results:
            pool_addr = row["address"].lower()
            factory = row["factory"].lower()
            token0 = row["token0"]
            token1 = row["token1"]
            tick_spacing = row["tick_spacing"]
            additional_data = row.get("additional_data")

            # Identify protocol
            if factory in v2_factories:
                protocol = "v2"
            elif factory in v3_factories:
                protocol = "v3"
            elif factory in v4_factories:
                protocol = "v4"

                # Filter out V4 pools with hooks (temporary - hooks not yet supported)
                if additional_data:
                    # Parse JSON if it's a string (asyncpg returns jsonb as string)
                    import json

                    if isinstance(additional_data, str):
                        try:
                            additional_data = json.loads(additional_data)
                        except json.JSONDecodeError:
                            pass  # If parsing fails, skip the filter

                    if isinstance(additional_data, dict):
                        hooks_address = additional_data.get("hooks_address", "").lower()
                        zero_address = "0x0000000000000000000000000000000000000000"

                        if hooks_address and hooks_address != zero_address:
                            v4_pools_with_hooks_filtered += 1
                            continue  # Skip V4 pools with hooks
            else:
                continue  # Skip unknown protocols

            # For V4, pool_addr is the pool_id, and factory is the pool manager
            if protocol == "v4":
                pools[pool_addr] = {
                    "address": factory,  # Pool manager address for V4
                    "pool_id": pool_addr,  # Actual pool identifier
                    "token0": {"address": token0},
                    "token1": {"address": token1},
                    "factory": factory,
                    "protocol": protocol,
                    "tick_spacing": tick_spacing,
                }
            else:
                pools[pool_addr] = {
                    "address": pool_addr,
                    "token0": {"address": token0},
                    "token1": {"address": token1},
                    "factory": factory,
                    "protocol": protocol,
                    "tick_spacing": tick_spacing,  # Include for V3/V4 (will be None for V2)
                }

        if v4_pools_with_hooks_filtered > 0:
            self.logger.info(
                f"⚠️  Filtered out {v4_pools_with_hooks_filtered} V4 pools with hooks"
            )

        self.logger.info(f"✅ Found {len(pools)} pools")

        # Step 3: Filter pools with comprehensive price discovery
        self.logger.info("STEP 3: FILTER POOLS WITH PRICE DISCOVERY")

        # Get Web3 instance for liquidity filtering
        rpc_url = self.config.chains.get_rpc_url(chain)
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Use protocol-specific thresholds from config
        liquidity_filter = PoolLiquidityFilter(
            web3=web3,
            min_liquidity_v2_usd=self.config.chains.MIN_LIQUIDITY_V2,
            min_liquidity_v3_usd=self.config.chains.MIN_LIQUIDITY_V3,
            min_liquidity_v4_usd=self.config.chains.MIN_LIQUIDITY_V4,
            chain=chain,
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
            pools=pools,
        )

        filtered_pools_dict = result["filtered_pools"]
        discovered_prices = result["discovered_prices"]

        # Use filtered_pools_dict directly instead of converting to PoolInfo
        # This preserves V4 pool_id and other important metadata
        filtered_pools = list(filtered_pools_dict.values())

        # Convert discovered prices to TokenPrice objects for compatibility
        token_prices = {}
        for token_addr, price in discovered_prices.items():
            # Find a pool that contains this token for the pool_address field
            pool_addr = None
            for p_addr, p_data in filtered_pools_dict.items():
                if token_addr in [
                    p_data["token0"]["address"],
                    p_data["token1"]["address"],
                ]:
                    pool_addr = p_addr
                    break

            token_prices[token_addr] = TokenPrice(
                token_address=token_addr,
                price_in_trusted=price,
                trusted_token="USD",  # Prices are in USD
                pool_address=pool_addr or "",
                liquidity=Decimal("0"),  # Not tracked individually
            )

        # Step 4: Prepare whitelist for publishing
        self.logger.info("STEP 4: PREPARE WHITELIST FOR PUBLISHING")

        # Format whitelist for publishing
        whitelist_for_publishing = []
        for token in whitelisted_tokens:
            token_data = {
                "address": token,
                "sources": whitelist_result.get("token_sources", {}).get(token, []),
                "info": whitelist_result.get("token_info", {}).get(token, {}),
            }

            # Add price if available
            if token in token_prices:
                price_info = token_prices[token]
                token_data["price"] = {
                    "value": str(price_info.price_in_trusted),
                    "trusted_token": price_info.trusted_token,
                    "pool_address": price_info.pool_address,
                    "liquidity": str(price_info.liquidity),
                }

            whitelist_for_publishing.append(token_data)

        # Metadata for publishing
        publish_metadata = {
            "chain": chain,
            "token_count": len(whitelisted_tokens),
            "generated_at": datetime.now(UTC).isoformat(),
            "sources_breakdown": whitelist_result.get("breakdown", {}),
            "total_pools": len(filtered_pools),
            "config": {
                "top_transfers": top_transfers,
                "min_liquidity_v2_usd": float(self.config.chains.MIN_LIQUIDITY_V2),
                "min_liquidity_v3_usd": float(self.config.chains.MIN_LIQUIDITY_V3),
                "min_liquidity_v4_usd": float(self.config.chains.MIN_LIQUIDITY_V4),
            },
        }

        # Step 5: Publish whitelist to Redis, NATS, and JSON
        self.logger.info("STEP 5: PUBLISH WHITELIST")

        async with WhitelistPublisher(self.config) as publisher:
            publish_results = await publisher.publish_whitelist(
                chain=chain,
                whitelist=whitelist_for_publishing,
                metadata=publish_metadata,
            )

            self.logger.info(f"Token whitelist publishing results: {publish_results}")

        # Step 5b: Publish pool whitelist to NATS (for ExEx and poolStateArena)
        # Using WhitelistManager for differential updates
        self.logger.info("STEP 5b: PUBLISH POOL WHITELIST TO NATS (DIFFERENTIAL)")

        # Prepare pools with full metadata for NATS publishing
        if filtered_pools:
            pools_for_nats = []
            skipped_pools = 0

            for pool_data in filtered_pools:
                # Get token addresses
                token0_addr = pool_data["token0"]["address"]
                token1_addr = pool_data["token1"]["address"]

                # Get token info - MUST have decimals and symbol
                token0_info = token_info.get(token0_addr, {})
                token1_info = token_info.get(token1_addr, {})

                # Skip pools with missing token metadata (decimals or symbol)
                if not token0_info.get("decimals") or not token0_info.get("symbol"):
                    pool_id = pool_data.get("pool_id", pool_data.get("address"))
                    self.logger.warning(
                        f"Skipping pool {pool_id}: missing token0 metadata "
                        f"(token: {token0_addr})"
                    )
                    skipped_pools += 1
                    continue

                if not token1_info.get("decimals") or not token1_info.get("symbol"):
                    pool_id = pool_data.get("pool_id", pool_data.get("address"))
                    self.logger.warning(
                        f"Skipping pool {pool_id}: missing token1 metadata "
                        f"(token: {token1_addr})"
                    )
                    skipped_pools += 1
                    continue

                # Build pool dict with proper structure for V2/V3/V4
                pool_dict = {
                    "address": pool_data["address"],
                    "token0": {
                        "address": token0_addr,
                        "decimals": token0_info["decimals"],
                        "symbol": token0_info["symbol"],
                        "name": token0_info.get("name", ""),
                    },
                    "token1": {
                        "address": token1_addr,
                        "decimals": token1_info["decimals"],
                        "symbol": token1_info["symbol"],
                        "name": token1_info.get("name", ""),
                    },
                    "protocol": pool_data["protocol"],
                    "factory": pool_data["factory"],
                }

                # Add V4-specific pool_id field (32-byte identifier)
                if "pool_id" in pool_data:
                    pool_dict["pool_id"] = pool_data["pool_id"]

                # Add protocol-specific required fields for V3/V4
                # V2 pools don't have fee/tick_spacing
                if pool_data["protocol"] in ["v3", "v4"]:
                    # Fee is required for V3/V4 (needed for swap calculations)
                    if "fee" not in pool_data or pool_data["fee"] is None:
                        pool_id = pool_data.get("pool_id", pool_data.get("address"))
                        self.logger.warning(
                            f"Skipping {pool_data['protocol']} pool {pool_id}: "
                            f"missing fee"
                        )
                        skipped_pools += 1
                        continue

                    # tick_spacing is required for V3/V4 (needed for tick validation)
                    if "tick_spacing" not in pool_data or pool_data[
                        "tick_spacing"
                    ] is None:
                        pool_id = pool_data.get("pool_id", pool_data.get("address"))
                        self.logger.warning(
                            f"Skipping {pool_data['protocol']} pool {pool_id}: "
                            f"missing tick_spacing"
                        )
                        skipped_pools += 1
                        continue

                    pool_dict["fee"] = pool_data["fee"]
                    pool_dict["tick_spacing"] = pool_data["tick_spacing"]

                pools_for_nats.append(pool_dict)

            if skipped_pools > 0:
                self.logger.warning(
                    f"Skipped {skipped_pools} pools due to missing token metadata"
                )

            # Publish to NATS using WhitelistManager (differential updates)
            if pools_for_nats:
                try:
                    # Prepare database config for WhitelistManager
                    db_config = {
                        "host": self.config.database.POSTGRES_HOST,
                        "port": self.config.database.POSTGRES_PORT,
                        "user": self.config.database.POSTGRES_USER,
                        "password": self.config.database.POSTGRES_PASSWORD,
                        "database": self.config.database.POSTGRES_DB,
                    }

                    # Use WhitelistManager for differential updates
                    async with WhitelistManager(db_config) as wl_manager:
                        # Publish differential update (Add/Remove/Full)
                        update_result = await wl_manager.publish_differential_update(
                            chain=chain, new_pools=pools_for_nats
                        )

                        self.logger.info(
                            f"📊 Whitelist differential update published: "
                            f"{update_result['update_type']} - "
                            f"+{update_result['added']} added, "
                            f"-{update_result['removed']} removed, "
                            f"total {update_result['total_pools']} pools "
                            f"(snapshot {update_result['snapshot_id']})"
                        )

                        publish_results.update(
                            {
                                "nats_pools_minimal": update_result["published"],
                                "nats_pools_full": update_result["published"],
                                "nats_pools_count": update_result["total_pools"],
                                "nats_pools_added": update_result["added"],
                                "nats_pools_removed": update_result["removed"],
                                "nats_update_type": update_result["update_type"],
                                "nats_snapshot_id": update_result["snapshot_id"],
                            }
                        )
                except Exception as e:
                    self.logger.error(
                        f"Failed to publish pools to NATS: {e}", exc_info=True
                    )
                    publish_results.update(
                        {
                            "nats_pools_minimal": False,
                            "nats_pools_full": False,
                            "nats_pools_count": 0,
                            "nats_pools_added": 0,
                            "nats_pools_removed": 0,
                            "nats_update_type": "error",
                        }
                    )
            else:
                self.logger.warning(
                    "No pools with complete metadata to publish to NATS"
                )
                publish_results.update(
                    {
                        "nats_pools_minimal": False,
                        "nats_pools_full": False,
                        "nats_pools_count": 0,
                        "nats_pools_added": 0,
                        "nats_pools_removed": 0,
                        "nats_update_type": "skipped",
                    }
                )
        else:
            self.logger.warning("No pools to publish to NATS")

        # Step 5c: Publish token whitelist to NATS (for dynamic token tracking)
        self.logger.info("STEP 5c: PUBLISH TOKEN WHITELIST TO NATS")

        # Prepare tokens with metadata for NATS publishing
        if whitelisted_tokens:
            tokens_for_nats = {}
            skipped_tokens = 0

            for token in whitelisted_tokens:
                # Get token info - MUST have decimals and symbol
                token_metadata = token_info.get(token, {})

                # Skip tokens with missing required metadata
                if not token_metadata.get("decimals") or not token_metadata.get(
                    "symbol"
                ):
                    self.logger.warning(
                        f"Skipping token {token}: missing decimals or symbol"
                    )
                    skipped_tokens += 1
                    continue

                # Get sources/filters for this token
                token_filters = whitelist_result.get("token_sources", {}).get(token, [])

                tokens_for_nats[token] = {
                    "symbol": token_metadata["symbol"],
                    "decimals": token_metadata["decimals"],
                    "name": token_metadata.get("name", ""),
                    "filters": token_filters,
                }

            if skipped_tokens > 0:
                self.logger.warning(
                    f"Skipped {skipped_tokens} tokens due to missing metadata"
                )

            # Publish to NATS (full + delta topics)
            if tokens_for_nats:
                try:
                    async with TokenWhitelistNatsPublisher() as token_publisher:
                        token_publish_results = (
                            await token_publisher.publish_token_whitelist(
                                chain=chain, tokens=tokens_for_nats
                            )
                        )
                        self.logger.info(
                            f"Token whitelist NATS publishing results: {token_publish_results}"
                        )
                        publish_results.update(
                            {
                                "nats_tokens_full": token_publish_results.get(
                                    "full", False
                                ),
                                "nats_tokens_add": token_publish_results.get(
                                    "add", False
                                ),
                                "nats_tokens_remove": token_publish_results.get(
                                    "remove", False
                                ),
                                "nats_tokens_count": len(tokens_for_nats),
                            }
                        )
                except Exception as e:
                    self.logger.error(
                        f"Failed to publish tokens to NATS: {e}", exc_info=True
                    )
                    publish_results.update(
                        {
                            "nats_tokens_full": False,
                            "nats_tokens_add": False,
                            "nats_tokens_remove": False,
                            "nats_tokens_count": 0,
                        }
                    )
            else:
                self.logger.warning(
                    "No tokens with complete metadata to publish to NATS"
                )
                publish_results.update(
                    {
                        "nats_tokens_full": False,
                        "nats_tokens_add": False,
                        "nats_tokens_remove": False,
                        "nats_tokens_count": 0,
                    }
                )
        else:
            self.logger.warning("No tokens to publish to NATS")

        # Step 6: Save detailed results locally
        self.logger.info("STEP 6: SAVE DETAILED RESULTS")

        # Save whitelist by stage for debugging
        whitelist_stages_path = self.output_dir / f"whitelist_by_stage_{chain}.json"
        with open(whitelist_stages_path, "w") as f:
            json.dump(
                {
                    "metadata": {
                        "chain": chain,
                        "generated_at": datetime.now(UTC).isoformat(),
                        "total_tokens": len(whitelisted_tokens),
                    },
                    "breakdown": whitelist_result.get("breakdown", {}),
                    "cross_chain_tokens": [
                        addr
                        for addr, sources in whitelist_result.get(
                            "token_sources", {}
                        ).items()
                        if "cross_chain" in sources
                    ],
                    "hyperliquid_tokens": [
                        addr
                        for addr, sources in whitelist_result.get(
                            "token_sources", {}
                        ).items()
                        if "hyperliquid" in sources
                    ],
                    "lighter_tokens": [
                        addr
                        for addr, sources in whitelist_result.get(
                            "token_sources", {}
                        ).items()
                        if "lighter" in sources
                    ],
                    "top_transferred_tokens": [
                        addr
                        for addr, sources in whitelist_result.get(
                            "token_sources", {}
                        ).items()
                        if "top_transferred" in sources
                    ],
                    "unmapped_hyperliquid": whitelist_result.get(
                        "unmapped_hyperliquid", {}
                    ),
                    "unmapped_lighter": whitelist_result.get("unmapped_lighter", {}),
                },
                f,
                indent=2,
                cls=DecimalEncoder,
            )
        self.logger.info(f"💾 Saved whitelist by stage to {whitelist_stages_path}")

        results = {
            "whitelist": {
                "total_tokens": len(whitelisted_tokens),
                "tokens": sorted(list(whitelisted_tokens)),
                "sources": whitelist_result.get("breakdown", {}),
                "token_details": whitelist_for_publishing,
                "unmapped_hyperliquid": whitelist_result.get(
                    "unmapped_hyperliquid", {}
                ),
                "unmapped_lighter": whitelist_result.get("unmapped_lighter", {}),
            },
            "pools": {
                "count": len(filtered_pools),
                "description": "Pools containing whitelisted or trusted tokens with sufficient liquidity",
                "pools": filtered_pools,
            },
            "token_prices": {
                addr: {
                    "price_in_trusted": str(price.price_in_trusted),
                    "trusted_token": price.trusted_token,
                    "pool_address": price.pool_address,
                    "liquidity": str(price.liquidity),
                }
                for addr, price in token_prices.items()
            },
            "metadata": publish_metadata,
            "publishing": publish_results,
        }

        # Save complete results
        results_path = self.output_dir / f"pipeline_results_{chain}.json"
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2, cls=DecimalEncoder)
        self.logger.info(f"Saved complete results to {results_path}")

        # Save pools separately for easy access
        pools_path = self.output_dir / f"filtered_pools_{chain}.json"
        pools_data = {
            "metadata": {
                "chain": chain,
                "generated_at": datetime.now(UTC).isoformat(),
                "pool_count": len(filtered_pools),
            },
            "pools": filtered_pools,
        }
        with open(pools_path, "w") as f:
            json.dump(pools_data, f, indent=2, cls=DecimalEncoder)
        self.logger.info(f"Saved filtered pools to {pools_path}")

        self.logger.info("=" * 80)
        self.logger.info("PIPELINE COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"Summary:")
        self.logger.info(f"  Whitelisted tokens: {len(whitelisted_tokens)}")
        self.logger.info(f"  Filtered pools: {len(filtered_pools)}")
        self.logger.info(f"  Token prices calculated: {len(token_prices)}")
        self.logger.info(
            f"  Published to: {', '.join([k for k, v in publish_results.items() if v])}"
        )
        self.logger.info("=" * 80)

        return results


async def main():
    """Main execution function."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize config and storage
    config = ConfigManager()

    db_config = {
        "host": config.database.POSTGRES_HOST,
        "port": config.database.POSTGRES_PORT,
        "user": config.database.POSTGRES_USER,
        "password": config.database.POSTGRES_PASSWORD,
        "database": config.database.POSTGRES_DB,
        "pool_size": 10,
        "pool_timeout": 10,
    }

    storage = PostgresStorage(config=db_config)
    await storage.connect()

    try:
        orchestrator = WhitelistOrchestrator(storage, config)

        # Run pipeline with configurable parameters
        # Liquidity thresholds are configured in ChainConfig (MIN_LIQUIDITY_V2/V3/V4)
        await orchestrator.run_pipeline(chain="ethereum", top_transfers=100)

    finally:
        await storage.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
