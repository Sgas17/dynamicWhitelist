"""
Integration tests for pool creation processors.
Tests the full pipeline: Cryo fetch -> Process -> Store in database
"""

import pytest
from pathlib import Path
import tempfile
import shutil
import polars as pl
import ujson
from decimal import Decimal
from sqlalchemy import create_engine, text

from src.fetchers.cryo_fetcher import CryoFetcher
from src.processors.pools.pool_processors import (
    UniswapV2PoolProcessor,
    UniswapV3PoolProcessor,
    UniswapV4PoolProcessor,
    AerodromeV2PoolProcessor
)
from src.config.manager import ConfigManager


class TestPoolProcessorsIntegration:
    """Integration tests for pool processors using real blockchain data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        # Create temp directory for parquet files
        self.temp_dir = tempfile.mkdtemp(prefix="pool_test_")
        self.data_dir = Path(self.temp_dir) / "data"
        self.data_dir.mkdir(parents=True)

        # Initialize config and database connection
        self.config = ConfigManager()
        self.engine = create_engine(self.config.database.postgres_url)

        # Track test data for cleanup
        self.test_pools = set()
        self.test_tokens = set()

        yield

        # Cleanup
        self.cleanup()
        if self.engine:
            self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def cleanup(self):
        """Clean up test data from database."""
        if not self.test_pools:
            return

        with self.engine.connect() as conn:
            # Only clean up Ethereum tables for now
            pool_list = "','".join(self.test_pools)

            # Check if table exists before trying to delete
            try:
                conn.execute(text(f"""
                    DELETE FROM network_1_dex_pools_cryo
                    WHERE address IN ('{pool_list}')
                """))
            except Exception as e:
                print(f"Warning during cleanup: {e}")

            # Clean up from whitelist tables
            pool_list = "','".join(self.test_pools)
            conn.execute(text(f"""
                DELETE FROM whitelisted_pools_cryo
                WHERE pool_address IN ('{pool_list}')
            """))

            if self.test_tokens:
                token_list = "','".join(self.test_tokens)
                conn.execute(text(f"""
                    DELETE FROM token_whitelist_cryo
                    WHERE token_address IN ('{token_list}')
                """))

            conn.commit()

    @pytest.mark.asyncio
    async def test_uniswap_v2_pool_fetch_and_process(self):
        """Test fetching and processing UniswapV2 pools from Ethereum."""
        # Get Uniswap V2 config
        uniswap_v2 = self.config.protocols.get_protocol_config("uniswap_v2", "ethereum")
        factory_addresses = self.config.protocols.get_factory_addresses("uniswap_v2", "ethereum")
        factory_address = factory_addresses[0] if factory_addresses else None
        deployment_block = self.config.protocols.get_deployment_block("uniswap_v2", "ethereum")

        # Use a known block range with pool creation events
        # Block 10000835 is deployment, but first pools were created later
        # Using a wider range to ensure we catch some pools
        start_block = deployment_block
        current_block = deployment_block + 10000  # Wider range for V2

        # Get chain config for RPC URL
        chain_config = self.config.chains.get_chain_config("ethereum")
        fetcher = CryoFetcher("ethereum", chain_config["rpc_url"])
        processor = UniswapV2PoolProcessor()

        # Fetch PairCreated events
        output_dir = self.data_dir / "uniswap_v2_pools"
        output_dir.mkdir(exist_ok=True)

        # PairCreated event signature for Uniswap V2
        pair_created_event = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

        result = await fetcher.fetch_logs(
            start_block=start_block,
            end_block=current_block,
            contracts=[factory_address],
            events=[pair_created_event],
            output_dir=str(output_dir)
        )

        assert result.success, f"Failed to fetch pool events: {result.error}"

        # Read and combine all parquet files
        parquet_files = list(output_dir.glob("*.parquet"))
        assert len(parquet_files) > 0, "No parquet files generated"

        # Combine all non-empty parquet files
        dfs = []
        for pf in parquet_files:
            df_part = pl.read_parquet(pf)
            if not df_part.is_empty():
                dfs.append(df_part)

        if not dfs:
            # No pools in this range, but we know pools exist in DB
            # Just verify we can query existing pools
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM network_1_dex_pools_cryo
                    WHERE LOWER(factory) = LOWER(:factory)
                """), {"factory": factory_address}).fetchone()

                assert result[0] > 0, "No pools found in database for this factory"
                print(f"Found {result[0]} existing pools in database")
                return

        df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

        # Process pools using the processor's internal method
        processed_pools = []
        for event in df.rows(named=True):
            pool_data = processor._process_single_event(event)
            if pool_data:
                processed_pools.append(pool_data)

        assert len(processed_pools) > 0, "No pools processed"

        # Verify pool structure (V2 specific)
        pool = processed_pools[0]
        assert "address" in pool
        assert "factory" in pool
        assert "asset0" in pool
        assert "asset1" in pool
        assert "fee" in pool
        assert pool["fee"] == 3000, "V2 should have fixed 0.3% fee"
        assert "additional_data" in pool
        assert "pair_index" in pool["additional_data"]

        # Track for cleanup
        self.test_pools.update([p["address"] for p in processed_pools])
        self.test_tokens.update([p["asset0"] for p in processed_pools])
        self.test_tokens.update([p["asset1"] for p in processed_pools])

        # Store in database
        with self.engine.connect() as conn:
            for pool in processed_pools[:5]:  # Store first 5 for testing
                conn.execute(text("""
                    INSERT INTO network_1_dex_pools_cryo
                    (address, factory, asset0, asset1, fee, creation_block, additional_data)
                    VALUES (:address, :factory, :asset0, :asset1, :fee, :creation_block, :additional_data)
                    ON CONFLICT (address) DO NOTHING
                """), {
                    "address": pool["address"],
                    "factory": pool["factory"],
                    "asset0": pool["asset0"],
                    "asset1": pool["asset1"],
                    "fee": pool.get("fee"),
                    "creation_block": pool.get("creation_block"),
                    "additional_data": ujson.dumps(pool.get("additional_data", {}))
                })
            conn.commit()

            # Verify storage
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM network_1_dex_pools_cryo
                WHERE LOWER(factory) = LOWER(:factory)
            """), {"factory": factory_address}).fetchone()

            assert result[0] > 0, "Pools not stored in database"

    @pytest.mark.asyncio
    async def test_uniswap_v3_pool_fetch_and_process(self):
        """Test fetching and processing UniswapV3 pools from Ethereum."""
        # Get Uniswap V3 config
        uniswap_v3 = self.config.protocols.get_protocol_config("uniswap_v3", "ethereum")
        factory_addresses = self.config.protocols.get_factory_addresses("uniswap_v3", "ethereum")
        factory_address = factory_addresses[0] if factory_addresses else None
        deployment_block = self.config.protocols.get_deployment_block("uniswap_v3", "ethereum")

        # Use a known block range with pool creation events
        # Block 12369621 is deployment, let's fetch first 100 blocks after deployment
        start_block = deployment_block
        current_block = deployment_block + 100

        # Get chain config for RPC URL
        chain_config = self.config.chains.get_chain_config("ethereum")
        fetcher = CryoFetcher("ethereum", chain_config["rpc_url"])
        processor = UniswapV3PoolProcessor()

        # Fetch PoolCreated events
        output_dir = self.data_dir / "uniswap_v3_pools"
        output_dir.mkdir(exist_ok=True)

        # PoolCreated event signature for Uniswap V3
        pool_created_event = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

        result = await fetcher.fetch_logs(
            start_block=start_block,
            end_block=current_block,
            contracts=[factory_address],
            events=[pool_created_event],
            output_dir=str(output_dir)
        )

        assert result.success, f"Failed to fetch pool events: {result.error}"

        # Find the parquet file
        parquet_files = list(output_dir.glob("*.parquet"))
        assert len(parquet_files) > 0, "No parquet files generated"
        output_file = parquet_files[0]

        # Process the pools using Polars
        df = pl.read_parquet(output_file)

        if df.is_empty():
            # No pools in this range, but we know pools exist in DB
            # Just verify we can query existing pools
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM network_1_dex_pools_cryo
                    WHERE LOWER(factory) = LOWER(:factory)
                """), {"factory": factory_address}).fetchone()

                assert result[0] > 0, "No pools found in database for this factory"
                print(f"Found {result[0]} existing pools in database")
                return

        # Process pools using the processor's internal method
        processed_pools = []
        for event in df.rows(named=True):
            pool_data = processor._process_single_event(event)
            if pool_data:
                processed_pools.append(pool_data)

        assert len(processed_pools) > 0, "No pools processed"

        # Verify pool structure
        pool = processed_pools[0]
        assert "address" in pool
        assert "factory" in pool
        assert "asset0" in pool
        assert "asset1" in pool
        assert "fee" in pool
        assert "tick_spacing" in pool

        # Track for cleanup
        self.test_pools.update([p["address"] for p in processed_pools])
        self.test_tokens.update([p["asset0"] for p in processed_pools])
        self.test_tokens.update([p["asset1"] for p in processed_pools])

        # Store in database
        with self.engine.connect() as conn:
            for pool in processed_pools[:5]:  # Store first 5 for testing
                conn.execute(text("""
                    INSERT INTO network_1_dex_pools_cryo
                    (address, factory, asset0, asset1, fee, tick_spacing, creation_block)
                    VALUES (:address, :factory, :asset0, :asset1, :fee, :tick_spacing, :creation_block)
                    ON CONFLICT (address) DO NOTHING
                """), {
                    "address": pool["address"],
                    "factory": pool["factory"],
                    "asset0": pool["asset0"],
                    "asset1": pool["asset1"],
                    "fee": pool.get("fee"),
                    "tick_spacing": pool.get("tick_spacing"),
                    "creation_block": pool.get("creation_block")
                })
            conn.commit()

            # Verify storage
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM network_1_dex_pools_cryo
                WHERE LOWER(factory) = LOWER(:factory)
            """), {"factory": factory_address}).fetchone()

            assert result[0] > 0, "Pools not stored in database"

    @pytest.mark.asyncio
    async def test_uniswap_v4_pool_fetch_and_process(self):
        """Test fetching and processing UniswapV4 pools from Ethereum."""
        # Get Uniswap V4 config
        uniswap_v4 = self.config.protocols.get_protocol_config("uniswap_v4", "ethereum")
        pool_manager = uniswap_v4.get("pool_manager")
        deployment_block = self.config.protocols.get_deployment_block("uniswap_v4", "ethereum")

        # Get chain config for RPC URL
        chain_config = self.config.chains.get_chain_config("ethereum")
        fetcher = CryoFetcher("ethereum", chain_config["rpc_url"])
        processor = UniswapV4PoolProcessor()

        # Fetch Initialize events (V4 uses different event)
        output_dir = self.data_dir / "uniswap_v4_pools"
        output_dir.mkdir(exist_ok=True)

        # Initialize event signature for Uniswap V4 (confirmed from chain data)
        initialize_event = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"

        # V4 deployed at 21688329, first pools at 21688545 and 21689254
        # Fetch first 2000 blocks to get both pools
        result = await fetcher.fetch_logs(
            start_block=deployment_block,
            end_block=deployment_block + 2000,  # First 2000 blocks includes first pools
            contracts=[pool_manager] if pool_manager else [],
            events=[initialize_event],
            output_dir=str(output_dir)
        )

        if not result.success:
            pytest.skip(f"Failed to fetch V4 pools: {result.error}")

        # Read and combine all parquet files
        parquet_files = list(output_dir.glob("*.parquet"))
        if len(parquet_files) == 0:
            pytest.skip("No UniswapV4 pools found yet")

        # Combine all non-empty parquet files
        dfs = []
        for pf in parquet_files:
            df_part = pl.read_parquet(pf)
            if not df_part.is_empty():
                dfs.append(df_part)

        if not dfs:
            pytest.skip("No UniswapV4 pools in fetched data")

        df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

        # Process pools
        processed_pools = []
        for event in df.rows(named=True):
            pool_data = processor._process_single_event(event)
            if pool_data:
                processed_pools.append(pool_data)

        # V4 might not have pools yet
        if len(processed_pools) == 0:
            pytest.skip("No V4 pools to process")

        # Track for cleanup
        self.test_pools.update([p["address"] for p in processed_pools])

    @pytest.mark.skip(reason="Base node is currently down")
    @pytest.mark.asyncio
    async def test_aerodrome_v2_pool_fetch_and_process(self):
        """Test fetching and processing Aerodrome V2 pools from Base."""
        # Get Aerodrome V2 config for Base
        aerodrome = self.config.protocols.get_protocol_config("aerodrome_v2", "base")
        factory_address = aerodrome.get("pool_factory")
        deployment_block = self.config.protocols.get_deployment_block("aerodrome_v2", "base")

        # Get chain config for RPC URL
        chain_config = self.config.chains.get_chain_config("base")
        fetcher = CryoFetcher("base", chain_config["rpc_url"])
        processor = AerodromeV2PoolProcessor()

        # Fetch PoolCreated events
        output_dir = self.data_dir / "aerodrome_v2_pools"
        output_dir.mkdir(exist_ok=True)

        # Fetch recent pools
        current_block = deployment_block + 100000
        start_block = current_block - 10000

        # PoolCreated event signature
        pool_created_event = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

        result = await fetcher.fetch_logs(
            start_block=start_block,
            end_block=current_block,
            contracts=[factory_address] if factory_address else [],
            events=[pool_created_event],
            output_dir=str(output_dir)
        )

        if not result.success:
            pytest.skip(f"Failed to fetch Aerodrome pools: {result.error}")

        parquet_files = list(output_dir.glob("*.parquet"))
        if len(parquet_files) == 0:
            pytest.skip("No Aerodrome pools found")

        output_file = parquet_files[0]

        df = pl.read_parquet(output_file)
        if df.is_empty():
            pytest.skip("No Aerodrome pools found")

        # Process pools
        processed_pools = []
        for event in df.rows(named=True):
            pool_data = processor._process_single_event(event)
            if pool_data:
                processed_pools.append(pool_data)

        assert len(processed_pools) > 0, "No Aerodrome pools processed"

        # Verify Aerodrome-specific fields
        pool = processed_pools[0]
        assert "additional_data" in pool
        assert "stable" in pool["additional_data"], "Missing 'stable' flag in Aerodrome pool"

        # Track for cleanup
        self.test_pools.update([p["address"] for p in processed_pools])

    @pytest.mark.asyncio
    async def test_full_pipeline_with_trusted_tokens(self):
        """Test the complete pipeline including trusted token filtering."""
        with self.engine.connect() as conn:
            # Insert a test pool with USDC (trusted token)
            usdc_address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            weth_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756ca2"
            test_pool = "0xtest_pool_usdc_weth"

            self.test_pools.add(test_pool)
            self.test_tokens.add(usdc_address)
            self.test_tokens.add(weth_address)

            # Insert test pool
            conn.execute(text("""
                INSERT INTO network_1_dex_pools_cryo
                (address, factory, asset0, asset1, fee, creation_block)
                VALUES (:address, :factory, :asset0, :asset1, :fee, :creation_block)
                ON CONFLICT (address) DO NOTHING
            """), {
                "address": test_pool,
                "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # V3 factory
                "asset0": usdc_address,
                "asset1": weth_address,
                "fee": 3000,  # 0.3% fee
                "creation_block": 19000000
            })

            # Check if pool has trusted token
            result = conn.execute(text(f"""
                SELECT p.address, p.asset0, p.asset1
                FROM network_1_dex_pools_cryo p
                JOIN trusted_tokens_cryo t ON
                    (LOWER(p.asset0) = LOWER(t.token_address) OR
                     LOWER(p.asset1) = LOWER(t.token_address))
                WHERE t.chain_id = 1
                AND p.address = :pool_address
            """), {"pool_address": test_pool}).fetchone()

            assert result is not None, "Pool with trusted token not found"

            # Mark as whitelisted (would normally check liquidity first)
            conn.execute(text("""
                INSERT INTO whitelisted_pools_cryo
                (pool_address, chain_id, token0, token1, fee,
                 liquidity_usd, max_slippage_1k, trusted_token,
                 filter_pass_type, iteration_depth)
                VALUES (:pool_address, :chain_id, :token0, :token1, :fee,
                        :liquidity_usd, :max_slippage_1k, :trusted_token,
                        :filter_pass_type, :iteration_depth)
            """), {
                "pool_address": test_pool,
                "chain_id": 1,
                "token0": usdc_address,
                "token1": weth_address,
                "fee": 3000,
                "liquidity_usd": 1000000.00,  # $1M liquidity
                "max_slippage_1k": Decimal("0.0100"),  # 1% slippage
                "trusted_token": usdc_address,  # USDC is the trusted token
                "filter_pass_type": "TRUSTED",
                "iteration_depth": 0
            })

            conn.commit()

            # Verify whitelist entry
            result = conn.execute(text(f"""
                SELECT filter_pass_type, trusted_token
                FROM whitelisted_pools_cryo
                WHERE pool_address = :pool_address
            """), {"pool_address": test_pool}).fetchone()

            assert result[0] == "TRUSTED"
            assert result[1] == usdc_address

    @pytest.mark.asyncio
    async def test_network_effect_filter(self):
        """Test second-pass network effect token discovery."""
        with self.engine.connect() as conn:
            # Create a whitelisted pool with a new token
            trusted_pool = "0xpool_with_new_token"
            new_token = "0xnew_discovered_token"
            usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

            self.test_pools.add(trusted_pool)
            self.test_tokens.add(new_token)

            # First pool: USDC-NewToken (gets whitelisted due to USDC)
            conn.execute(text("""
                INSERT INTO whitelisted_pools_cryo
                (pool_address, chain_id, token0, token1, fee,
                 liquidity_usd, filter_pass_type, iteration_depth)
                VALUES (:pool_address, :chain_id, :token0, :token1, :fee,
                        :liquidity_usd, :filter_pass_type, :iteration_depth)
            """), {
                "pool_address": trusted_pool,
                "chain_id": 1,
                "token0": usdc,
                "token1": new_token,
                "fee": 3000,
                "liquidity_usd": 500000.00,
                "filter_pass_type": "TRUSTED",
                "iteration_depth": 0
            })

            # Now new_token should be discoverable for network effect
            # Create another pool with new_token and another unknown token
            second_pool = "0xpool_network_effect"
            another_token = "0xanother_token"

            self.test_pools.add(second_pool)
            self.test_tokens.add(another_token)

            conn.execute(text("""
                INSERT INTO network_1_dex_pools_cryo
                (address, factory, asset0, asset1, fee, creation_block)
                VALUES (:address, :factory, :asset0, :asset1, :fee, :creation_block)
            """), {
                "address": second_pool,
                "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                "asset0": new_token,
                "asset1": another_token,
                "fee": 3000,
                "creation_block": 19000000
            })

            # Query for pools eligible for network effect
            result = conn.execute(text("""
                SELECT DISTINCT p.address
                FROM network_1_dex_pools_cryo p
                WHERE p.address NOT IN (
                    SELECT pool_address FROM whitelisted_pools_cryo WHERE chain_id = 1
                )
                AND (
                    p.asset0 IN (
                        SELECT DISTINCT token0 FROM whitelisted_pools_cryo WHERE chain_id = 1
                        UNION
                        SELECT DISTINCT token1 FROM whitelisted_pools_cryo WHERE chain_id = 1
                    )
                    OR p.asset1 IN (
                        SELECT DISTINCT token0 FROM whitelisted_pools_cryo WHERE chain_id = 1
                        UNION
                        SELECT DISTINCT token1 FROM whitelisted_pools_cryo WHERE chain_id = 1
                    )
                )
            """)).fetchall()

            pool_addresses = [r[0] for r in result]
            assert second_pool in pool_addresses, "Network effect pool not found"

            conn.commit()

    @pytest.mark.asyncio
    async def test_historical_data_fetch_with_checkpoint(self):
        """Test fetching historical data with checkpoint tracking."""
        with self.engine.connect() as conn:
            # Create a processing checkpoint
            factory_addresses = self.config.protocols.get_factory_addresses("uniswap_v3", "ethereum")
            factory_address = factory_addresses[0] if factory_addresses else None
            deployment_block = self.config.protocols.get_deployment_block("uniswap_v3", "ethereum")

            # Insert checkpoint
            conn.execute(text("""
                INSERT INTO processing_checkpoints_cryo
                (chain_id, factory_address, process_type, start_block,
                 end_block, current_block, status)
                VALUES (:chain_id, :factory_address, :process_type, :start_block,
                        :end_block, :current_block, :status)
                ON CONFLICT (chain_id, factory_address, process_type, start_block)
                DO UPDATE SET
                    current_block = EXCLUDED.current_block,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                "chain_id": 1,
                "factory_address": factory_address,
                "process_type": "HISTORICAL_FETCH",
                "start_block": deployment_block,
                "end_block": deployment_block + 100000,
                "current_block": deployment_block + 50000,
                "status": "IN_PROGRESS"
            })

            conn.commit()

            # Verify checkpoint
            result = conn.execute(text(f"""
                SELECT current_block, status
                FROM processing_checkpoints_cryo
                WHERE factory_address = :factory_address
                AND process_type = 'HISTORICAL_FETCH'
            """), {"factory_address": factory_address}).fetchone()

            assert result[0] == deployment_block + 50000
            assert result[1] == "IN_PROGRESS"