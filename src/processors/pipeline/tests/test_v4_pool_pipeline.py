"""
Tests for V4 pool pipeline.

Focuses on live integration tests with real blockchain data.
"""

import pytest
import logging
from pathlib import Path

from src.processors.pipeline.v4_pool_pipeline import V4PoolPipeline, fetch_v4_pools
from src.config.manager import ConfigManager


logger = logging.getLogger(__name__)


@pytest.fixture
def pipeline():
    """Create V4PoolPipeline instance."""
    return V4PoolPipeline()


@pytest.fixture
def config():
    """Get configuration manager."""
    return ConfigManager()


@pytest.mark.asyncio
class TestV4PoolPipelineLive:
    """Live integration tests using real blockchain data."""

    async def test_fetch_events_ethereum_recent(self, pipeline):
        """Test fetching recent V4 events for Ethereum."""
        # Use a small recent block range to avoid long fetch times
        config = ConfigManager()
        v4_config = config.protocols.get_protocol_config("uniswap_v4", "ethereum")
        deployment_block = v4_config.get("deployment_block", 21688329)

        # Test with 100 blocks from deployment
        start_block = deployment_block
        end_block = deployment_block + 100

        success = await pipeline.fetch_events(
            chain="ethereum", start_block=start_block, end_block=end_block
        )

        assert success, "Event fetching should succeed"

        # Verify files were created
        data_dir = Path(config.base.DATA_DIR)
        events_dir = data_dir / "ethereum" / "uniswap_v4_initialize_events"
        parquet_files = list(events_dir.glob("*.parquet"))

        logger.info(f"Found {len(parquet_files)} parquet files")
        # Note: May be 0 if no events in this range, which is valid

    async def test_process_events_ethereum(self, pipeline):
        """Test processing V4 events for Ethereum."""
        result = await pipeline.process_events("ethereum")

        assert result["success"], (
            f"Processing should succeed: {result.get('error', '')}"
        )
        assert "processed_count" in result
        assert "metadata" in result

        logger.info(f"Processed {result['processed_count']} pools")

    async def test_full_pipeline_ethereum_incremental(self, pipeline):
        """Test full pipeline for Ethereum with incremental processing."""
        result = await pipeline.run_full_pipeline(
            chain="ethereum",
            from_deployment=False,  # Incremental
        )

        assert result["success"], f"Pipeline should succeed: {result.get('error', '')}"
        assert result["fetch_success"], "Fetch step should succeed"
        assert result["process_success"], "Process step should succeed"
        assert "processed_count" in result
        assert "metadata" in result

        logger.info(f"Pipeline result: {result}")

    async def test_validate_config_all_chains(self, config):
        """Test that V4 configurations exist for all supported chains."""
        chains = ["ethereum", "base", "arbitrum"]

        for chain in chains:
            try:
                v4_config = config.protocols.get_protocol_config("uniswap_v4", chain)
                assert "deployment_block" in v4_config, (
                    f"Missing deployment_block for {chain}"
                )
                assert "pool_manager" in v4_config, f"Missing pool_manager for {chain}"

                deployment_block = v4_config["deployment_block"]
                pool_manager = v4_config["pool_manager"]

                assert deployment_block > 0, (
                    f"Invalid deployment_block for {chain}: {deployment_block}"
                )
                assert pool_manager.startswith("0x"), (
                    f"Invalid pool_manager for {chain}: {pool_manager}"
                )

                logger.info(
                    f"{chain}: deployment={deployment_block}, manager={pool_manager}"
                )

            except Exception as e:
                pytest.fail(f"Config validation failed for {chain}: {e}")

    async def test_convenience_function(self):
        """Test convenience function for single chain processing."""
        result = await fetch_v4_pools(chain="ethereum", from_deployment=False)

        assert result["success"], (
            f"Convenience function should succeed: {result.get('error', '')}"
        )
        assert result["chain"] == "ethereum"


@pytest.mark.slow
@pytest.mark.asyncio
class TestV4PoolPipelineHistorical:
    """Historical tests for full data fetching (marked as slow)."""

    async def test_full_pipeline_ethereum_historical_small_range(self):
        """Test full pipeline with small historical range."""
        pipeline = V4PoolPipeline()
        config = ConfigManager()

        v4_config = config.protocols.get_protocol_config("uniswap_v4", "ethereum")
        deployment_block = v4_config.get("deployment_block", 21688329)

        # Small range for testing
        start_block = deployment_block
        end_block = deployment_block + 1000

        result = await pipeline.run_full_pipeline(
            chain="ethereum",
            from_deployment=False,
            start_block=start_block,
            end_block=end_block,
        )

        assert result["success"], (
            f"Historical pipeline should succeed: {result.get('error', '')}"
        )

        # Check we got reasonable results
        if result.get("processed_count", 0) > 0:
            logger.info(f"Processed {result['processed_count']} pools")
            assert result["metadata"], "Should have metadata"
        else:
            logger.info("No pools found in test range (this is valid)")


class TestV4PoolPipelineUnit:
    """Unit tests for pipeline components."""

    def test_pipeline_initialization(self):
        """Test pipeline initializes correctly."""
        pipeline = V4PoolPipeline()
        assert pipeline.config is not None
        assert hasattr(pipeline, "config")

    def test_get_latest_finalized_block_fallback(self):
        """Test fallback behavior for latest block."""
        pipeline = V4PoolPipeline()

        # Test with invalid RPC should use fallback
        fallback_block = pipeline._get_latest_finalized_block(
            "http://invalid-rpc", 1000
        )
        assert fallback_block == 2000  # start + 1000
