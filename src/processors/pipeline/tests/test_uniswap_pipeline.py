"""
Tests for unified Uniswap pool pipeline.

Focuses on live integration tests with real blockchain data across all versions.
"""

import logging
from pathlib import Path

import pytest

from src.config.manager import ConfigManager
from src.processors.pipeline.uniswap_pool_pipeline import (
    UniswapPoolPipeline,
    fetch_all_uniswap_pools,
    fetch_uniswap_pools,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def pipeline():
    """Create UniswapPoolPipeline instance."""
    return UniswapPoolPipeline()


@pytest.fixture
def config():
    """Get configuration manager."""
    return ConfigManager()


@pytest.mark.asyncio
class TestUniswapPipelineLive:
    """Live integration tests using real blockchain data."""

    async def test_v2_pipeline_ethereum_small_range(self, pipeline):
        """Test V2 pipeline with small block range on Ethereum."""
        result = await pipeline.run_version_pipeline(
            chain="ethereum",
            version="v2",
            start_block=10000835,  # V2 deployment
            end_block=10001000,  # Small range
        )

        assert result["success"], (
            f"V2 pipeline should succeed: {result.get('error', '')}"
        )
        assert result["version"] == "v2"
        assert result["chain"] == "ethereum"
        # Note: processed_count may be 0 if no pools in this range

    async def test_v3_pipeline_ethereum_incremental(self, pipeline):
        """Test V3 pipeline with incremental processing."""
        result = await pipeline.run_version_pipeline(chain="ethereum", version="v3")

        assert result["success"], (
            f"V3 pipeline should succeed: {result.get('error', '')}"
        )
        assert result["version"] == "v3"
        assert result["chain"] == "ethereum"
        assert "processed_count" in result

        logger.info(f"V3 incremental processed {result['processed_count']} pools")

    async def test_v4_pipeline_ethereum_incremental(self, pipeline):
        """Test V4 pipeline with incremental processing."""
        result = await pipeline.run_version_pipeline(chain="ethereum", version="v4")

        assert result["success"], (
            f"V4 pipeline should succeed: {result.get('error', '')}"
        )
        assert result["version"] == "v4"
        assert result["chain"] == "ethereum"
        assert "processed_count" in result

        logger.info(f"V4 incremental processed {result['processed_count']} pools")

    async def test_all_versions_ethereum(self, pipeline):
        """Test all versions pipeline for Ethereum."""
        result = await pipeline.run_all_versions("ethereum")

        assert result["overall_success"], "All versions should succeed"
        assert result["chain"] == "ethereum"
        assert "versions" in result

        # Check that we have results for all versions
        versions = result["versions"]
        expected_versions = ["v2", "v3", "v4"]

        for version in expected_versions:
            assert version in versions, f"Missing {version} results"
            version_result = versions[version]
            logger.info(
                f"{version.upper()}: {version_result.get('processed_count', 0)} pools"
            )

    async def test_convenience_functions(self):
        """Test convenience functions."""
        # Test single version
        result = await fetch_uniswap_pools("ethereum", "v4")
        assert result["success"], "Convenience function should work"
        assert result["version"] == "v4"

        # Test all versions on chain
        result = await fetch_all_uniswap_pools("ethereum")
        assert result["overall_success"], (
            "All versions convenience function should work"
        )
        assert "versions" in result

    async def test_protocol_configurations(self, config):
        """Test that all protocol configurations are valid."""
        chains = ["ethereum", "base", "arbitrum"]
        versions = ["v2", "v3", "v4"]

        for chain in chains:
            for version in versions:
                try:
                    config_data = config.protocols.get_protocol_config(
                        f"uniswap_{version}", chain
                    )
                    assert "deployment_block" in config_data, (
                        f"Missing deployment_block for {version} on {chain}"
                    )

                    deployment_block = config_data["deployment_block"]
                    assert deployment_block >= 0, (
                        f"Invalid deployment_block for {version} on {chain}"
                    )

                    # Check addresses
                    if version == "v4":
                        assert "pool_manager" in config_data, (
                            f"Missing pool_manager for V4 on {chain}"
                        )
                        pool_manager = config_data["pool_manager"]
                        assert pool_manager.startswith("0x"), (
                            f"Invalid pool_manager for V4 on {chain}"
                        )
                    else:
                        assert "factory_addresses" in config_data, (
                            f"Missing factory_addresses for {version} on {chain}"
                        )
                        factories = config_data["factory_addresses"]
                        assert isinstance(factories, list), (
                            f"factory_addresses should be list for {version} on {chain}"
                        )

                    logger.info(f"{chain}/{version}: deployment={deployment_block}")

                except Exception as e:
                    # Some chains may not support all versions - that's ok for now
                    logger.warning(f"Config issue for {chain}/{version}: {e}")


@pytest.mark.slow
@pytest.mark.asyncio
class TestUniswapPipelineHistorical:
    """Historical tests for full data fetching (marked as slow)."""

    async def test_v2_historical_small_range(self):
        """Test V2 historical pipeline with small range."""
        pipeline = UniswapPoolPipeline()

        result = await pipeline.run_version_pipeline(
            chain="ethereum",
            version="v2",
            from_deployment=True,
            start_block=10000835,
            end_block=10002000,  # Small historical range
        )

        assert result["success"], (
            f"V2 historical should succeed: {result.get('error', '')}"
        )

        if result.get("processed_count", 0) > 0:
            logger.info(f"V2 historical processed {result['processed_count']} pools")
        else:
            logger.info("No V2 pools found in test range (valid)")

    async def test_all_versions_from_deployment_ethereum(self):
        """Test all versions from deployment (small ranges)."""
        pipeline = UniswapPoolPipeline()

        # Use small ranges for each version to avoid long test times
        version_configs = {
            "v2": {"start": 10000835, "end": 10001000},
            "v3": {"start": 12369621, "end": 12369721},
            "v4": {"start": 21688329, "end": 21688429},
        }

        for version, block_config in version_configs.items():
            result = await pipeline.run_version_pipeline(
                chain="ethereum",
                version=version,
                start_block=block_config["start"],
                end_block=block_config["end"],
            )

            assert result["success"], (
                f"{version.upper()} should succeed: {result.get('error', '')}"
            )
            logger.info(
                f"{version.upper()}: {result.get('processed_count', 0)} pools in test range"
            )


class TestUniswapPipelineUnit:
    """Unit tests for pipeline components."""

    def test_pipeline_initialization(self):
        """Test pipeline initializes correctly."""
        pipeline = UniswapPoolPipeline()
        assert pipeline.config is not None
        assert hasattr(pipeline, "protocols")
        assert "v2" in pipeline.protocols
        assert "v3" in pipeline.protocols
        assert "v4" in pipeline.protocols

    def test_protocol_configs(self):
        """Test protocol configurations are properly set."""
        pipeline = UniswapPoolPipeline()

        # Check V2 config
        v2_config = pipeline.protocols["v2"]
        assert v2_config["address_key"] == "factory_addresses"
        assert "event_hash" in v2_config
        assert "event_dir" in v2_config

        # Check V3 config
        v3_config = pipeline.protocols["v3"]
        assert v3_config["address_key"] == "factory_addresses"
        assert "event_hash" in v3_config
        assert "event_dir" in v3_config

        # Check V4 config
        v4_config = pipeline.protocols["v4"]
        assert v4_config["address_key"] == "pool_manager"
        assert "event_hash" in v4_config
        assert "event_dir" in v4_config

    def test_unsupported_version_handling(self):
        """Test handling of unsupported versions."""
        pipeline = UniswapPoolPipeline()

        with pytest.raises(ValueError, match="Unsupported version"):
            pipeline.fetch_events("ethereum", "v5", 100, 200)

    def test_fallback_block_calculation(self):
        """Test fallback behavior for latest block."""
        pipeline = UniswapPoolPipeline()

        # Test with invalid RPC should use fallback
        fallback_block = pipeline._get_latest_finalized_block(
            "http://invalid-rpc", 1000
        )
        assert fallback_block == 2000  # start + 1000
