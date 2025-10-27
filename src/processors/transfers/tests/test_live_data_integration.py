"""
Live data integration tests for UnifiedTransferProcessor.

These tests connect to actual TimescaleDB and test with real data.
Requires proper database configuration and environment variables.
"""

import pytest
import asyncio
import os
import tempfile
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import logging

from ..unified_transfer_processor import UnifiedTransferProcessor
from ....core.storage.timescaledb import (
    get_timescale_engine,
    setup_timescale_tables,
    cleanup_old_data
)

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests in this file as requiring live data
pytestmark = pytest.mark.live_data


def check_timescale_available():
    """Check if TimescaleDB is available for testing."""
    try:
        engine = get_timescale_engine()
        with engine.connect() as conn:
            from sqlalchemy import text
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.warning(f"TimescaleDB not available for testing: {e}")
        return False


@pytest.fixture(scope="session")
def live_processor():
    """Create processor with real TimescaleDB connection."""
    if not check_timescale_available():
        pytest.skip("TimescaleDB not available")
    
    processor = UnifiedTransferProcessor("ethereum")
    return processor


@pytest.fixture
def sample_live_data():
    """Create realistic sample transfer data for live testing."""
    import uuid
    import random
    
    # Use unique timestamps and token addresses to avoid conflicts
    base_time = datetime.now() - timedelta(minutes=random.randint(10, 60))
    unique_suffix = str(uuid.uuid4()).replace('-', '')[:8]
    
    return [
        {
            "token_address": f"0xtest1{unique_suffix.ljust(34, '0')}",
            "transaction_hash": f"0x{'1234567890abcdef' * 4}",
            "from_address": "0x742d35cc6e3c014c41ba3ab0b41f2e2e5e9e5678",
            "to_address": "0x8ba1f109551bd432803012645heff07b94090123",
            "value": 1000000000000000000,  # 1 ETH
            "timestamp": base_time,
            "block_number": 18500000
        },
        {
            "token_address": f"0xtest1{unique_suffix.ljust(34, '0')}",
            "transaction_hash": f"0x{'2345678901bcdef1' * 4}",
            "from_address": "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",  # MEV address
            "to_address": "0x9ba1f109551bd432803012645heff07b94090456",
            "value": 500000000000000000,  # 0.5 ETH
            "timestamp": base_time + timedelta(minutes=2),
            "block_number": 18500001
        },
        {
            "token_address": f"0xtest2{unique_suffix.ljust(34, '0')}",
            "transaction_hash": f"0x{'3456789012cdef12' * 4}",
            "from_address": "0x000000000000000000000000d4bc53434c5e12cb41381a556c3c47e1a86e80e3",  # MEV address
            "to_address": "0xaba1f109551bd432803012645heff07b94090789",
            "value": 2000000000000000000,  # 2 ETH
            "timestamp": base_time + timedelta(minutes=5),
            "block_number": 18500002
        }
    ]


class TestLiveDataIntegration:
    """Live integration tests with real TimescaleDB."""

    def test_timescale_connection(self, live_processor):
        """Test actual TimescaleDB connection."""
        assert live_processor.validate_config() is True
        logger.info("✅ TimescaleDB connection validated")

    def test_mev_addresses_loading(self, live_processor):
        """Test MEV addresses are loaded correctly from real config."""
        mev_addresses = live_processor._load_mev_addresses()
        
        assert isinstance(mev_addresses, set)
        assert len(mev_addresses) == 4  # Should have exactly 4 addresses
        
        # Check all 4 original addresses are present
        expected_addresses = {
            "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",
            "0x000000000000000000000000d4bc53434c5e12cb41381a556c3c47e1a86e80e3",
            "0x00000000000000000000000000000000009e50a7ddb7a7b0e2ee6604fd120e49",
            "0x000000000000000000000000e8c060f8052e07423f71d445277c61ac5138a2e5"
        }
        
        assert mev_addresses == expected_addresses
        logger.info(f"✅ All 4 MEV addresses loaded: {len(mev_addresses)}")

    @pytest.mark.asyncio
    async def test_live_database_stats(self, live_processor):
        """Test database statistics with real TimescaleDB."""
        stats = await live_processor.get_database_stats()
        
        # Should not have errors
        assert "error" not in stats
        
        # Should have table sizes
        assert "table_sizes" in stats
        assert isinstance(stats["table_sizes"], list)
        
        # Log real database stats
        logger.info("📊 Real Database Statistics:")
        for table in stats["table_sizes"]:
            logger.info(f"  - {table['table']}: {table['size_pretty']}")
        
        if stats.get("raw_data"):
            raw = stats["raw_data"]
            logger.info(f"  - Raw records: {raw['total_records']} ({raw['retention_days']} day retention)")
        
        if stats.get("hourly_data"):
            hourly = stats["hourly_data"]
            logger.info(f"  - Hourly records: {hourly['total_records']} ({hourly['retention_days']} day retention)")
        
        # Test compression stats
        if stats.get("compression"):
            for comp in stats["compression"]:
                if comp["enabled"]:
                    logger.info(f"  - {comp['table']} compression ratio: {comp['compression_ratio']:.1f}x")

    @pytest.mark.asyncio
    async def test_live_mev_analysis(self, live_processor):
        """Test MEV analysis with real TimescaleDB data."""
        # Query for MEV activity in last 24 hours
        mev_tokens = await live_processor.get_mev_active_tokens(
            hours_back=24,
            limit=10
        )
        
        assert isinstance(mev_tokens, list)
        logger.info(f"🤖 Found {len(mev_tokens)} MEV-active tokens in last 24h")
        
        # Log results
        for i, token in enumerate(mev_tokens[:5], 1):
            logger.info(f"  {i}. {token['token_address'][:10]}... "
                       f"({token['total_transfers']} total transfers, "
                       f"score: {token['mev_activity_score']:.2f}, "
                       f"volume: {token['cumulative_volume']:.2e})")

    @pytest.mark.asyncio
    async def test_live_data_storage_and_retrieval(self, live_processor, sample_live_data):
        """Test storing and retrieving data with real TimescaleDB."""
        # Store sample data
        stored_count = await live_processor._store_raw_data(sample_live_data)
        
        assert stored_count > 0
        logger.info(f"✅ Stored {stored_count} raw transfer records")
        
        # Wait a moment for data to be available
        await asyncio.sleep(1)
        
        # Try to retrieve top tokens (should include our test data)
        result = await live_processor._aggregate_and_get_top_tokens(
            sample_live_data, min_transfers=1
        )
        
        assert isinstance(result, list)
        logger.info(f"✅ Retrieved {len(result)} aggregated tokens")

    @pytest.mark.asyncio
    async def test_live_redis_integration(self, live_processor, sample_live_data):
        """Test Redis caching with real Redis instance."""
        try:
            # Create test tokens data
            test_tokens = [
                {
                    "token_address": "0xa0b86a33e6441e53a0f1e87f58f8fc87f7ff1234",
                    "transfer_count": 100,
                    "unique_senders": 50,
                    "unique_receivers": 75,
                    "avg_transfers": 85.5
                }
            ]
            
            # Test caching
            await live_processor._update_redis_cache(test_tokens)
            logger.info("✅ Redis cache updated successfully")
            
            # Test retrieval
            cached_tokens = await live_processor.get_cached_top_tokens()
            logger.info(f"✅ Retrieved {len(cached_tokens)} cached tokens")
            
            # Test individual token stats
            stats = await live_processor.get_token_stats(test_tokens[0]["token_address"])
            if stats:
                logger.info(f"✅ Individual token stats retrieved: {stats['transfer_count']} transfers")
            else:
                logger.info("ℹ️  No cached stats found (expected for new test data)")
                
        except Exception as e:
            logger.warning(f"Redis test failed (may not be configured): {e}")

    @pytest.mark.asyncio
    async def test_live_end_to_end_workflow(self, live_processor):
        """Test complete end-to-end workflow with real components."""
        # Create temporary test data
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test parquet file
            # Use unique test data to avoid conflicts with previous test runs
            import uuid
            unique_suffix = str(uuid.uuid4()).replace('-', '')[:8]
            test_token = f"0xtest{unique_suffix.ljust(36, '0')}"  # 42 chars total including 0x
            
            test_data = [
                {
                    "token_address": test_token,
                    "transaction_hash": f"0x{'fedcba9876543210' * 4}",
                    "from_address": "0x1111111111111111111111111111111111111111",
                    "to_address": "0x2222222222222222222222222222222222222222", 
                    "value": 1000000000000000000,
                    "timestamp": datetime.now() - timedelta(seconds=30),  # Use seconds for uniqueness
                    "block_number": 18500100
                }
            ]
            
            df = pl.DataFrame(test_data)
            test_file = Path(temp_dir) / "transfers_test.parquet"
            df.write_parquet(test_file)
            
            # Run complete processing workflow
            result = await live_processor.process(
                data_dir=temp_dir,
                hours_back=1,
                min_transfers=1,
                store_raw=True,
                update_cache=True
            )
            
            assert result.success is True
            logger.info(f"✅ End-to-end workflow completed: {result.processed_count} records processed")
            
            if result.data:
                logger.info(f"✅ Found {len(result.data)} qualifying tokens")
            else:
                logger.info("ℹ️  No tokens met minimum transfer criteria (expected for test data)")

    def test_table_existence_and_structure(self, live_processor):
        """Test that TimescaleDB tables exist and have correct structure."""
        try:
            engine = get_timescale_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                
                # Check raw transfers table
                raw_result = conn.execute(text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'token_raw_transfers'
                    ORDER BY ordinal_position
                """))
                
                raw_columns = {row[0]: row[1] for row in raw_result}
                logger.info(f"📋 Raw transfers table columns: {list(raw_columns.keys())}")
                
                # Check hourly transfers table  
                hourly_result = conn.execute(text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_name = 'token_hourly_transfers'
                    ORDER BY ordinal_position
                """))
                
                hourly_columns = {row[0]: row[1] for row in hourly_result}
                logger.info(f"📋 Hourly transfers table columns: {list(hourly_columns.keys())}")
                
                # Verify essential columns exist
                assert "timestamp" in raw_columns
                assert "token_address" in raw_columns
                assert "transfer_count" in raw_columns
                
                assert "hour_timestamp" in hourly_columns
                assert "token_address" in hourly_columns
                assert "avg_transfers_24h" in hourly_columns
                
                logger.info("✅ Table structures validated")
                
        except Exception as e:
            pytest.fail(f"Table structure validation failed: {e}")

    @pytest.mark.asyncio
    async def test_performance_with_real_data(self, live_processor):
        """Test performance with real database queries."""
        import time
        
        # Test MEV query performance
        start_time = time.time()
        mev_tokens = await live_processor.get_mev_active_tokens(hours_back=1, limit=50)
        mev_duration = time.time() - start_time
        
        logger.info(f"⚡ MEV query took {mev_duration:.2f}s, found {len(mev_tokens)} tokens")
        
        # Test database stats performance
        start_time = time.time()
        stats = await live_processor.get_database_stats()
        stats_duration = time.time() - start_time
        
        logger.info(f"⚡ Database stats query took {stats_duration:.2f}s")
        
        # Performance should be reasonable (< 5 seconds each)
        assert mev_duration < 5.0, f"MEV query too slow: {mev_duration:.2f}s"
        assert stats_duration < 5.0, f"Stats query too slow: {stats_duration:.2f}s"

    def test_configuration_validation_live(self, live_processor):
        """Test configuration validation with live systems."""
        # Should pass with real TimescaleDB
        assert live_processor.validate_config() is True
        
        # Test chain and protocol setup
        assert live_processor.chain == "ethereum"
        assert live_processor.protocol == "transfers"
        
        # Test logger is configured
        assert live_processor.logger is not None
        
        logger.info("✅ Live configuration validation passed")


# Utility functions for live testing

def run_live_tests():
    """Helper function to run live tests manually."""
    if not check_timescale_available():
        print("❌ TimescaleDB not available - cannot run live tests")
        return False
    
    print("🚀 Running live data integration tests...")
    result = pytest.main([
        __file__,
        "-v",
        "-s",
        "--tb=short",
        "-m", "live_data"
    ])
    
    return result == 0


if __name__ == "__main__":
    # Allow running this file directly for manual testing
    success = run_live_tests()
    exit(0 if success else 1)