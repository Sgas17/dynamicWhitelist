#!/usr/bin/env python3
"""
Integration tests for the storage abstraction layer.
Tests the PostgreSQL, Redis, and JSON storage implementations with existing schema.
"""

import asyncio
import os
import sys
from datetime import datetime

import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.core.storage import StorageManager


class TestStorageIntegration:
    """Integration tests for storage layer with existing database schema."""

    @pytest.fixture(scope="class")
    async def storage_manager(self):
        """Fixture to provide storage manager for tests."""
        storage = StorageManager()
        await storage.initialize()
        yield storage
        await storage.shutdown()

    @pytest.mark.asyncio
    async def test_storage_initialization(self):
        """Test storage manager initialization and health checks."""
        storage = StorageManager()

        # Test initialization
        await storage.initialize()
        assert storage.is_initialized

        # Test health check
        health = await storage.health_check()
        assert isinstance(health, dict)
        assert "postgres" in health
        assert "redis" in health
        assert "json" in health
        assert "healthy" in health

        # Test shutdown
        await storage.shutdown()
        assert not storage.is_initialized

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test storage manager as async context manager."""
        async with StorageManager() as storage:
            assert storage.is_initialized
            health = await storage.health_check()
            # At least JSON should be healthy
            assert health["json"] is True

    @pytest.mark.asyncio
    async def test_json_operations(self):
        """Test JSON storage operations."""
        async with StorageManager() as storage:
            # Test saving and loading whitelist
            test_tokens = [
                {
                    "id": "0x1234567890abcdef1234567890abcdef12345678",
                    "symbol": "TEST1",
                    "name": "Test Token 1",
                    "market_cap_rank": 100,
                },
                {
                    "id": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "symbol": "TEST2",
                    "name": "Test Token 2",
                    "market_cap_rank": 200,
                },
            ]

            # Save whitelist
            success = storage.json.save_whitelist("ethereum", test_tokens)
            assert success

            # Load whitelist
            loaded_tokens = storage.json.load_whitelist("ethereum")
            assert loaded_tokens is not None
            assert len(loaded_tokens) == 2
            assert loaded_tokens[0]["symbol"] == "TEST1"

            # Test pools
            test_pools = [
                {
                    "address": "0xpool1234567890abcdef1234567890abcdef1234",
                    "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "asset0": "0x1234567890abcdef1234567890abcdef12345678",
                    "asset1": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "fee": 3000,
                    "creation_block": 18000000,
                    "tick_spacing": 60,
                }
            ]

            success = storage.json.save_pools("ethereum", "uniswap_v3", test_pools)
            assert success

            loaded_pools = storage.json.load_pools("ethereum", "uniswap_v3")
            assert loaded_pools is not None
            assert len(loaded_pools) == 1
            assert loaded_pools[0]["fee"] == 3000

    @pytest.mark.asyncio
    async def test_postgres_operations_if_available(self):
        """Test PostgreSQL operations if database is available."""
        async with StorageManager() as storage:
            # Check if PostgreSQL is available
            if not await storage.postgres.health_check():
                pytest.skip("PostgreSQL not available")

            # Test token operations with existing schema
            test_tokens = [
                {
                    "id": "test-token-1",
                    "symbol": "TEST1",
                    "name": "Test Token 1",
                    "market_cap_rank": 100,
                },
                {
                    "id": "test-token-2",
                    "symbol": "TEST2",
                    "name": "Test Token 2",
                    "market_cap_rank": 200,
                },
            ]

            # Store tokens
            count = await storage.postgres.store_tokens_batch(test_tokens, "ethereum")
            assert count == 2

            # Retrieve token
            token = await storage.postgres.get_token("test-token-1", "ethereum")
            assert token is not None
            assert token["symbol"] == "TEST1"

            # Get all tokens
            all_tokens = await storage.postgres.get_whitelisted_tokens("ethereum")
            assert isinstance(all_tokens, list)
            # Should contain at least our test tokens
            test_symbols = {
                t["symbol"] for t in all_tokens if t["symbol"] in ["TEST1", "TEST2"]
            }
            assert "TEST1" in test_symbols

            # Test pool operations
            test_pools = [
                {
                    "address": "0xtest1234567890abcdef1234567890abcdef1234",
                    "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                    "asset0": "0x1234567890abcdef1234567890abcdef12345678",
                    "asset1": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "fee": 3000,
                    "creation_block": 18000000,
                    "tick_spacing": 60,
                    "additional_data": {"test": True},
                }
            ]

            # Store pools
            count = await storage.postgres.store_pools_batch(
                test_pools, "ethereum", "uniswap_v3"
            )
            assert count == 1

            # Retrieve pool
            pool = await storage.postgres.get_pool(
                "0xtest1234567890abcdef1234567890abcdef1234", "ethereum"
            )
            assert pool is not None
            assert pool["fee"] == 3000
            assert pool["additional_data"]["test"] is True

            # Get all pools
            all_pools = await storage.postgres.get_active_pools("ethereum")
            assert isinstance(all_pools, list)
            # Should contain at least our test pool
            test_addresses = {p["address"] for p in all_pools}
            assert "0xtest1234567890abcdef1234567890abcdef1234" in test_addresses

    @pytest.mark.asyncio
    async def test_redis_operations_if_available(self):
        """Test Redis operations if Redis is available."""
        async with StorageManager() as storage:
            # Check if Redis is available
            if not await storage.redis.health_check():
                pytest.skip("Redis not available")

            # Test basic cache operations
            test_data = {"test": "data", "number": 123}

            # Set and get
            success = await storage.redis.set("test_key", test_data, ttl=60)
            assert success

            retrieved = await storage.redis.get("test_key")
            assert retrieved == test_data

            # Test existence
            exists = await storage.redis.exists("test_key")
            assert exists

            # Test whitelist caching
            test_whitelist = [
                {"symbol": "BTC", "price": 50000},
                {"symbol": "ETH", "price": 3000},
            ]

            success = await storage.redis.set_whitelist("ethereum", test_whitelist)
            assert success

            cached_whitelist = await storage.redis.get_whitelist("ethereum")
            assert cached_whitelist == test_whitelist

            # Test invalidation
            success = await storage.redis.invalidate_whitelist("ethereum")
            assert success

            cached_after_invalidation = await storage.redis.get_whitelist("ethereum")
            assert cached_after_invalidation is None

    @pytest.mark.asyncio
    async def test_high_level_operations(self):
        """Test high-level storage manager operations."""
        async with StorageManager() as storage:
            # Test cached operations (should work with JSON fallback)
            test_whitelist = [
                {
                    "id": "cache-test-1",
                    "symbol": "CACHE1",
                    "name": "Cache Test 1",
                    "market_cap_rank": 1,
                }
            ]

            # This should save to all available backends
            results = await storage.save_whitelist_everywhere(
                "test_chain", test_whitelist
            )
            assert isinstance(results, dict)
            assert "json" in results
            assert results["json"] is True  # JSON should always work

            # Test export functionality
            success = await storage.export_all_data("test_export.json")
            assert success

            # Verify export file was created
            exported_data = storage.json.load("test_export.json")
            assert exported_data is not None
            assert "whitelists" in exported_data or "pools" in exported_data


if __name__ == "__main__":
    # Run tests directly with asyncio
    async def run_tests():
        test_instance = TestStorageIntegration()

        print("=" * 60)
        print("Storage Integration Test Suite")
        print("=" * 60)

        tests = [
            ("Initialization", test_instance.test_storage_initialization),
            ("Context Manager", test_instance.test_context_manager),
            ("JSON Operations", test_instance.test_json_operations),
            (
                "PostgreSQL Operations",
                test_instance.test_postgres_operations_if_available,
            ),
            ("Redis Operations", test_instance.test_redis_operations_if_available),
            ("High-Level Operations", test_instance.test_high_level_operations),
        ]

        results = []
        for name, test_func in tests:
            try:
                print(f"\n--- Running {name} ---")
                await test_func()
                print(f"✅ {name} PASSED")
                results.append((name, True))
            except Exception as e:
                print(f"❌ {name} FAILED: {e}")
                results.append((name, False))

        # Summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)

        for name, result in results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{name}: {status}")

        passed = sum(1 for _, result in results if result)
        total = len(results)

        print(f"\nTotal: {passed}/{total} tests passed")

        if passed == total:
            print("\n🎉 All tests passed!")
        else:
            print(f"\n⚠️  {total - passed} tests failed")
            print("Note: Some tests may fail if PostgreSQL or Redis are not available")

    asyncio.run(run_tests())
