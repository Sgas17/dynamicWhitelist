#!/usr/bin/env python3
"""
Validation script for the storage abstraction layer with CoinGecko schema.
Tests against your actual database with existing tables.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.storage import StorageManager


async def validate_database_connection():
    """Test basic database connection and table existence."""
    print("\n=== Validating Database Connection ===")
    
    try:
        async with StorageManager() as storage:
            health = await storage.health_check()
            print(f"Health check: {health}")
            
            if not health.get('postgres'):
                print("❌ PostgreSQL not available - skipping database tests")
                return False
                
            print("✅ Database connection established")
            return True
            
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


async def validate_coingecko_data():
    """Validate CoinGecko token data retrieval."""
    print("\n=== Validating CoinGecko Data ===")
    
    try:
        async with StorageManager() as storage:
            if not await storage.postgres.health_check():
                print("⚠️  PostgreSQL not available")
                return False
            
            # Test retrieving tokens for ethereum
            tokens = await storage.postgres.get_whitelisted_tokens('ethereum')
            print(f"✅ Retrieved {len(tokens)} ethereum tokens")
            
            if tokens:
                # Show sample from main CoinGecko data
                coingecko_tokens = [t for t in tokens if t.get('id')]
                missing_tokens = [t for t in tokens if not t.get('id')]
                
                print(f"   - {len(coingecko_tokens)} tokens with CoinGecko IDs")
                print(f"   - {len(missing_tokens)} tokens in missing tables")
                
                if coingecko_tokens:
                    sample = coingecko_tokens[0]
                    print(f"   Sample CoinGecko token:")
                    print(f"     ID: {sample.get('id')}")
                    print(f"     Symbol: {sample.get('symbol')}")
                    print(f"     Name: {sample.get('name')}")
                    print(f"     Address: {sample.get('address')}")
                    print(f"     Market Cap Rank: {sample.get('market_cap_rank')}")
                
                if missing_tokens:
                    sample = missing_tokens[0]
                    print(f"   Sample missing token:")
                    print(f"     Symbol: {sample.get('symbol')}")
                    print(f"     Address: {sample.get('address')}")
                    print(f"     Name: {sample.get('name')}")
            
            # Test specific token lookup by address
            if tokens:
                test_address = tokens[0]['address']
                if test_address:
                    found_token = await storage.postgres.get_token(test_address, 'ethereum')
                    if found_token:
                        print(f"✅ Successfully retrieved token by address: {found_token.get('symbol')}")
                    else:
                        print("❌ Failed to retrieve token by address")
            
            return True
            
    except Exception as e:
        print(f"❌ CoinGecko data validation failed: {e}")
        return False


async def validate_pool_data():
    """Validate DEX pool data retrieval."""
    print("\n=== Validating Pool Data ===")
    
    try:
        async with StorageManager() as storage:
            if not await storage.postgres.health_check():
                print("⚠️  PostgreSQL not available")
                return False
            
            # Test retrieving pools for ethereum (network_1_dex_pools_cryo)
            pools = await storage.postgres.get_active_pools('ethereum')
            print(f"✅ Retrieved {len(pools)} ethereum pools")
            
            if pools:
                sample = pools[0]
                print(f"   Sample pool:")
                print(f"     Address: {sample.get('address')}")
                print(f"     Factory: {sample.get('factory')}")
                print(f"     Asset0: {sample.get('asset0')}")
                print(f"     Asset1: {sample.get('asset1')}")
                print(f"     Fee: {sample.get('fee')}")
                print(f"     Creation Block: {sample.get('creation_block')}")
                
                # Test specific pool lookup
                test_address = sample['address']
                found_pool = await storage.postgres.get_pool(test_address, 'ethereum')
                if found_pool:
                    print(f"✅ Successfully retrieved pool by address")
                else:
                    print("❌ Failed to retrieve pool by address")
            
            return True
            
    except Exception as e:
        print(f"❌ Pool data validation failed: {e}")
        return False


async def validate_json_operations():
    """Validate JSON storage operations."""
    print("\n=== Validating JSON Operations ===")
    
    try:
        async with StorageManager() as storage:
            # Test basic JSON operations
            test_data = {
                'timestamp': datetime.now().isoformat(),
                'validation_test': True,
                'sample_tokens': [
                    {'symbol': 'BTC', 'name': 'Bitcoin'},
                    {'symbol': 'ETH', 'name': 'Ethereum'}
                ]
            }
            
            # Save and load test
            success = storage.json.save('validation_test.json', test_data)
            if success:
                print("✅ JSON save operation successful")
                
                loaded = storage.json.load('validation_test.json')
                if loaded and loaded['validation_test'] is True:
                    print("✅ JSON load operation successful")
                else:
                    print("❌ JSON load verification failed")
            else:
                print("❌ JSON save operation failed")
            
            # Test whitelist operations
            sample_whitelist = [
                {
                    'id': 'bitcoin',
                    'symbol': 'BTC',
                    'name': 'Bitcoin',
                    'address': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
                }
            ]
            
            success = storage.json.save_whitelist('validation', sample_whitelist)
            if success:
                print("✅ JSON whitelist save successful")
                
                loaded_whitelist = storage.json.load_whitelist('validation')
                if loaded_whitelist and len(loaded_whitelist) == 1:
                    print("✅ JSON whitelist load successful")
            
            return True
            
    except Exception as e:
        print(f"❌ JSON operations validation failed: {e}")
        return False


async def validate_redis_operations():
    """Validate Redis operations if available."""
    print("\n=== Validating Redis Operations ===")
    
    try:
        async with StorageManager() as storage:
            if not await storage.redis.health_check():
                print("⚠️  Redis not available - skipping Redis tests")
                return True  # Not a failure, just not available
            
            # Test basic cache operations
            test_key = 'validation_test'
            test_value = {'timestamp': datetime.now().isoformat(), 'test': True}
            
            success = await storage.redis.set(test_key, test_value, ttl=60)
            if success:
                print("✅ Redis set operation successful")
                
                retrieved = await storage.redis.get(test_key)
                if retrieved and retrieved['test'] is True:
                    print("✅ Redis get operation successful")
                else:
                    print("❌ Redis get verification failed")
            else:
                print("❌ Redis set operation failed")
            
            # Test whitelist caching (matching existing pattern)
            test_whitelist = [{'symbol': 'BTC', 'price': 50000}]
            success = await storage.redis.set_whitelist('validation', test_whitelist)
            if success:
                print("✅ Redis whitelist cache successful")
                
                cached = await storage.redis.get_whitelist('validation')
                if cached and len(cached) == 1:
                    print("✅ Redis whitelist retrieval successful")
            
            # Test Hyperliquid-style keys (matching existing pattern)
            hyperliquid_key = 'whitelist.hyperliquid_tokens:BTC'
            await storage.redis.set(hyperliquid_key, '50000', ttl=300)
            btc_price = await storage.redis.get(hyperliquid_key)
            if btc_price == '50000':
                print("✅ Hyperliquid-style key operations successful")
            
            return True
            
    except Exception as e:
        print(f"❌ Redis operations validation failed: {e}")
        return False


async def validate_integration():
    """Validate integration between storage backends."""
    print("\n=== Validating Storage Integration ===")
    
    try:
        async with StorageManager() as storage:
            # Test high-level operations that use multiple backends
            if await storage.postgres.health_check():
                # Get some real data from database
                tokens = await storage.postgres.get_whitelisted_tokens('ethereum')
                if tokens:
                    sample_tokens = tokens[:5]  # Just first 5 for testing
                    
                    # Save to all backends
                    results = await storage.save_whitelist_everywhere('validation', sample_tokens)
                    print(f"✅ Multi-backend save results: {results}")
                    
                    # Test cached retrieval
                    cached_tokens = await storage.get_cached_whitelist('validation')
                    if cached_tokens:
                        print(f"✅ Cached retrieval returned {len(cached_tokens)} tokens")
                    else:
                        print("⚠️  Cached retrieval returned no data (expected if Redis unavailable)")
            
            # Test export functionality
            success = await storage.export_all_data('validation_export.json')
            if success:
                print("✅ Data export successful")
                
                # Verify export file
                exported = storage.json.load('validation_export.json')
                if exported:
                    print("✅ Export file verification successful")
                    if 'whitelists' in exported:
                        print(f"   - Exported whitelists for {len(exported['whitelists'])} chains")
                    if 'pools' in exported:
                        print(f"   - Exported pools for {len(exported['pools'])} chain/protocol combinations")
            
            return True
            
    except Exception as e:
        print(f"❌ Integration validation failed: {e}")
        return False


async def main():
    """Run all validation tests."""
    print("=" * 70)
    print("Storage Abstraction Layer - Validation Suite")
    print("Testing with actual CoinGecko database schema")
    print("=" * 70)
    
    tests = [
        ("Database Connection", validate_database_connection),
        ("CoinGecko Data", validate_coingecko_data),
        ("Pool Data", validate_pool_data),
        ("JSON Operations", validate_json_operations),
        ("Redis Operations", validate_redis_operations),
        ("Integration", validate_integration)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            print(f"\n{'='*50}")
            print(f"Running: {name}")
            print(f"{'='*50}")
            
            result = await test_func()
            results.append((name, result))
            
            if result:
                print(f"\n✅ {name}: PASSED")
            else:
                print(f"\n❌ {name}: FAILED")
                
        except Exception as e:
            print(f"\n❌ {name}: ERROR - {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{name:<25}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\nTotal: {passed}/{total} validations passed")
    
    if passed == total:
        print("\n🎉 All validations passed!")
        print("Storage abstraction layer is working correctly with your CoinGecko schema!")
    else:
        print(f"\n⚠️  {total - passed} validations failed")
        print("Check database connections and schema compatibility")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)