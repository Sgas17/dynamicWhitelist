#!/usr/bin/env python3
"""
Quick validation test for chain-specific data fetchers (no network calls).

KISS: Just test that the fetcher framework is properly structured.
"""

import asyncio
from unittest.mock import patch, AsyncMock

from ....fetchers import get_fetcher, list_fetchers, CryoFetcher
from ....fetchers.ethereum_fetcher import EthereumFetcher


def test_fetcher_classes():
    """Test that fetcher classes are properly structured."""
    print("=== Testing Fetcher Class Structure ===")
    
    try:
        # Test that classes can be imported and instantiated
        fetcher = EthereumFetcher("http://test.example.com:8545")
        
        # Test required methods exist
        required_methods = [
            'validate_config', 'get_latest_block', 'fetch_logs', 
            'get_identifier', 'calculate_block_range'
        ]
        
        for method in required_methods:
            assert hasattr(fetcher, method), f"Missing method: {method}"
            assert callable(getattr(fetcher, method)), f"Method not callable: {method}"
        
        # Test identifier
        identifier = fetcher.get_identifier()
        assert identifier == "ethereum_fetcher", f"Unexpected identifier: {identifier}"
        
        # Test block range calculation with mocked network call
        with patch.object(fetcher, 'get_latest_block', new_callable=AsyncMock) as mock_latest:
            mock_latest.return_value = 18000000
            start, end = asyncio.run(fetcher.calculate_block_range(1))
        assert isinstance(start, int), "Start block not integer"
        assert isinstance(end, int), "End block not integer" 
        assert start < end, "Invalid block range"
        
        print("✅ Fetcher class structure validation passed")
        
    except Exception as e:
        print(f"❌ Fetcher class structure validation failed: {e}")
        raise


def test_fetcher_discovery():
    """Test fetcher discovery system."""
    print("\n=== Testing Fetcher Discovery System ===")
    
    try:
        # Test discovery functions
        fetchers = list_fetchers()
        print(f"Available fetchers: {fetchers}")
        
        assert len(fetchers) > 0, "No fetchers discovered"
        assert 'ethereum' in fetchers, "Ethereum fetcher not discovered"
        
        # Test getting fetcher by name
        EthFetcherClass = get_fetcher('ethereum')
        assert EthFetcherClass is not None, "Could not get Ethereum fetcher"
        assert EthFetcherClass.__name__ == 'EthereumFetcher', "Wrong fetcher class"
        
        # Test error handling for unknown fetcher
        try:
            get_fetcher('nonexistent')
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "No fetcher available" in str(e), "Wrong error message"
        
        print("✅ Fetcher discovery validation passed")
        
    except Exception as e:
        print(f"❌ Fetcher discovery validation failed: {e}")
        raise


def test_ethereum_specific_methods():
    """Test Ethereum-specific methods."""
    print("\n=== Testing Ethereum-Specific Methods ===")
    
    try:
        fetcher = EthereumFetcher("http://test.example.com:8545")
        
        # Test Ethereum-specific methods exist
        ethereum_methods = [
            'fetch_uniswap_v3_pools',
            'fetch_uniswap_v4_pools', 
            'fetch_recent_transfers',
            'fetch_liquidity_events'
        ]
        
        for method in ethereum_methods:
            assert hasattr(fetcher, method), f"Missing Ethereum method: {method}"
            assert callable(getattr(fetcher, method)), f"Ethereum method not callable: {method}"
        
        # Test configuration properties
        assert fetcher.chain == "ethereum", "Wrong chain configuration"
        assert fetcher.blocks_per_minute == 5, "Wrong blocks per minute"
        assert isinstance(fetcher.cryo_options, list), "Cryo options not list"
        
        print("✅ Ethereum-specific methods validation passed")
        
    except Exception as e:
        print(f"❌ Ethereum-specific methods validation failed: {e}")
        raise


def test_cryo_wrapper():
    """Test cryo wrapper functionality."""
    print("\n=== Testing Cryo Wrapper ===")
    
    try:
        cryo_fetcher = CryoFetcher("ethereum", "http://test.example.com:8545") 
        
        # Test cryo-specific attributes
        assert hasattr(cryo_fetcher, 'cryo_options'), "Missing cryo_options"
        assert hasattr(cryo_fetcher, 'blocks_per_request'), "Missing blocks_per_request"
        
        # Test cryo options structure
        options = cryo_fetcher.cryo_options
        assert '--rpc' in options, "RPC option missing"
        assert 'http://test.example.com:8545' in options, "RPC URL missing"
        assert '--u256-types' in options, "u256-types option missing"
        
        print("✅ Cryo wrapper validation passed")
        
    except Exception as e:
        print(f"❌ Cryo wrapper validation failed: {e}")
        raise


def main():
    """Run all validation tests."""
    print("=" * 60)
    print("Chain-Specific Data Fetcher Validation")
    print("=" * 60)
    
    tests = [
        ("Fetcher Class Structure", test_fetcher_classes),
        ("Fetcher Discovery System", test_fetcher_discovery),
        ("Ethereum-Specific Methods", test_ethereum_specific_methods),
        ("Cryo Wrapper", test_cryo_wrapper)
    ]
    
    passed = 0
    for name, test_func in tests:
        try:
            success = test_func()
            if success:
                passed += 1
            print()
        except Exception as e:
            print(f"❌ {name}: ERROR - {e}\n")
    
    print("=" * 60)
    print(f"Results: {passed}/{len(tests)} validations passed")
    
    if passed == len(tests):
        print("🎉 All fetcher validations passed!")
        print("✅ Chain-specific data fetchers are properly structured")
    else:
        print("⚠️  Some validations failed")
    
    return passed == len(tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)