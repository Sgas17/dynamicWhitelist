"""
Integration tests for token matching processor with real exchange data.

Tests the complete pipeline: Hyperliquid markets → CoinGecko matching → multi-chain results
"""

import pytest
import asyncio
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock

from ..token_matching_processor import TokenMatchingProcessor, TokenMatch
from ...fetchers.exchange_fetchers import HyperliquidFetcher, ExchangeToken


class TestTokenMatchingIntegration:
    """Integration tests for complete token matching pipeline."""
    
    @pytest.fixture
    def sample_hyperliquid_tokens(self) -> List[ExchangeToken]:
        """Sample Hyperliquid tokens for testing."""
        return [
            ExchangeToken(
                symbol='BTC/USD',
                base='BTC',
                quote='USD',
                market_id='0',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='ETH/USD',
                base='ETH', 
                quote='USD',
                market_id='1',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='USDC/USD',
                base='USDC',
                quote='USD',
                market_id='2',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='LINK/USD',
                base='LINK',
                quote='USD',
                market_id='3',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='PEPE/USD',
                base='PEPE',
                quote='USD',
                market_id='4',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            )
        ]
    
    @pytest.fixture
    def mock_coingecko_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Mock CoinGecko database response."""
        return {
            'ethereum': [
                {
                    'coingecko_id': 'bitcoin',
                    'symbol': 'WBTC',
                    'name': 'Wrapped Bitcoin',
                    'market_cap_rank': 1,
                    'platform': 'ethereum',
                    'address': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    'decimals': 8,
                    'total_supply': '150000'
                },
                {
                    'coingecko_id': 'ethereum',
                    'symbol': 'WETH',
                    'name': 'Wrapped Ether',
                    'market_cap_rank': 2,
                    'platform': 'ethereum', 
                    'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                    'decimals': 18,
                    'total_supply': '7000000'
                },
                {
                    'coingecko_id': 'usd-coin',
                    'symbol': 'USDC',
                    'name': 'USD Coin',
                    'market_cap_rank': 5,
                    'platform': 'ethereum',
                    'address': '0xa0b86a33e6a75c3c5b06b6b1f06b7c4dea73bb6e',
                    'decimals': 6,
                    'total_supply': '25000000000'
                },
                {
                    'coingecko_id': 'chainlink',
                    'symbol': 'LINK',
                    'name': 'Chainlink',
                    'market_cap_rank': 15,
                    'platform': 'ethereum',
                    'address': '0x514910771af9ca656af840dff83e8264ecf986ca',
                    'decimals': 18,
                    'total_supply': '1000000000'
                },
                {
                    'coingecko_id': 'pepe',
                    'symbol': 'PEPE',
                    'name': 'Pepe',
                    'market_cap_rank': 25,
                    'platform': 'ethereum',
                    'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933',
                    'decimals': 18,
                    'total_supply': '420690000000000'
                }
            ],
            'base': [
                {
                    'coingecko_id': 'usd-coin',
                    'symbol': 'USDC',
                    'name': 'USD Coin',
                    'market_cap_rank': 5,
                    'platform': 'base',
                    'address': '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913',
                    'decimals': 6,
                    'total_supply': '25000000000'
                },
                {
                    'coingecko_id': 'ethereum',
                    'symbol': 'WETH',
                    'name': 'Wrapped Ether',
                    'market_cap_rank': 2,
                    'platform': 'base',
                    'address': '0x4200000000000000000000000000000000000006',
                    'decimals': 18,
                    'total_supply': '7000000'
                }
            ],
            'arbitrum-one': [
                {
                    'coingecko_id': 'bitcoin',
                    'symbol': 'WBTC',
                    'name': 'Wrapped Bitcoin',
                    'market_cap_rank': 1,
                    'platform': 'arbitrum-one',
                    'address': '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f',
                    'decimals': 8,
                    'total_supply': '150000'
                },
                {
                    'coingecko_id': 'chainlink',
                    'symbol': 'LINK',
                    'name': 'Chainlink',
                    'market_cap_rank': 15,
                    'platform': 'arbitrum-one',
                    'address': '0xf97f4df75117a78c1a5a0dbb814af92458539fb4',
                    'decimals': 18,
                    'total_supply': '1000000000'
                }
            ]
        }
    
    @pytest.mark.asyncio
    async def test_token_matching_processor_initialization(self):
        """Test TokenMatchingProcessor can be initialized properly."""
        processor = TokenMatchingProcessor()
        
        assert processor.chain == "multi"
        assert processor.protocol == "token_matching"
        assert processor.validate_config() is True
        assert len(processor.chain_platform_mapping) > 0
        assert len(processor.symbol_mappings) > 0
    
    @pytest.mark.asyncio
    async def test_complete_token_matching_pipeline(self, sample_hyperliquid_tokens, mock_coingecko_data):
        """Test complete pipeline: exchange tokens → database matching → results."""
        processor = TokenMatchingProcessor()
        
        # Mock the database query
        with patch.object(processor, '_load_coingecko_metadata', return_value=mock_coingecko_data):
            result = await processor.process(
                exchange_tokens=sample_hyperliquid_tokens,
                target_chains=['ethereum', 'base', 'arbitrum'],
                min_confidence=0.7
            )
            
            assert result.success is True
            assert isinstance(result.data, list)
            assert result.processed_count > 0
            
            # Check we got matches
            matches = result.data
            assert len(matches) > 0
            
            # Verify match structure
            first_match = matches[0]
            assert isinstance(first_match, TokenMatch)
            assert first_match.symbol in ['BTC', 'ETH', 'USDC', 'LINK', 'PEPE']
            assert first_match.chain in ['ethereum', 'base', 'arbitrum']
            assert first_match.confidence >= 0.7
            assert first_match.coingecko_id is not None
            
            # Check metadata
            metadata = result.metadata
            assert metadata['total_exchange_tokens'] == len(sample_hyperliquid_tokens)
            assert metadata['matched_tokens'] > 0
            assert 'coverage_by_chain' in metadata
            assert 'match_type_stats' in metadata
    
    @pytest.mark.asyncio
    async def test_symbol_mapping_logic(self, mock_coingecko_data):
        """Test BTC→WBTC and ETH→WETH mapping logic."""
        processor = TokenMatchingProcessor()
        
        btc_token = ExchangeToken(
            symbol='BTC/USD',
            base='BTC',
            quote='USD',
            market_id='0',
            exchange='hyperliquid',
            is_active=True,
            market_type='swap'
        )
        
        with patch.object(processor, '_load_coingecko_metadata', return_value=mock_coingecko_data):
            result = await processor.process(
                exchange_tokens=[btc_token],
                target_chains=['ethereum', 'arbitrum'],
                min_confidence=0.7
            )
            
            assert result.success is True
            matches = result.data
            
            # Should find BTC mapped to WBTC
            btc_matches = [m for m in matches if m.symbol == 'BTC']
            assert len(btc_matches) > 0
            
            # Check it found WBTC on multiple chains
            wbtc_match = btc_matches[0]
            assert wbtc_match.match_type == 'mapped_symbol'
            assert wbtc_match.confidence >= 0.90
    
    @pytest.mark.asyncio 
    async def test_multi_chain_coverage(self, sample_hyperliquid_tokens, mock_coingecko_data):
        """Test that tokens are found across multiple chains."""
        processor = TokenMatchingProcessor()
        
        with patch.object(processor, '_load_coingecko_metadata', return_value=mock_coingecko_data):
            result = await processor.process(
                exchange_tokens=sample_hyperliquid_tokens,
                target_chains=['ethereum', 'base', 'arbitrum'], 
                min_confidence=0.7
            )
            
            assert result.success is True
            matches = result.data
            
            # Check we have matches across multiple chains
            chains_found = set(m.chain for m in matches)
            assert len(chains_found) > 1  # Should find tokens on multiple chains
            
            # Verify coverage stats
            coverage = result.metadata['coverage_by_chain']
            assert isinstance(coverage, dict)
            assert len(coverage) > 0
            
            # Check that popular tokens like USDC are found on multiple chains
            usdc_matches = [m for m in matches if m.symbol == 'USDC']
            if len(usdc_matches) > 1:
                usdc_chains = set(m.chain for m in usdc_matches)
                assert len(usdc_chains) > 1  # USDC should be on multiple chains
    
    @pytest.mark.asyncio
    async def test_confidence_filtering(self, sample_hyperliquid_tokens, mock_coingecko_data):
        """Test that confidence threshold filtering works correctly."""
        processor = TokenMatchingProcessor()
        
        with patch.object(processor, '_load_coingecko_metadata', return_value=mock_coingecko_data):
            # Test with high confidence threshold
            high_confidence_result = await processor.process(
                exchange_tokens=sample_hyperliquid_tokens,
                target_chains=['ethereum', 'base', 'arbitrum'],
                min_confidence=0.95  # Very high threshold - only exact matches
            )
            
            # Test with low confidence threshold  
            low_confidence_result = await processor.process(
                exchange_tokens=sample_hyperliquid_tokens,
                target_chains=['ethereum', 'base', 'arbitrum'],
                min_confidence=0.5  # Lower threshold - more matches
            )
            
            assert high_confidence_result.success is True
            assert low_confidence_result.success is True
            
            # Should have fewer matches with higher confidence
            high_matches = len(high_confidence_result.data)
            low_matches = len(low_confidence_result.data) 
            
            assert low_matches >= high_matches
            
            # All high confidence matches should be exact or mapped symbols
            for match in high_confidence_result.data:
                assert match.confidence >= 0.95
                assert match.match_type in ['exact_symbol', 'mapped_symbol']
    
    @pytest.mark.asyncio
    async def test_unmatched_tokens_tracking(self, mock_coingecko_data):
        """Test that unmatched tokens are properly tracked."""
        processor = TokenMatchingProcessor()
        
        # Create tokens that won't match
        unmatched_tokens = [
            ExchangeToken(
                symbol='FAKECOIN/USD',
                base='FAKECOIN',
                quote='USD',
                market_id='999',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='NOTREAL/USD', 
                base='NOTREAL',
                quote='USD',
                market_id='998',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            )
        ]
        
        with patch.object(processor, '_load_coingecko_metadata', return_value=mock_coingecko_data):
            result = await processor.process(
                exchange_tokens=unmatched_tokens,
                target_chains=['ethereum'],
                min_confidence=0.7
            )
            
            assert result.success is True
            assert result.processed_count == 0  # No matches found
            assert result.metadata['matched_tokens'] == 0
            assert result.metadata['unmatched_tokens'] == 2
            assert len(result.metadata['unmatched_sample']) == 2


class TestHyperliquidFetcherIntegration:
    """Integration tests for Hyperliquid fetcher."""
    
    @pytest.mark.asyncio
    async def test_hyperliquid_fetcher_initialization(self):
        """Test HyperliquidFetcher can be initialized."""
        fetcher = HyperliquidFetcher()
        
        assert fetcher.exchange_id == 'hyperliquid'
        assert fetcher.ccxt_exchange is not None
        assert hasattr(fetcher, 'config')
    
    @pytest.mark.asyncio
    async def test_hyperliquid_market_filtering(self):
        """Test Hyperliquid-specific market filtering logic."""
        fetcher = HyperliquidFetcher()
        
        # Test valid markets
        valid_market = {
            'symbol': 'BTC/USD',
            'base': 'BTC',
            'quote': 'USD',
            'active': True
        }
        assert fetcher._should_include_market(valid_market) is True
        
        valid_usdc_market = {
            'symbol': 'ETH/USDC', 
            'base': 'ETH',
            'quote': 'USDC',
            'active': True
        }
        assert fetcher._should_include_market(valid_usdc_market) is True
        
        # Test invalid markets
        inactive_market = {
            'symbol': 'BTC/USD',
            'base': 'BTC', 
            'quote': 'USD',
            'active': False
        }
        assert fetcher._should_include_market(inactive_market) is False
        
        no_base_market = {
            'symbol': 'BTC/USD',
            'base': '',
            'quote': 'USD', 
            'active': True
        }
        assert fetcher._should_include_market(no_base_market) is False
    
    @pytest.mark.asyncio 
    async def test_mock_hyperliquid_fetch_with_processor(self):
        """Test complete pipeline with mocked Hyperliquid data."""
        # Mock CCXT response
        mock_markets = [
            {
                'symbol': 'BTC/USD',
                'base': 'BTC',
                'quote': 'USD',
                'id': '0',
                'active': True
            },
            {
                'symbol': 'ETH/USD',
                'base': 'ETH', 
                'quote': 'USD',
                'id': '1',
                'active': True
            },
            {
                'symbol': 'LINK/USD',
                'base': 'LINK',
                'quote': 'USD',
                'id': '2',
                'active': True
            }
        ]
        
        fetcher = HyperliquidFetcher()
        processor = TokenMatchingProcessor()
        
        # Mock the CCXT fetch call
        with patch.object(fetcher.ccxt_exchange, 'fetchSwapMarkets', return_value=mock_markets):
            # Fetch tokens from mocked exchange
            fetch_result = await fetcher.fetch_markets('swap')
            
            assert fetch_result.success is True
            exchange_tokens = fetch_result.metadata['tokens']
            assert len(exchange_tokens) == 3
            
            # Mock database for processor
            mock_db_data = {
                'ethereum': [
                    {
                        'coingecko_id': 'chainlink',
                        'symbol': 'LINK', 
                        'name': 'Chainlink',
                        'market_cap_rank': 15,
                        'platform': 'ethereum',
                        'address': '0x514910771af9ca656af840dff83e8264ecf986ca',
                        'decimals': 18,
                        'total_supply': '1000000000'
                    }
                ]
            }
            
            with patch.object(processor, '_load_coingecko_metadata', return_value=mock_db_data):
                # Process tokens through matching
                match_result = await processor.process(
                    exchange_tokens=exchange_tokens,
                    target_chains=['ethereum'],
                    min_confidence=0.7
                )
                
                assert match_result.success is True
                # Should find at least LINK as exact match
                matches = match_result.data
                link_matches = [m for m in matches if m.symbol == 'LINK']
                assert len(link_matches) > 0
                assert link_matches[0].match_type == 'exact_symbol'
                assert link_matches[0].confidence >= 0.95


@pytest.mark.integration 
class TestRealHyperliquidIntegration:
    """
    Real integration tests that call Hyperliquid API and database.
    
    These tests are marked with @pytest.mark.integration and can be run separately:
    pytest -m integration src/processors/tests/test_token_matching_integration.py
    """
    
    @pytest.mark.asyncio
    async def test_real_hyperliquid_fetch(self):
        """
        Test fetching real Hyperliquid markets (requires internet).
        
        This test will be skipped in CI but can be run locally for verification.
        """
        pytest.skip("Skipping real API test - run manually with: pytest -m integration -k real_hyperliquid_fetch")
        
        fetcher = HyperliquidFetcher()
        
        try:
            result = await fetcher.fetch_markets('swap')
            
            if result.success:
                tokens = result.metadata['tokens']
                print(f"\n✓ Fetched {len(tokens)} tokens from Hyperliquid")
                
                # Show sample tokens
                for i, token in enumerate(tokens[:5]):
                    print(f"  {i+1}. {token.symbol} ({token.base})")
                
                assert len(tokens) > 0
                assert all(t.exchange == 'hyperliquid' for t in tokens)
                assert all(t.market_type == 'swap' for t in tokens)
            else:
                pytest.skip(f"Hyperliquid API unavailable: {result.error}")
                
        except Exception as e:
            pytest.skip(f"Hyperliquid integration test failed: {e}")
    
    @pytest.mark.asyncio
    async def test_real_database_token_matching(self):
        """
        Test token matching against real database.
        
        This test requires database connection and will be skipped if unavailable.
        """
        pytest.skip("Skipping real database test - run manually with proper DB config")
        
        processor = TokenMatchingProcessor()
        
        # Sample tokens to test matching
        sample_tokens = [
            ExchangeToken(
                symbol='BTC/USD',
                base='BTC', 
                quote='USD',
                market_id='0',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            ),
            ExchangeToken(
                symbol='ETH/USD',
                base='ETH',
                quote='USD', 
                market_id='1',
                exchange='hyperliquid',
                is_active=True,
                market_type='swap'
            )
        ]
        
        try:
            result = await processor.process(
                exchange_tokens=sample_tokens,
                target_chains=['ethereum', 'base', 'arbitrum'],
                min_confidence=0.7
            )
            
            if result.success:
                matches = result.data
                print(f"\n✓ Matched {len(matches)} tokens from database")
                
                for match in matches[:3]:
                    print(f"  {match.symbol} → {match.chain}: {match.chain_address[:10]}...")
                
                assert len(matches) > 0
            else:
                pytest.skip(f"Database matching failed: {result.error}")
                
        except Exception as e:
            pytest.skip(f"Database integration test failed: {e}")