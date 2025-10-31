"""
Unit tests for exchange market fetchers.

Tests use mocks and don't make real API calls.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock

from ....fetchers.exchange_fetchers import (
    BaseExchangeFetcher, 
    HyperliquidFetcher, 
    BinanceFetcher,
    ExchangeToken
)
from ....fetchers.base import FetchResult, FetchError
from src.config.manager import ConfigManager


class TestBaseExchangeFetcher:
    """Test base exchange fetcher functionality."""
    
    def test_init_with_valid_exchange(self):
        """Test fetcher initialization with valid exchange."""
        with patch('ccxt.binance') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = BaseExchangeFetcher('binance')
            
            assert fetcher.exchange_id == 'binance'
            assert fetcher.ccxt_exchange is not None
            mock_exchange.assert_called_once()
    
    def test_init_with_invalid_exchange(self):
        """Test fetcher initialization with invalid exchange."""
        with pytest.raises(FetchError, match="Unsupported exchange"):
            BaseExchangeFetcher('nonexistent_exchange')
    
    def test_create_exchange_token(self):
        """Test exchange token creation from market data."""
        with patch('ccxt.binance') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = BaseExchangeFetcher('binance')
            
            market_data = {
                'symbol': 'BTC/USDT',
                'base': 'BTC',
                'quote': 'USDT',
                'id': 'btcusdt',
                'active': True
            }
            
            token = fetcher._create_exchange_token(market_data, 'spot')
            
            assert isinstance(token, ExchangeToken)
            assert token.symbol == 'BTC/USDT'
            assert token.base == 'BTC'
            assert token.quote == 'USDT'
            assert token.exchange == 'binance'
            assert token.market_type == 'spot'
            assert token.is_active is True
    
    def test_should_include_market_default(self):
        """Test default market inclusion logic."""
        with patch('ccxt.binance') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = BaseExchangeFetcher('binance')
            
            # Active market with valid base
            assert fetcher._should_include_market({
                'active': True,
                'base': 'BTC'
            }) is True
            
            # Inactive market
            assert fetcher._should_include_market({
                'active': False,
                'base': 'BTC'
            }) is False
            
            # No base asset
            assert fetcher._should_include_market({
                'active': True,
                'base': ''
            }) is False
    
    @pytest.mark.asyncio
    async def test_fetch_markets_success(self):
        """Test successful market fetching."""
        mock_markets = [
            {
                'symbol': 'BTC/USD',
                'base': 'BTC',
                'quote': 'USD',
                'id': 'btcusd',
                'active': True
            },
            {
                'symbol': 'ETH/USD',
                'base': 'ETH', 
                'quote': 'USD',
                'id': 'ethusd',
                'active': True
            }
        ]
        
        with patch('ccxt.hyperliquid') as mock_exchange_class:
            mock_exchange = Mock()
            mock_exchange.fetchSwapMarkets.return_value = mock_markets
            mock_exchange_class.return_value = mock_exchange
            
            fetcher = BaseExchangeFetcher('hyperliquid')
            result = await fetcher.fetch_markets('swap')
            
            assert result.success is True
            assert result.metadata['total_markets'] == 2
            assert result.metadata['filtered_tokens'] == 2
            assert len(result.metadata['tokens']) == 2
    
    @pytest.mark.asyncio
    async def test_fetch_markets_empty_response(self):
        """Test market fetching with empty response."""
        with patch('ccxt.hyperliquid') as mock_exchange_class:
            mock_exchange = Mock()
            mock_exchange.fetchSwapMarkets.return_value = []
            mock_exchange_class.return_value = mock_exchange
            
            fetcher = BaseExchangeFetcher('hyperliquid')
            result = await fetcher.fetch_markets('swap')
            
            assert result.success is False
            assert "No swap markets returned" in result.error
    
    @pytest.mark.asyncio
    async def test_fetch_markets_exception(self):
        """Test market fetching with exception."""
        with patch('ccxt.hyperliquid') as mock_exchange_class:
            mock_exchange = Mock()
            mock_exchange.fetchSwapMarkets.side_effect = Exception("API Error")
            mock_exchange_class.return_value = mock_exchange
            
            fetcher = BaseExchangeFetcher('hyperliquid')
            result = await fetcher.fetch_markets('swap')
            
            assert result.success is False
            assert "Failed to fetch hyperliquid markets" in result.error


class TestHyperliquidFetcher:
    """Test Hyperliquid-specific fetcher."""
    
    def test_init(self):
        """Test Hyperliquid fetcher initialization."""
        with patch('ccxt.hyperliquid') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = HyperliquidFetcher()
            
            assert fetcher.exchange_id == 'hyperliquid'
            assert isinstance(fetcher.config, ConfigManager)
    
    def test_should_include_market_hyperliquid_logic(self):
        """Test Hyperliquid-specific market inclusion logic."""
        with patch('ccxt.hyperliquid') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = HyperliquidFetcher()
            
            # Valid USD quote
            assert fetcher._should_include_market({
                'active': True,
                'base': 'BTC',
                'quote': 'USD'
            }) is True
            
            # Valid USDC quote
            assert fetcher._should_include_market({
                'active': True,
                'base': 'ETH',
                'quote': 'USDC'
            }) is True
            
            # Inactive market
            assert fetcher._should_include_market({
                'active': False,
                'base': 'BTC',
                'quote': 'USD'
            }) is False
    
    @pytest.mark.asyncio
    async def test_fetch_hyperliquid_markets(self):
        """Test fetching Hyperliquid swap markets."""
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
            }
        ]
        
        with patch('ccxt.hyperliquid') as mock_exchange_class:
            mock_exchange = Mock()
            mock_exchange.fetchSwapMarkets.return_value = mock_markets
            mock_exchange_class.return_value = mock_exchange
            
            fetcher = HyperliquidFetcher()
            result = await fetcher.fetch_markets('swap')
            
            assert result.success is True
            assert result.metadata['exchange'] == 'hyperliquid'
            assert result.metadata['market_type'] == 'swap'
            assert len(result.metadata['tokens']) == 2
            
            # Check token details
            tokens = result.metadata['tokens']
            assert tokens[0].base == 'BTC'
            assert tokens[0].exchange == 'hyperliquid'
            assert tokens[0].market_type == 'swap'


class TestBinanceFetcher:
    """Test Binance fetcher example."""
    
    def test_init(self):
        """Test Binance fetcher initialization."""
        with patch('ccxt.binance') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = BinanceFetcher()
            
            assert fetcher.exchange_id == 'binance'
    
    def test_should_include_market_binance_logic(self):
        """Test Binance-specific market inclusion logic."""
        with patch('ccxt.binance') as mock_exchange:
            mock_exchange.return_value = Mock()
            
            fetcher = BinanceFetcher()
            
            # Valid USDT quote
            assert fetcher._should_include_market({
                'active': True,
                'base': 'BTC',
                'quote': 'USDT'
            }) is True
            
            # Invalid quote
            assert fetcher._should_include_market({
                'active': True,
                'base': 'BTC',
                'quote': 'RANDOM'
            }) is False
    
    @pytest.mark.asyncio
    async def test_fetch_spot_markets(self):
        """Test fetching Binance spot markets."""
        mock_markets = [
            {
                'symbol': 'BTC/USDT',
                'base': 'BTC',
                'quote': 'USDT',
                'id': 'BTCUSDT',
                'active': True,
                'spot': True
            },
            {
                'symbol': 'ETH/USDT',
                'base': 'ETH',
                'quote': 'USDT',
                'id': 'ETHUSDT', 
                'active': True,
                'spot': True
            }
        ]
        
        with patch('ccxt.binance') as mock_exchange_class:
            mock_exchange = Mock()
            mock_exchange.fetchMarkets.return_value = mock_markets
            mock_exchange_class.return_value = mock_exchange
            
            fetcher = BinanceFetcher()
            result = await fetcher.fetch_markets('spot')
            
            assert result.success is True
            assert result.metadata['exchange'] == 'binance'
            assert result.metadata['market_type'] == 'spot'


class TestExchangeToken:
    """Test ExchangeToken dataclass."""
    
    def test_token_creation(self):
        """Test creating exchange token."""
        token = ExchangeToken(
            symbol='BTC/USDT',
            base='BTC',
            quote='USDT',
            market_id='btcusdt',
            exchange='binance',
            is_active=True,
            market_type='spot'
        )
        
        assert token.symbol == 'BTC/USDT'
        assert token.base == 'BTC'
        assert token.quote == 'USDT'
        assert token.market_id == 'btcusdt'
        assert token.exchange == 'binance'
        assert token.is_active is True
        assert token.market_type == 'spot'
        assert token.additional_data is None
    
    def test_token_with_additional_data(self):
        """Test creating token with additional data."""
        additional = {'volume': 1000, 'last_price': 50000}
        
        token = ExchangeToken(
            symbol='BTC/USDT',
            base='BTC', 
            quote='USDT',
            market_id='btcusdt',
            exchange='binance',
            is_active=True,
            market_type='spot',
            additional_data=additional
        )
        
        assert token.additional_data == additional
        assert token.additional_data['volume'] == 1000