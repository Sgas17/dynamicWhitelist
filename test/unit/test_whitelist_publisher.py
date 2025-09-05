"""
Unit tests for NATS whitelist publisher functionality.
"""

from unittest.mock import AsyncMock, patch
from src.utils.nats.whitelist_publisher import WhitelistPublisher


class TestWhitelistPublisher:
    """Test cases for the WhitelistPublisher class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.publisher = WhitelistPublisher("local")
    
    def test_init(self):
        """Test publisher initialization."""
        assert self.publisher.stream_name == "WHITELIST_UPDATES"
        assert "whitelist.tokens.hyperliquid" in self.publisher.subjects
        assert "whitelist.tokens.base" in self.publisher.subjects
        assert "whitelist.tokens.ethereum" in self.publisher.subjects
        assert "whitelist.pools.uniswap_v3" in self.publisher.subjects
        assert "whitelist.pools.uniswap_v4" in self.publisher.subjects
        assert self.publisher.nats_client is not None
    
    @patch.object(WhitelistPublisher, 'aconnect')
    def test_connect_sync(self, mock_aconnect):
        """Test synchronous connection wrapper."""
        with patch('asyncio.run') as mock_run:
            self.publisher.connect()
            mock_run.assert_called_once()
    
    @patch.object(WhitelistPublisher, 'aclose')
    def test_close_sync(self, mock_aclose):
        """Test synchronous close wrapper."""
        with patch('asyncio.run') as mock_run:
            self.publisher.close()
            mock_run.assert_called_once()
    
    async def test_aconnect(self):
        """Test async connection and stream setup."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        await self.publisher.aconnect()
        
        mock_nats_client.aconnect.assert_called_once()
        mock_nats_client.aregister_new_stream.assert_called_once_with(
            "WHITELIST_UPDATES",
            self.publisher.subjects
        )
    
    async def test_aclose(self):
        """Test async connection close."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        await self.publisher.aclose()
        
        mock_nats_client.aclose.assert_called_once()
    
    async def test_apublish_hyperliquid_tokens(self):
        """Test async publishing of Hyperliquid tokens."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        test_tokens = {"BTC": 50000.0, "ETH": 3000.0}
        
        with patch('asyncio.get_event_loop') as mock_loop:
            mock_loop.return_value.time.return_value = 1234567890.0
            
            await self.publisher.apublish_hyperliquid_tokens(test_tokens)
        
        # Verify the call was made with correct subject
        mock_nats_client.apublish.assert_called_once()
        call_args = mock_nats_client.apublish.call_args
        assert call_args[0][0] == "whitelist.tokens.hyperliquid"
        
        # Verify message structure
        message = call_args[0][1]
        assert message["type"] == "hyperliquid_tokens_update"
        assert message["data"] == test_tokens
        assert "timestamp" in message
    
    def test_publish_hyperliquid_tokens_sync(self):
        """Test synchronous publishing of Hyperliquid tokens."""
        test_tokens = {"BTC": 50000.0, "ETH": 3000.0}
        
        with patch.object(self.publisher, 'apublish_hyperliquid_tokens'):
            with patch('asyncio.run') as mock_run:
                self.publisher.publish_hyperliquid_tokens(test_tokens)
                mock_run.assert_called_once()
    
    async def test_apublish_token_update(self):
        """Test async publishing of general token updates."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        test_tokens = {"USDC": 1.0, "DAI": 0.998}
        test_chain = "ethereum"
        
        with patch('asyncio.get_event_loop') as mock_loop:
            mock_loop.return_value.time.return_value = 1234567890.0
            
            await self.publisher.apublish_token_update(test_chain, test_tokens)
        
        # Verify the call was made with correct subject
        mock_nats_client.apublish.assert_called_once()
        call_args = mock_nats_client.apublish.call_args
        assert call_args[0][0] == f"whitelist.tokens.{test_chain}"
        
        # Verify message structure
        message = call_args[0][1]
        assert message["type"] == f"{test_chain}_tokens_update"
        assert message["chain"] == test_chain
        assert message["data"] == test_tokens
        assert "timestamp" in message
    
    async def test_apublish_pool_update(self):
        """Test async publishing of pool updates."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        test_pools = {"pool1": {"token0": "USDC", "token1": "ETH", "fee": 0.0005}}
        test_protocol = "uniswap_v3"
        
        with patch('asyncio.get_event_loop') as mock_loop:
            mock_loop.return_value.time.return_value = 1234567890.0
            
            await self.publisher.apublish_pool_update(test_protocol, test_pools)
        
        # Verify the call was made with correct subject
        mock_nats_client.apublish.assert_called_once()
        call_args = mock_nats_client.apublish.call_args
        assert call_args[0][0] == f"whitelist.pools.{test_protocol}"
        
        # Verify message structure
        message = call_args[0][1]
        assert message["type"] == f"{test_protocol}_pools_update"
        assert message["protocol"] == test_protocol
        assert message["data"] == test_pools
        assert "timestamp" in message
    
    async def test_apublish_whitelist_status(self):
        """Test async publishing of whitelist status."""
        mock_nats_client = AsyncMock()
        self.publisher.nats_client = mock_nats_client
        
        test_status = {
            "total_tokens": 100,
            "last_updated": "2024-01-01T00:00:00Z",
            "healthy": True
        }
        
        with patch('asyncio.get_event_loop') as mock_loop:
            mock_loop.return_value.time.return_value = 1234567890.0
            
            await self.publisher.apublish_whitelist_status(test_status)
        
        # Verify the call was made with correct subject
        mock_nats_client.apublish.assert_called_once()
        call_args = mock_nats_client.apublish.call_args
        assert call_args[0][0] == "whitelist.status"
        
        # Verify message structure
        message = call_args[0][1]
        assert message["type"] == "whitelist_status"
        assert message["data"] == test_status
        assert "timestamp" in message
    
    def test_publish_token_update_sync(self):
        """Test synchronous wrapper for token updates."""
        test_tokens = {"USDC": 1.0}
        test_chain = "base"
        
        with patch.object(self.publisher, 'apublish_token_update'):
            with patch('asyncio.run') as mock_run:
                self.publisher.publish_token_update(test_chain, test_tokens)
                mock_run.assert_called_once()
    
    def test_publish_pool_update_sync(self):
        """Test synchronous wrapper for pool updates."""
        test_pools = {"pool1": {"data": "test"}}
        test_protocol = "uniswap_v4"
        
        with patch.object(self.publisher, 'apublish_pool_update'):
            with patch('asyncio.run') as mock_run:
                self.publisher.publish_pool_update(test_protocol, test_pools)
                mock_run.assert_called_once()
    
    def test_publish_whitelist_status_sync(self):
        """Test synchronous wrapper for status updates."""
        test_status = {"healthy": True}
        
        with patch.object(self.publisher, 'apublish_whitelist_status'):
            with patch('asyncio.run') as mock_run:
                self.publisher.publish_whitelist_status(test_status)
                mock_run.assert_called_once()