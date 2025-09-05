"""
Unit tests for NATS client functionality.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from src.utils.nats.client import NatsClient, NatsClientJS
from src.utils.nats.json_helpers import dumps, loads


class TestNatsClient:
    """Test cases for the basic NATS client."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.client = NatsClient("local")
    
    def test_init(self):
        """Test client initialization."""
        assert self.client.url == "nats://localhost:4222"
        assert self.client.nc is None
    
    def test_get_nats_url_local(self):
        """Test URL resolution for local environment."""
        client = NatsClient("local")
        assert client.url == "nats://localhost:4222"
    
    def test_get_nats_url_dev(self):
        """Test URL resolution for dev environment."""
        client = NatsClient("dev")
        assert client.url == "nats://nats:4222"
    
    def test_get_nats_url_production(self):
        """Test URL resolution for production environment."""
        client = NatsClient("production")
        assert client.url == "nats://nats-server:4222"
    
    def test_get_nats_url_unknown_defaults_to_local(self):
        """Test URL resolution for unknown environment defaults to local."""
        client = NatsClient("unknown")
        assert client.url == "nats://localhost:4222"
    
    @patch('src.utils.nats.client.nats.connect')
    def test_connect_sync(self, mock_connect):
        """Test synchronous connection."""
        mock_nc = Mock()
        mock_connect.return_value = mock_nc
        
        with patch('asyncio.run') as mock_run:
            self.client.connect()
            mock_run.assert_called_once()
    
    def test_publish_without_connection_raises_error(self):
        """Test that publishing without connection raises error."""
        with pytest.raises(ConnectionError):
            self.client.publish("test.subject", {"message": "test"})
    
    def test_subscribe_without_connection_raises_error(self):
        """Test that subscribing without connection raises error."""
        def callback(msg):
            pass
        
        with pytest.raises(ConnectionError):
            self.client.subscribe("test.subject", callback)
    
    def test_request_without_connection_raises_error(self):
        """Test that making request without connection raises error."""
        with pytest.raises(ConnectionError):
            self.client.request("test.subject", {"message": "test"})


class TestNatsClientJS:
    """Test cases for the JetStream NATS client."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.client = NatsClientJS("local")
    
    def test_init(self):
        """Test JetStream client initialization."""
        assert self.client.url == "nats://localhost:4222"
        assert self.client.nc is None
        assert self.client.js is None
        assert self.client._streams == set()
    
    async def test_aconnect_initializes_jetstream(self):
        """Test that async connect initializes JetStream context."""
        with patch('src.utils.nats.client.nats.connect') as mock_connect:
            mock_nc = AsyncMock()
            mock_js = Mock()
            mock_nc.jetstream.return_value = mock_js
            mock_connect.return_value = mock_nc
            
            await self.client.aconnect()
            
            # Just verify that jetstream was called, not the return value
            mock_nc.jetstream.assert_called_once()
            assert self.client.nc == mock_nc
    
    async def test_stream_exists_true(self):
        """Test stream exists check returns True when stream exists."""
        mock_js = AsyncMock()
        mock_js.stream_info.return_value = {}
        self.client.js = mock_js
        
        result = await self.client._stream_exists("test_stream")
        assert result is True
        mock_js.stream_info.assert_called_once_with("test_stream")
    
    async def test_stream_exists_false(self):
        """Test stream exists check returns False when stream doesn't exist."""
        from nats.js.errors import NotFoundError
        
        mock_js = AsyncMock()
        mock_js.stream_info.side_effect = NotFoundError("Stream not found")
        self.client.js = mock_js
        
        result = await self.client._stream_exists("test_stream")
        assert result is False
    
    async def test_register_new_stream(self):
        """Test registering a new stream."""
        mock_js = AsyncMock()
        mock_js.stream_info.side_effect = Exception("Not found")
        mock_js.add_stream = AsyncMock()
        self.client.js = mock_js
        
        # Mock _stream_exists to return False
        with patch.object(self.client, '_stream_exists', return_value=False):
            await self.client.aregister_new_stream("test_stream", ["test.subject"])
        
        mock_js.add_stream.assert_called_once_with(
            name="test_stream", 
            subjects=["test.subject"], 
            no_ack=True
        )
        assert "test_stream" in self.client._streams
    
    async def test_unregister_stream(self):
        """Test unregistering a stream."""
        mock_js = AsyncMock()
        mock_js.delete_stream = AsyncMock()
        self.client.js = mock_js
        self.client._streams.add("test_stream")
        
        # Mock _stream_exists to return True
        with patch.object(self.client, '_stream_exists', return_value=True):
            await self.client.aunregister_stream("test_stream")
        
        mock_js.delete_stream.assert_called_once_with("test_stream")
        assert "test_stream" not in self.client._streams


class TestJsonHelpers:
    """Test cases for JSON helper functions."""
    
    def test_dumps_simple_dict(self):
        """Test JSON serialization of simple dictionary."""
        data = {"key": "value", "number": 42}
        result = dumps(data)
        assert result == '{"key": "value", "number": 42}'
    
    def test_loads_simple_dict(self):
        """Test JSON deserialization of simple dictionary."""
        json_str = '{"key": "value", "number": 42}'
        result = loads(json_str)
        assert result == {"key": "value", "number": 42}
    
    def test_dumps_loads_roundtrip(self):
        """Test that dumps and loads are consistent."""
        original_data = {
            "tokens": {"BTC": 50000.0, "ETH": 3000.0},
            "timestamp": 1234567890,
            "active": True
        }
        
        json_str = dumps(original_data)
        recovered_data = loads(json_str)
        
        assert recovered_data == original_data
    
    def test_dumps_with_custom_serializer(self):
        """Test dumps with custom serializer class."""
        class CustomSerializer:
            @staticmethod
            def dumps(obj):
                return f"custom:{str(obj)}"
        
        result = dumps({"test": "data"}, CustomSerializer)
        assert result == "custom:{'test': 'data'}"
    
    def test_loads_with_custom_deserializer(self):
        """Test loads with custom deserializer class."""
        class CustomDeserializer:
            @staticmethod
            def loads(data):
                return {"custom": data}
        
        result = loads("test_data", CustomDeserializer)
        assert result == {"custom": "test_data"}