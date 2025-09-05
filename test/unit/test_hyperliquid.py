from src.whitelist.build_whitelist import Whitelist
from unittest.mock import Mock, patch


def test_whitelist_can_get_hyperliquid_tokens(whitelist:Whitelist):
    assert len(whitelist.hyperliquid_tokens) > 0
    print("first 5 tokens in whitelist.hyperliquid_tokens", list(whitelist.hyperliquid_tokens.keys())[:5])

def test_whitelist_can_get_hyperliquid_prices(whitelist:Whitelist):
    price = whitelist.hyperliquid_tokens['btc'] 
    assert price != 0 
    print("price of btc", price)


def test_whitelist_with_nats_disabled():
    """Test whitelist initialization with NATS disabled"""
    whitelist = Whitelist(enable_nats=False)
    assert whitelist.enable_nats is False
    assert not hasattr(whitelist, 'nats_publisher')
    assert len(whitelist.hyperliquid_tokens) > 0


@patch('src.whitelist.build_whitelist.WhitelistPublisher')
def test_whitelist_with_nats_enabled(mock_publisher_class):
    """Test whitelist initialization with NATS enabled"""
    mock_publisher = Mock()
    mock_publisher_class.return_value = mock_publisher
    
    whitelist = Whitelist(env="local", enable_nats=True)
    
    # Verify NATS publisher was created and connected
    mock_publisher_class.assert_called_once_with("local")
    mock_publisher.connect.assert_called_once()
    mock_publisher.publish_hyperliquid_tokens.assert_called_once()
    
    assert whitelist.enable_nats is True
    assert hasattr(whitelist, 'nats_publisher')


@patch('src.whitelist.build_whitelist.WhitelistPublisher')  
def test_whitelist_update_method(mock_publisher_class):
    """Test whitelist update method publishes to NATS"""
    mock_publisher = Mock()
    mock_publisher_class.return_value = mock_publisher
    
    whitelist = Whitelist(env="local", enable_nats=True)
    
    # Reset call count after initialization
    mock_publisher.publish_hyperliquid_tokens.reset_mock()
    
    # Call update
    whitelist.update_whitelist()
    
    # Verify NATS publishing was called again
    mock_publisher.publish_hyperliquid_tokens.assert_called_once()


@patch('src.whitelist.build_whitelist.WhitelistPublisher')
def test_whitelist_close_method(mock_publisher_class):
    """Test whitelist close method closes NATS connection"""
    mock_publisher = Mock()
    mock_publisher_class.return_value = mock_publisher
    
    whitelist = Whitelist(env="local", enable_nats=True)
    whitelist.close()
    
    # Verify NATS connection was closed
    mock_publisher.close.assert_called_once()
    