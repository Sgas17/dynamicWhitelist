import redis
from src.utils.hyperliquid_helpers import get_hyperliquid_symbols_and_prices
from src.utils.nats import WhitelistPublisher


class Whitelist:
    """
    This is the main class that creates a whitelist 
    """

    def __init__(self, env: str = "local", enable_nats: bool = True):
        self.r = redis.Redis()
        self.enable_nats = enable_nats
        if self.enable_nats:
            self.nats_publisher = WhitelistPublisher(env)
            self.nats_publisher.connect()
        
        self.hyperliquid_tokens = get_hyperliquid_symbols_and_prices()
        self._store_hyperliquid_tokens_in_redis()
        
        if self.enable_nats:
            self._publish_hyperliquid_tokens_to_nats()

    def _store_hyperliquid_tokens_in_redis(self):
        """Store hyperliquid tokens and prices in Redis with the key format whitelist.hyperliquid_tokens:{symbol}"""
        for symbol, price in self.hyperliquid_tokens.items():
            if price is not None:
                key = f"whitelist.hyperliquid_tokens:{symbol}"
                self.r.set(key, str(price))
    
    def _publish_hyperliquid_tokens_to_nats(self):
        """Publish hyperliquid tokens and prices to NATS"""
        if self.enable_nats and hasattr(self, 'nats_publisher'):
            # Filter out None prices for NATS publishing
            valid_tokens = {symbol: price for symbol, price in self.hyperliquid_tokens.items() if price is not None}
            self.nats_publisher.publish_hyperliquid_tokens(valid_tokens)
    
    def update_whitelist(self):
        """Update the whitelist with fresh data"""
        self.hyperliquid_tokens = get_hyperliquid_symbols_and_prices()
        self._store_hyperliquid_tokens_in_redis()
        
        if self.enable_nats:
            self._publish_hyperliquid_tokens_to_nats()
    
    def close(self):
        """Clean up connections"""
        if self.enable_nats and hasattr(self, 'nats_publisher'):
            self.nats_publisher.close()