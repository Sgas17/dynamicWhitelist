import redis
import nats
from src.utils.hyperliquid_helpers import get_hyperliquid_symbols_and_prices


class Whitelist:
    """
    This is the main class that creates a whitelist 
    """

    def __init__(self):
        self.r = redis.Redis()
        self.hyperliquid_tokens = get_hyperliquid_symbols_and_prices()
        self._store_hyperliquid_tokens_in_redis()

    def _store_hyperliquid_tokens_in_redis(self):
        """Store hyperliquid tokens and prices in Redis with the key format whitelist.hyperliquid_tokens:{symbol}"""
        for symbol, price in self.hyperliquid_tokens.items():
            if price is not None:
                key = f"whitelist.hyperliquid_tokens:{symbol}"
                self.r.set(key, str(price))