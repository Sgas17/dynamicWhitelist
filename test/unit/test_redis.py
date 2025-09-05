import pytest
from redis import Redis
from src.whitelist.build_whitelist import Whitelist
import time



def test_whitelist_redis_connection(whitelist):
    try:
        whitelist.r.ping()
        assert True
    except Exception as e:
        pytest.fail("Could not connect to redis")

def test_whitelist_redis_set_and_get(whitelist:Whitelist):
    whitelist.r.set("test", "test")
    assert whitelist.r.get("test") == b"test"

def test_whitelist_redis_set_and_get_with_ttl(whitelist:Whitelist):
    whitelist.r.set("test", "test", ex=10)
    assert whitelist.r.get("test") == b"test"

def test_whitelist_get_redis_with_hyperliquid_tokens(whitelist:Whitelist):
    price = whitelist.r.get("whitelist.hyperliquid_tokens:btc")
    assert price != None
    print("price of btc", price)



