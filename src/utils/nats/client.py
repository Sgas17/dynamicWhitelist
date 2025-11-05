import asyncio
import functools
import json
from typing import Any, Callable, Optional, TypeVar

import nats
from nats.aio.client import Client as NATS
from nats.aio.subscription import Msg, Subscription
from nats.errors import NoRespondersError
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

T = TypeVar("T")

DEFAULT_TIMEOUT = 30.0
DEFAULT_NATS_URLS = {
    "local": "nats://localhost:4222",
    "dev": "nats://nats:4222",
    "production": "nats://nats-server:4222",
}


def _get_nats_url(env: str) -> str:
    """Get NATS URL for the specified environment"""
    return DEFAULT_NATS_URLS.get(env, DEFAULT_NATS_URLS["local"])


def dumps(msg: Any) -> str:
    """Serialize message to JSON string"""
    return json.dumps(msg)


def loads(data: str) -> Any:
    """Deserialize JSON string to Python object"""
    return json.loads(data)


class NatsClient:
    """
    A simple NATS client for JSON-encoded messages.
    Methods starting with 'a' execute asynchronously.
    """

    def __init__(self, env: str = "local"):
        self.url = _get_nats_url(env)
        self.nc: Optional[NATS] = None

    # Connection methods
    async def aconnect(self):
        """Asynchronously connect to NATS server"""
        print(f"Connecting to NATS at {self.url}")
        self.nc = await nats.connect(servers=[self.url])
        print(f"Connected to NATS at {self.url}")

    def connect(self):
        """Connect to NATS server (synchronous wrapper)"""
        asyncio.run(asyncio.wait_for(self.aconnect(), timeout=DEFAULT_TIMEOUT))

    async def aclose(self):
        """Asynchronously close connection to NATS server"""
        if self.nc:
            await self.nc.close()

    def close(self):
        """Close connection to NATS server (synchronous wrapper)"""
        asyncio.run(self.aclose())

    # Publishing methods
    async def apublish(self, subject: str, msg: Any):
        """Asynchronously publish a message to a subject"""
        if not self.nc:
            raise ConnectionError("Not connected to NATS server")
        enc_msg = dumps(msg).encode()
        await self.nc.publish(subject, enc_msg)

    def publish(self, subject: str, msg: Any):
        """Publish a message to a subject (synchronous wrapper)"""
        asyncio.run(self.apublish(subject, msg))

    # Subscription methods
    async def asubscribe(self, subject: str, callback_hdlr: Callable[[Any], None]):
        """Asynchronously subscribe to a subject"""
        if not self.nc:
            raise ConnectionError("Not connected to NATS server")
        wrapped_callback = functools.partial(
            self.subscribe_cb_wrapper, callback_hdlr=callback_hdlr
        )
        return await self.nc.subscribe(subject, cb=wrapped_callback)

    def subscribe(self, subject: str, callback_hdlr: Callable[[Any], None]):
        """Subscribe to a subject (synchronous wrapper)"""
        return asyncio.run(self.asubscribe(subject, callback_hdlr))

    # Request-reply methods
    async def arequest(
        self, subject: str, msg: Any, timeout: float = DEFAULT_TIMEOUT
    ) -> Any:
        """Asynchronously send a request and wait for response"""
        if not self.nc:
            raise ConnectionError("Not connected to NATS server")
        enc_msg = dumps(msg).encode()
        try:
            response = await self.nc.request(subject, enc_msg, timeout=timeout)
        except NoRespondersError as e:
            raise NoRespondersError(f"{str(e)} subject: {subject}, message: {msg}")
        return loads(response.data.decode())

    def request(self, subject: str, msg: Any, timeout: float = DEFAULT_TIMEOUT) -> Any:
        """Send a request and wait for response (synchronous wrapper)"""
        return asyncio.run(self.arequest(subject, msg, timeout))

    # Unsubscribe methods
    async def aunsubscribe(self, sub: Subscription):
        """Asynchronously unsubscribe from a subject"""
        await sub.unsubscribe()

    def unsubscribe(self, sub: Subscription):
        """Unsubscribe from a subject (synchronous wrapper)"""
        asyncio.run(self.aunsubscribe(sub))

    # Callback wrappers
    async def subscribe_cb_wrapper(
        self, msg: Msg, callback_hdlr: Callable[[Any], None]
    ):
        """Wrapper for subscription callbacks to handle JSON decoding"""
        decoded_msg = loads(msg.data.decode())
        callback_hdlr(decoded_msg)

    async def request_cb_wrapper(self, msg: Msg, callback_hdlr: Callable[[Any], Any]):
        """Wrapper for request callbacks to handle JSON encoding/decoding"""
        decoded_msg = loads(msg.data.decode())
        response = callback_hdlr(decoded_msg)
        await msg.respond(dumps(response).encode())


class NatsClientJS(NatsClient):
    """
    A NATS client with JetStream support for persistent messaging.
    Extends NatsClient with stream management and durable subscriptions.
    """

    def __init__(self, env: str = "local"):
        super().__init__(env)
        self.js: Optional[JetStreamContext] = None
        self._streams = set()

    async def aconnect(self):
        """Connect to NATS and initialize JetStream context"""
        await super().aconnect()
        self.js = self.nc.jetstream()
        print("JetStream context initialized")

    async def _stream_exists(self, stream_name: str) -> bool:
        """Check if a JetStream stream exists"""
        try:
            await self.js.stream_info(stream_name)
            return True
        except NotFoundError:
            return False

    async def aregister_new_stream(
        self, stream_name: str, subjects: list[str], no_ack: bool = True
    ):
        """Register a new JetStream stream"""
        if not await self._stream_exists(stream_name):
            await self.js.add_stream(name=stream_name, subjects=subjects, no_ack=no_ack)
            self._streams.add(stream_name)
            print(f"Registered stream: {stream_name} with subjects: {subjects}")

    def register_stream(
        self, stream_name: str, subjects: list[str], no_ack: bool = True
    ):
        """Register a new JetStream stream (synchronous wrapper)"""
        asyncio.run(self.aregister_new_stream(stream_name, subjects, no_ack))

    async def aunregister_stream(self, stream_name: str):
        """Unregister a JetStream stream"""
        if await self._stream_exists(stream_name):
            await self.js.delete_stream(stream_name)
            self._streams.discard(stream_name)
            print(f"Unregistered stream: {stream_name}")

    def unregister_stream(self, stream_name: str):
        """Unregister a JetStream stream (synchronous wrapper)"""
        asyncio.run(self.aunregister_stream(stream_name))

    async def apublish(self, subject: str, msg: Any):
        """Publish a message to JetStream"""
        if not self.js:
            raise ConnectionError("JetStream not initialized")
        enc_msg = dumps(msg).encode()
        await self.js.publish(subject, enc_msg)

    async def asubscribe(
        self, subject: str, callback_hdlr: Callable[[Any], None], durable: str = None
    ):
        """Subscribe to a JetStream subject"""
        if not self.js:
            raise ConnectionError("JetStream not initialized")
        wrapped_callback = functools.partial(
            self.subscribe_cb_wrapper, callback_hdlr=callback_hdlr
        )
        return await self.js.subscribe(subject, cb=wrapped_callback, durable=durable)

    def subscribe_durable(
        self, subject: str, callback_hdlr: Callable[[Any], None], durable: str
    ):
        """Subscribe to a JetStream subject with durable name (synchronous wrapper)"""
        return asyncio.run(self.asubscribe(subject, callback_hdlr, durable))
