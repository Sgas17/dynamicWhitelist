#!/usr/bin/env python3
"""Quick script to check NATS minimal message format."""

import asyncio
import json

from nats.aio.client import Client as NATS


async def main():
    """Subscribe to minimal NATS topic and print messages."""
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    print("✅ Connected to NATS")
    print("📡 Subscribing to whitelist.pools.ethereum.minimal...")
    print("Waiting for messages (Ctrl+C to exit)...\n")

    async def message_handler(msg):
        data = json.loads(msg.data.decode())
        print("=" * 80)
        print(f"📨 Received message on {msg.subject}:")
        print(json.dumps(data, indent=2))
        print("=" * 80)
        print()

        # Check if protocols field exists
        if "protocols" in data:
            print(f"✅ PROTOCOLS FIELD EXISTS: {len(data['protocols'])} protocols")
            print(f"   Example: {data['protocols'][:5]}")
        else:
            print("❌ PROTOCOLS FIELD MISSING")
        print()

    await nc.subscribe("whitelist.pools.ethereum.minimal", cb=message_handler)

    # Keep alive
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
