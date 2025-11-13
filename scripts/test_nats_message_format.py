#!/usr/bin/env python3
"""
Test script to verify NATS message format for pool_state_arena compatibility.
"""
import asyncio
import json
import sys
import os
from nats.aio.client import Client as NATS

# Get NATS URL from environment
NATS_URL = os.getenv("NATS_URL_LOCAL", "nats://localhost:4222")


async def test_message_format():
    """Subscribe to NATS and check the message format."""
    nc = NATS()

    try:
        await nc.connect(NATS_URL)
        print(f"✅ Connected to NATS at {NATS_URL}")

        # Subscribe to the full whitelist topic
        subject = "whitelist.pools.ethereum.full"
        print(f"📡 Subscribing to: {subject}")

        message_received = False

        async def message_handler(msg):
            nonlocal message_received
            message_received = True

            print(f"\n📨 Received message on {msg.subject}")

            try:
                data = json.loads(msg.data.decode())
                print(f"\n📊 Message structure:")
                print(json.dumps(data, indent=2)[:1000])  # First 1000 chars

                # Verify structure
                print(f"\n🔍 Validation:")
                print(f"  ✓ Has 'chain' field: {'chain' in data}")
                print(f"  ✓ Has 'pools' field: {'pools' in data}")

                if 'pools' in data and len(data['pools']) > 0:
                    pool = data['pools'][0]
                    print(f"\n📦 First pool structure:")
                    print(json.dumps(pool, indent=2))

                    print(f"\n🔍 Pool validation:")
                    print(f"  ✓ Has 'id' field: {'id' in pool}")
                    print(f"  ✓ Has 'protocol' field: {'protocol' in pool}")
                    print(f"  ✓ Has 'token0' field: {'token0' in pool}")
                    print(f"  ✓ Has 'token1' field: {'token1' in pool}")
                    print(f"  ✓ Has 'token0_decimals' field: {'token0_decimals' in pool}")
                    print(f"  ✓ Has 'token1_decimals' field: {'token1_decimals' in pool}")

                    if 'protocol' in pool:
                        print(f"  ✓ Protocol format: {pool['protocol']}")

                    # Check for V4 pool
                    v4_pools = [p for p in data['pools'] if p.get('protocol') == 'uniswap_v4']
                    if v4_pools:
                        print(f"\n📦 V4 pool example:")
                        print(json.dumps(v4_pools[0], indent=2))

            except Exception as e:
                print(f"❌ Error parsing message: {e}")

        sub = await nc.subscribe(subject, cb=message_handler)
        print(f"✅ Subscribed, waiting for messages (5 seconds)...")

        # Wait for messages
        await asyncio.sleep(5)

        if not message_received:
            print("\n⚠️  No messages received. The whitelist might not have changed.")
            print("   Try forcing a full update or adding a new pool.")

        await sub.unsubscribe()

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        await nc.close()
        print("\n✅ Disconnected from NATS")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(test_message_format()))
