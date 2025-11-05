"""
Test script to extract and verify whitelist data from Redis.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import redis.asyncio as redis

from src.config import ConfigManager


async def extract_whitelist_from_redis():
    """Extract whitelist data from Redis and save to files."""
    config = ConfigManager()
    redis_config = config.redis

    # Connect to Redis
    client = redis.Redis(
        host=redis_config.host,
        port=redis_config.port,
        db=redis_config.db,
        decode_responses=True,
    )

    try:
        # Test connection
        await client.ping()
        print("✅ Connected to Redis")

        # Get whitelist data
        chain = "ethereum"
        whitelist_key = f"whitelist:{chain}"
        metadata_key = f"whitelist:{chain}:metadata"

        # Get all whitelist token data
        whitelist_data = await client.get(whitelist_key)
        metadata_data = await client.get(metadata_key)

        if not whitelist_data:
            print("❌ No whitelist data found in Redis")
            return

        whitelist = json.loads(whitelist_data)
        metadata = json.loads(metadata_data) if metadata_data else {}

        print(f"\n📊 Whitelist Summary:")
        print(f"  Total tokens: {len(whitelist)}")
        print(f"  Metadata: {metadata}")

        # Save to output directory
        output_dir = Path("data/redis_extracts")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save full whitelist
        whitelist_path = output_dir / f"whitelist_{chain}_{timestamp}.json"
        with open(whitelist_path, "w") as f:
            json.dump({"metadata": metadata, "tokens": whitelist}, f, indent=2)
        print(f"💾 Saved whitelist to {whitelist_path}")

        # Extract tokens by source
        by_source = {
            "cross_chain": [],
            "hyperliquid": [],
            "lighter": [],
            "top_transferred": [],
            "multi_source": [],
        }

        for token in whitelist:
            sources = token.get("sources", [])
            if len(sources) > 1:
                by_source["multi_source"].append(token)
            for source in sources:
                if source in by_source:
                    by_source[source].append(token)

        # Save by source breakdown
        by_source_path = output_dir / f"whitelist_by_source_{chain}_{timestamp}.json"
        with open(by_source_path, "w") as f:
            json.dump(
                {
                    "metadata": {
                        "chain": chain,
                        "extracted_at": datetime.now().isoformat(),
                        "total_tokens": len(whitelist),
                    },
                    "counts": {k: len(v) for k, v in by_source.items()},
                    "tokens_by_source": by_source,
                },
                f,
                indent=2,
            )
        print(f"💾 Saved by-source breakdown to {by_source_path}")

        # Print summary
        print(f"\n📈 Breakdown by source:")
        for source, tokens in by_source.items():
            print(f"  {source}: {len(tokens)}")

        # Extract just addresses for easy use
        addresses_path = output_dir / f"whitelist_addresses_{chain}_{timestamp}.json"
        with open(addresses_path, "w") as f:
            json.dump(
                {
                    "total": len(whitelist),
                    "addresses": [
                        t.get("address") for t in whitelist if t.get("address")
                    ],
                },
                f,
                indent=2,
            )
        print(f"💾 Saved addresses only to {addresses_path}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await client.close()


async def main():
    """Main entry point."""
    print("=" * 80)
    print("REDIS WHITELIST EXTRACTION TEST")
    print("=" * 80)
    await extract_whitelist_from_redis()
    print("=" * 80)
    print("EXTRACTION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
