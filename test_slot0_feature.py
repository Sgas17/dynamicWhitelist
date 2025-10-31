import scrape_rethdb_data
import json
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv("RETH_DB_PATH")

# Test slot0_only=True
pool = {
    "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    "protocol": "v3",
    "tick_spacing": 10,
    "slot0_only": True
}

result = scrape_rethdb_data.collect_pools(db_path, [pool], [])
data = json.loads(result)[0]

print("✓ slot0_only=True test:")
print(f"  - Has slot0: {'slot0' in data}")
print(f"  - Has liquidity: {'liquidity' in data}")
print(f"  - Liquidity value: {data.get('liquidity', 'N/A')}")
print(f"  - Ticks count: {len(data.get('ticks', []))}")
print(f"  - Bitmaps count: {len(data.get('bitmaps', []))}")

if data.get('liquidity') and len(data.get('ticks', [])) == 0:
    print("\n✅ SUCCESS! slot0_only feature is working correctly!")
else:
    print("\n❌ FAILED: Feature not working as expected")
