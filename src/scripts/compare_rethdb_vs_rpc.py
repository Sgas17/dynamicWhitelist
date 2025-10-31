"""Compare RethDB tick data vs RPC for the same pool"""
import os
from web3 import Web3
from eth_utils import to_checksum_address
import sys
sys.path.append('/home/sam-sullivan/scrape_rethdb_data')
from scrape_rethdb_data import RethDbReader

# Setup
RPC_URL = "http://localhost:8545"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

RETH_DB_PATH = os.getenv('RETH_DB_PATH', '/var/lib/docker/volumes/eth-docker_reth-el-data/_data/db')

# Test pool: USDC/ETH 0.05%
POOL_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
POOL_ADDRESS = to_checksum_address(POOL_ADDRESS)
TICK_SPACING = 10

def encode_v3_tick_slot(tick: int, mapping_slot: int = 5) -> str:
    """Calculate V3 tick storage slot"""
    encoded = w3.codec.encode(['int24', 'uint256'], [tick, mapping_slot])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

print("="*80)
print("COMPARING RETHDB vs RPC TICK DATA")
print("="*80)
print(f"\nPool: {POOL_ADDRESS}")
print(f"Tick spacing: {TICK_SPACING}")

# Test tick 0
test_tick = 0

print(f"\n{'='*80}")
print(f"TICK {test_tick}")
print(f"{'='*80}")

# Get from RPC
tick_slot = encode_v3_tick_slot(test_tick)
rpc_value = w3.eth.get_storage_at(POOL_ADDRESS, tick_slot)
rpc_value_int = int.from_bytes(rpc_value, 'big')
rpc_liquidity_gross = rpc_value_int & ((1 << 128) - 1)
rpc_liquidity_net_raw = rpc_value_int >> 128

print(f"\nRPC:")
print(f"  Storage slot: {tick_slot}")
print(f"  Raw value: {rpc_value.hex()}")
print(f"  liquidityGross: {rpc_liquidity_gross}")
print(f"  liquidityNet (raw): {rpc_liquidity_net_raw}")

# Get from RethDB
print(f"\nRethDB:")
reader = RethDbReader(RETH_DB_PATH)

try:
    pool_data = reader.read_v3_pool(
        pool_address=POOL_ADDRESS,
        tick_spacing=TICK_SPACING
    )

    print(f"  Block number: {pool_data.get('block_number', 'N/A')}")
    print(f"  Total ticks: {len(pool_data.get('ticks', []))}")

    # Find tick 0
    tick_0_data = None
    for tick_data in pool_data.get('ticks', []):
        if tick_data['tick'] == test_tick:
            tick_0_data = tick_data
            break

    if tick_0_data:
        print(f"  Found tick {test_tick}:")
        print(f"    liquidityGross: {tick_0_data.get('liquidity_gross', 'N/A')}")
        print(f"    liquidityNet: {tick_0_data.get('liquidity_net', 'N/A')}")
        print(f"    raw_data: {tick_0_data.get('raw_data', 'N/A')}")

        # Compare
        print(f"\n  COMPARISON:")
        rpc_matches_rethdb = (
            rpc_liquidity_gross == tick_0_data.get('liquidity_gross', 0)
        )
        print(f"    RPC liquidityGross matches RethDB: {rpc_matches_rethdb}")
        if not rpc_matches_rethdb:
            print(f"    ❌ MISMATCH!")
            print(f"       RPC: {rpc_liquidity_gross}")
            print(f"       RethDB: {tick_0_data.get('liquidity_gross', 0)}")
    else:
        print(f"  ❌ Tick {test_tick} NOT FOUND in RethDB data!")

except Exception as e:
    print(f"  ❌ Error reading from RethDB: {e}")
    import traceback
    traceback.print_exc()
