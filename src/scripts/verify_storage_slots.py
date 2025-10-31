"""Verify storage slot calculations match between RethDB and RPC"""
import asyncio
import json
from eth_typing import HexStr
from web3 import Web3
from eth_utils import to_checksum_address

# RPC endpoint
RPC_URL = "http://localhost:8545"  # Adjust as needed
w3 = Web3(Web3.HTTPProvider(RPC_URL))

def encode_v3_tick_slot(tick: int, mapping_slot: int = 5) -> str:
    """Calculate V3 tick storage slot: keccak256(abi.encode(tick, mapping_slot))"""
    # ABI encode (int24, uint256)
    encoded = w3.codec.encode(['int24', 'uint256'], [tick, mapping_slot])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

def encode_v3_bitmap_slot(word_pos: int, mapping_slot: int = 6) -> str:
    """Calculate V3 bitmap storage slot: keccak256(abi.encode(wordPos, mapping_slot))"""
    # ABI encode (int16, uint256)
    encoded = w3.codec.encode(['int16', 'uint256'], [word_pos, mapping_slot])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

def encode_v4_pool_base_slot(pool_id: str, pools_slot: int = 6) -> str:
    """Calculate V4 pool base slot: keccak256(abi.encode(poolId, pools_slot))"""
    # ABI encode (bytes32, uint256)
    encoded = w3.codec.encode(['bytes32', 'uint256'], [bytes.fromhex(pool_id.replace('0x', '')), pools_slot])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

def add_offset_to_slot(slot: str, offset: int) -> str:
    """Add offset to storage slot"""
    slot_int = int(slot, 16)
    new_slot = slot_int + offset
    return f"0x{new_slot:064x}"

def encode_v4_tick_slot(pool_id: str, tick: int) -> str:
    """Calculate V4 tick slot: nested mapping with poolId and tick"""
    # Step 1: Get pool base slot
    base_slot = encode_v4_pool_base_slot(pool_id)

    # Step 2: Add offset for ticks mapping (offset = 4)
    ticks_mapping_slot = add_offset_to_slot(base_slot, 4)

    # Step 3: Calculate tick slot
    encoded = w3.codec.encode(['int24', 'uint256'], [tick, int(ticks_mapping_slot, 16)])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

def encode_v4_bitmap_slot(pool_id: str, word_pos: int) -> str:
    """Calculate V4 bitmap slot: nested mapping with poolId and wordPos"""
    # Step 1: Get pool base slot
    base_slot = encode_v4_pool_base_slot(pool_id)

    # Step 2: Add offset for tickBitmap mapping (offset = 5)
    bitmap_mapping_slot = add_offset_to_slot(base_slot, 5)

    # Step 3: Calculate bitmap slot
    encoded = w3.codec.encode(['int16', 'uint256'], [word_pos, int(bitmap_mapping_slot, 16)])
    slot_hash = w3.keccak(encoded)
    return slot_hash.hex()

def encode_v4_slot0_slot(pool_id: str) -> str:
    """Calculate V4 slot0 slot"""
    base_slot = encode_v4_pool_base_slot(pool_id)
    # slot0 is at offset 0 from base
    return base_slot

def encode_v4_liquidity_slot(pool_id: str) -> str:
    """Calculate V4 liquidity slot"""
    base_slot = encode_v4_pool_base_slot(pool_id)
    # liquidity is at offset 3 from base
    return add_offset_to_slot(base_slot, 3)

async def test_v3_pool_slots():
    """Test V3 pool storage slot calculations"""
    print("="*80)
    print("TESTING V3 POOL STORAGE SLOTS")
    print("="*80)

    # Use a known V3 pool with liquidity: USDC/ETH 0.05% pool
    pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    pool_address = to_checksum_address(pool_address)

    print(f"\nPool: {pool_address}")

    # Test slot0 (simple slot 0)
    slot0_value = w3.eth.get_storage_at(pool_address, 0)
    print(f"\nSlot0 (slot 0): {slot0_value.hex()}")

    # Test liquidity (simple slot 4)
    liquidity_value = w3.eth.get_storage_at(pool_address, 4)
    print(f"Liquidity (slot 4): {liquidity_value.hex()}")
    print(f"Liquidity decimal: {int.from_bytes(liquidity_value, 'big')}")

    # Test a known tick with liquidity
    test_tick = 0  # Current tick area should have liquidity
    tick_slot = encode_v3_tick_slot(test_tick)
    tick_value = w3.eth.get_storage_at(pool_address, tick_slot)

    print(f"\nTick {test_tick}:")
    print(f"  Slot: {tick_slot}")
    print(f"  Value: {tick_value.hex()}")

    # Parse liquidity from tick value
    # liquidityGross is lower 128 bits
    value_int = int.from_bytes(tick_value, 'big')
    liquidity_gross = value_int & ((1 << 128) - 1)
    liquidity_net_raw = value_int >> 128

    print(f"  liquidityGross: {liquidity_gross}")
    print(f"  liquidityNet (raw): {liquidity_net_raw}")

    # Test bitmap
    word_pos = 0  # Word position for tick 0
    bitmap_slot = encode_v3_bitmap_slot(word_pos)
    bitmap_value = w3.eth.get_storage_at(pool_address, bitmap_slot)
    print(f"\nBitmap word {word_pos}:")
    print(f"  Slot: {bitmap_slot}")
    print(f"  Value: {bitmap_value.hex()}")

    return {
        'protocol': 'v3',
        'pool': pool_address,
        'slot0_slot': '0x0000000000000000000000000000000000000000000000000000000000000000',
        'slot0_value': slot0_value.hex(),
        'liquidity_slot': '0x0000000000000000000000000000000000000000000000000000000000000004',
        'liquidity_value': liquidity_value.hex(),
        'tick_slot': tick_slot,
        'tick_value': tick_value.hex(),
        'tick_liquidity_gross': liquidity_gross,
        'bitmap_slot': bitmap_slot,
        'bitmap_value': bitmap_value.hex()
    }

async def test_v4_pool_slots():
    """Test V4 pool storage slot calculations"""
    print("\n" + "="*80)
    print("TESTING V4 POOL STORAGE SLOTS")
    print("="*80)

    # V4 PoolManager address
    pool_manager = "0x5302086A3a25d473aAbBd0356eFf8Dd811a4d89B"
    pool_manager = to_checksum_address(pool_manager)

    # Example pool ID - we'll need to get one from the database
    # For now, let's test the calculation logic
    pool_id = "0x0000000000000000000000000000000000000000000000000000000000000001"  # Placeholder

    print(f"\nPoolManager: {pool_manager}")
    print(f"PoolId: {pool_id}")

    # Calculate various slots
    base_slot = encode_v4_pool_base_slot(pool_id)
    slot0_slot = encode_v4_slot0_slot(pool_id)
    liquidity_slot = encode_v4_liquidity_slot(pool_id)
    tick_slot = encode_v4_tick_slot(pool_id, 0)
    bitmap_slot = encode_v4_bitmap_slot(pool_id, 0)

    print(f"\nCalculated slots:")
    print(f"  Base slot: {base_slot}")
    print(f"  Slot0: {slot0_slot}")
    print(f"  Liquidity slot: {liquidity_slot}")
    print(f"  Tick 0 slot: {tick_slot}")
    print(f"  Bitmap word 0 slot: {bitmap_slot}")

    # Try to read values
    slot0_value = w3.eth.get_storage_at(pool_manager, slot0_slot)
    liquidity_value = w3.eth.get_storage_at(pool_manager, liquidity_slot)
    tick_value = w3.eth.get_storage_at(pool_manager, tick_slot)
    bitmap_value = w3.eth.get_storage_at(pool_manager, bitmap_slot)

    print(f"\nValues from RPC:")
    print(f"  Slot0: {slot0_value.hex()}")
    print(f"  Liquidity: {liquidity_value.hex()}")
    print(f"  Tick 0: {tick_value.hex()}")
    print(f"  Bitmap word 0: {bitmap_value.hex()}")

    return {
        'protocol': 'v4',
        'pool_manager': pool_manager,
        'pool_id': pool_id,
        'base_slot': base_slot,
        'slot0_slot': slot0_slot,
        'slot0_value': slot0_value.hex(),
        'liquidity_slot': liquidity_slot,
        'liquidity_value': liquidity_value.hex(),
        'tick_slot': tick_slot,
        'tick_value': tick_value.hex(),
        'bitmap_slot': bitmap_slot,
        'bitmap_value': bitmap_value.hex()
    }

async def main():
    results = {}

    # Test V3
    try:
        v3_results = await test_v3_pool_slots()
        results['v3'] = v3_results
    except Exception as e:
        print(f"V3 test failed: {e}")
        import traceback
        traceback.print_exc()

    # Test V4
    try:
        v4_results = await test_v4_pool_slots()
        results['v4'] = v4_results
    except Exception as e:
        print(f"V4 test failed: {e}")
        import traceback
        traceback.print_exc()

    # Save results
    output_file = 'data/storage_slot_verification.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n✅ Saved results to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
