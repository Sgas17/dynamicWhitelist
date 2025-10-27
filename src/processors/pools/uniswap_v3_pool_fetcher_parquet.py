import pathlib
import sys

# Add the project root to the Python path
project_root = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import eth_abi.abi as eth_abi
import polars
import ujson
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes
from src.core.storage.postgres_pools import (
    store_pools_to_database,
    check_uniswap_database_results
)

# Import config system
from src.config.manager import ConfigManager

# Initialize config
config = ConfigManager()

CHAIN_NAME = "ethereum"
EXCHANGE_NAME = "uniswap_v3"
DATA_DIR = pathlib.Path(config.base.DATA_DIR)
CHAIN_PATH = DATA_DIR / CHAIN_NAME
EVENTS_PATH = CHAIN_PATH / f"{EXCHANGE_NAME}_poolcreated_events/"
POOL_FILE = DATA_DIR / f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"

# Use centralized protocol config for factory addresses
FACTORY_ADDRESSES = config.protocol.get_factory_addresses("uniswap_v3", CHAIN_NAME)
LP_TYPE = "UniswapV3"

try:
    with open(POOL_FILE, "r") as file:
        lp_data = ujson.load(file)
    lp_metadata = lp_data.pop(-1)
    last_pool_block = int(lp_metadata["block_number"])
    last_pool_count = lp_metadata["number_of_pools"]
    print(f"Found {last_pool_count} pools up to block {last_pool_block}")
except FileNotFoundError:
    lp_data = []
    last_pool_block = 0

poolcreated_events = (
    polars.read_parquet(EVENTS_PATH / "*.parquet")
    .filter(polars.col("block_number") >= last_pool_block)
    .sort(polars.col("block_number"))
)


if poolcreated_events.is_empty():
    print("No new results")
    sys.exit()

last_event_block = poolcreated_events.select(polars.col("block_number")).max().item()

_pool_manager_addresses = [HexBytes(address) for address in FACTORY_ADDRESSES]

def to_serializable(val):
    if val is None:
        return None
    if isinstance(val, (bytes, HexBytes)):
        return '0x' + val.hex()
    return str(val)

for event in poolcreated_events.rows(named=True):
    factory_address = event["address"]
    if factory_address not in _pool_manager_addresses:
        continue

    currency0 = to_checksum_address(
        eth_abi.decode(
            types=["address"],
            data=event["topic1"],
        )[0]
    )
    currency1 = to_checksum_address(
        eth_abi.decode(
            types=["address"],
            data=event["topic2"],
        )[0]
    )
    fee = eth_abi.decode(
        types=["uint24"],
        data=event["topic3"],
    )[0]

    tick_spacing, pool_address = eth_abi.decode(
        types=["int24", "address"],
        data=event["data"],
    )
    pool_address = to_checksum_address(pool_address)
    block_number = event["block_number"]

    lp_data.append(
        {
            "address": to_serializable(pool_address),
            "fee": fee,
            "tick_spacing": tick_spacing,
            "asset0": to_serializable(currency0),
            "asset1": to_serializable(currency1),
            "type": LP_TYPE,
            "creation_block": block_number,
            "factory": to_serializable(factory_address),
        }
    )

lp_data.append({"block_number": last_event_block, "number_of_pools": len(lp_data)})

# Store pools to database
print("Storing pools to database...")
# Filter out the metadata entry (last item) before storing to database
pool_data_for_db = [pool for pool in lp_data if isinstance(pool, dict) and 'address' in pool]

# Deduplicate by pool address to avoid PostgreSQL conflict errors
seen_addresses = set()
deduplicated_pools = []
duplicates_count = 0

for pool in pool_data_for_db:
    address = pool['address']
    if address not in seen_addresses:
        seen_addresses.add(address)
        deduplicated_pools.append(pool)
    else:
        duplicates_count += 1

if duplicates_count > 0:
    print(f"Warning: Found {duplicates_count} duplicate pool addresses, removing duplicates")
    print(f"This might indicate:")
    print(f"  - Duplicate events in your parquet files")
    print(f"  - Same pool created by multiple factories (shouldn't happen)")
    print(f"  - Data corruption or processing issues")
    print(f"Storing {len(deduplicated_pools)} unique pools to database...")
else:
    print(f"No duplicates found. Storing {len(deduplicated_pools)} pools to database...")

store_pools_to_database(deduplicated_pools)

# Check database results
check_uniswap_database_results(LP_TYPE)

# Before dumping to JSON, ensure all values are serializable
import copy
serializable_lp_data = []
for entry in lp_data:
    if isinstance(entry, dict):
        serializable_lp_data.append({k: to_serializable(v) for k, v in entry.items()})
    else:
        serializable_lp_data.append(entry)

with open(POOL_FILE, "w") as file:
    ujson.dump(serializable_lp_data, file, indent=2)
    print(f"Stored {lp_data[-1]['number_of_pools']} pools")
