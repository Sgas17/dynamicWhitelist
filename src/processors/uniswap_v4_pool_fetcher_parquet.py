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
from eth_abi.abi import decode
from processors.utils.database_helper import (
    store_pools_to_database,
    check_uniswap_database_results,
)

CHAIN_NAME = "ethereum"
EXCHANGE_NAME = "uniswap_v4"
DATA_DIR = pathlib.Path("/home/sam-sullivan/dynamic_whitelist/data")
CHAIN_PATH = DATA_DIR / CHAIN_NAME
EVENTS_PATH = CHAIN_PATH / f"{EXCHANGE_NAME}_initialized_events/"
POOL_FILE = DATA_DIR / f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"
POOL_MANAGER_ADDRESS = "0x000000000004444c5dc75cB358380D2e3dE08A90"
LP_TYPE = "UniswapV4"

def to_serializable(val):
    if val is None:
        return None
    if isinstance(val, (bytes, HexBytes)):
        return '0x' + val.hex()
    return str(val)

try:
    with open(POOL_FILE, "r") as file:
        lp_data = ujson.load(file)
    lp_metadata = lp_data.pop(-1)
    last_pool_block = lp_metadata["block_number"]
    last_pool_count = lp_metadata["number_of_pools"]
    print(f"Found {last_pool_count} pools up to block {last_pool_block}")
except FileNotFoundError:
    lp_data = []
    last_pool_block = 0

poolcreated_events = (
    polars.read_parquet(str(EVENTS_PATH / "*.parquet"))
    .filter(polars.col("block_number") >= last_pool_block)
    .sort(polars.col("block_number"))
)


if poolcreated_events.is_empty():
    print("No new results")
    sys.exit()

last_event_block = poolcreated_events.select(polars.col("block_number")).max().item()

_pool_manager_address = HexBytes(POOL_MANAGER_ADDRESS)

for event in poolcreated_events.rows(named=True):
    factory_pool_manager_address = event["address"]
    if factory_pool_manager_address != _pool_manager_address:
        continue

    pool_id = HexBytes(event["topic1"]).to_0x_hex()
    currency0 = to_checksum_address(
        decode(
            types=["address"],
            data=event["topic2"],
        )[0]
    )
    currency1 = to_checksum_address(
        decode(
            types=["address"],
            data=event["topic3"],
        )[0]
    )

    fee, tick_spacing, hooks_address, sqrt_price, tick = decode(
        types=["uint24", "int24", "address", "uint160", "int24"],
        data=event["data"],
    )
    block_number = event["block_number"]



    lp_data.append(
        {
            "address": pool_id,
            "fee": fee,
            "tick_spacing": tick_spacing,
            "asset0": currency0,
            "asset1": currency1,
            "creation_block": block_number,
            "factory": to_serializable(factory_pool_manager_address),
            "type": LP_TYPE,
            "additional_data": {
                "hooks_address": to_serializable(hooks_address),
        },
        }
    )

lp_data.append({"block_number": last_event_block, "number_of_pools": len(lp_data)})


# Store pools to database
print("Storing pools to database...")
# Filter out the metadata entry (last item) before storing to database
pool_data_for_db = [pool for pool in lp_data if isinstance(pool, dict) and 'address' in pool]
store_pools_to_database(pool_data_for_db)

#check database results
check_uniswap_database_results(LP_TYPE)

with open(POOL_FILE, "w") as file:
    ujson.dump(lp_data, file, indent=2)
    print(f"Stored {lp_data[-1]['number_of_pools']} pools")
