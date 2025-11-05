import itertools
import os
import pathlib
from threading import Lock
from weakref import WeakSet

import eth_abi.abi as eth_abi
import polars
import pydantic
import pydantic_core
from degenbot.types import BoundedCache
from degenbot.uniswap.types import (
    UniswapV3BitmapAtWord,
    UniswapV3LiquidityAtTick,
    UniswapV3PoolLiquidityMappingUpdate,
    UniswapV3PoolState,
)
from degenbot.uniswap.v3_liquidity_pool import UniswapV3Pool
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes

# Import database functions
# Note: Liquidity snapshot functions will be moved to postgres_liquidity.py
# For now, keep these imports but they will be migrated
from utils.database_helper import (
    get_database_engine,
    get_snapshot_metadata,
    load_liquidity_snapshot_from_database,
    setup_liquidity_snapshot_tables,
    store_liquidity_snapshot_to_database,
)

# Import config system
from src.config.manager import ConfigManager

# Initialize config
config = ConfigManager()

CHAIN_NAME = "ethereum"
EXCHANGE_NAME = "uniswap_v3"
DATA_DIR = pathlib.Path(config.base.DATA_DIR)
CHAIN_PATH = DATA_DIR / CHAIN_NAME
EVENTS_PATH = CHAIN_PATH / f"{EXCHANGE_NAME}_modifyliquidity_events/"
POOL_FILE = DATA_DIR / f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"

# Use centralized protocol config for pool manager address
POOL_MANAGER_ADDRESS = config.protocol.get_factory_addresses("uniswap_v3", CHAIN_NAME)[
    0
]
LP_TYPE = "UniswapV3"
POOL_FILES = [
    os.path.join(DATA_DIR, f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"),
]
SNAPSHOT_FILENAME = os.path.join(
    DATA_DIR, f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}_snapshot.json"
)
# Use centralized protocol config for event hashes
MINT_EVENT_TOPIC = config.protocol.get_event_hash("uniswap_v3_mint")
BURN_EVENT_TOPIC = config.protocol.get_event_hash("uniswap_v3_burn")

# last_event_block = 12_379_621

BLOCK_CHUNK_SIZE = 100000


class MockV3LiquidityPool(UniswapV3Pool):
    def __init__(self):
        self._initial_state_block = 0
        self._state_cache = BoundedCache(max_items=8)
        self._subscribers = set()  # type: ignore
        self._state_lock = Lock()
        self.sparse_liquidity_map = False
        self.name = "V3 POOL"


liquidity_snapshot: dict[str, dict] = {}
lp_data: dict[str, dict] = {}

for path in POOL_FILES:
    try:
        lps: list = pydantic_core.from_json(pathlib.Path(path).read_text())
        print(f"Loaded {len(lps)} pools from {path}")
        for i, lp in enumerate(lps):
            try:
                # Skip metadata entries that don't have 'address' field
                if isinstance(lp, dict) and "address" in lp:
                    # Ensure numeric fields are properly typed (JSON converts them to strings)
                    pool_data = {
                        "address": lp["address"],
                        "fee": int(lp["fee"]) if lp.get("fee") is not None else None,
                        "tick_spacing": int(lp["tick_spacing"])
                        if lp.get("tick_spacing") is not None
                        else None,
                        "asset0": lp["asset0"],
                        "asset1": lp["asset1"],
                        "type": lp["type"],
                        "creation_block": int(lp["creation_block"])
                        if lp.get("creation_block") is not None
                        else None,
                        "factory": lp["factory"],
                    }
                    lp_data[lp["address"]] = pool_data
                elif isinstance(lp, dict) and (
                    "block_number" in lp or "number_of_pools" in lp
                ):
                    # This is metadata, skip silently
                    continue
                else:
                    print(f"Skipping invalid pool entry {i}: {lp}")
            except Exception as e:
                print(f"Error processing pool {i}: {type(e)}: {e} on row {lp}")
                continue
    except Exception as e:
        print(f"Error loading pools from {path}: {type(e)}: {e}")

    print(f"Successfully loaded {len(lp_data)} pools")


def get_pools_with_events(start_block: int = 0, end_block: int | None = None) -> set:
    """
    Get only the pools that have liquidity events in the given block range.
    """
    print("Finding pools with liquidity events...")
    events = (
        polars.scan_parquet(EVENTS_PATH / "*.parquet")
        .select("address", "block_number")
        .filter(polars.col("block_number") >= start_block)
    )

    if end_block is not None:
        events = events.filter(polars.col("block_number") < end_block)

    # Get unique pool addresses that have events
    pools_with_events = set(events.collect().get_column("address").unique().to_list())
    print(f"Found {len(pools_with_events)} pools with liquidity events")
    return pools_with_events


def get_liquidity_events(
    start_block: int = 0,
    end_block: int | None = None,
    pool_addresses: list | None = None,
) -> polars.LazyFrame:
    """
    Fetch the liquidity events for a given block range.
    """
    if pool_addresses is None:
        pool_addresses = [HexBytes(addr) for addr in lp_data.keys()]

    events = (
        polars.scan_parquet(EVENTS_PATH / "*.parquet")
        .select(
            (
                "address",
                "block_number",
                "transaction_index",
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
            )
        )
        .filter(
            (polars.col("address").is_in(pool_addresses))
            & (polars.col("block_number") >= start_block)
        )
        .sort(polars.col("block_number"), polars.col("transaction_index"))
    )

    if end_block is not None:
        events = events.filter(polars.col("block_number") < end_block)

    return events


try:
    # First try to load from database
    print("Attempting to load snapshot from database...")
    liquidity_snapshot = load_liquidity_snapshot_from_database()

    if liquidity_snapshot:
        snapshot_last_block = liquidity_snapshot.pop("snapshot_block")
        print(
            f"Loaded database snapshot: {len(liquidity_snapshot)} pools @ block {snapshot_last_block}"
        )

        # Convert string keys back to integers for processing
        for pool_address, snapshot in liquidity_snapshot.items():
            if isinstance(snapshot, dict):
                liquidity_snapshot[pool_address] = {
                    "tick_bitmap": {
                        int(k): v for k, v in snapshot.get("tick_bitmap", {}).items()
                    },
                    "tick_data": {
                        int(k): v for k, v in snapshot.get("tick_data", {}).items()
                    },
                }
    else:
        # Fallback to file-based snapshot
        print("No database snapshot found, attempting to load from file...")
        try:
            liquidity_snapshot = pydantic_core.from_json(
                pathlib.Path(SNAPSHOT_FILENAME).read_bytes()
            )
            snapshot_last_block = liquidity_snapshot.pop("snapshot_block")
            assert isinstance(snapshot_last_block, int)
            print(
                f"Loaded file snapshot: {len(liquidity_snapshot)} pools @ block {snapshot_last_block}"
            )
            for pool_address, snapshot in liquidity_snapshot.items():
                liquidity_snapshot[pool_address] = {
                    "tick_bitmap": {
                        int(k): v for k, v in snapshot["tick_bitmap"].items()
                    },
                    "tick_data": {int(k): v for k, v in snapshot["tick_data"].items()},
                }
        except FileNotFoundError:
            snapshot_last_block = -1
            liquidity_snapshot = {}
            print("No snapshot found, initializing from empty state at block 0.")

except Exception as e:
    print(f"Error loading snapshots: {e}")
    snapshot_last_block = -1
    liquidity_snapshot = {}
    print("Falling back to empty state at block 0.")

# Get pools that actually have liquidity events
pools_with_events = get_pools_with_events(start_block=snapshot_last_block + 1)
print(f"Filtering to {len(pools_with_events)} pools with liquidity events")

# Initialize empty state for pools with events that don't have snapshots
for pool_address in pools_with_events:
    if pool_address not in liquidity_snapshot:
        liquidity_snapshot[pool_address] = {
            "tick_bitmap": {},
            "tick_data": {},
        }


last_event_block = (
    get_liquidity_events().select(polars.max("block_number")).collect().item()
)


print(f"data contains events up to block {last_event_block}")

lp_helper = MockV3LiquidityPool()
lp_helper.sparse_liquidity_map = False
lp_helper._subscribers = WeakSet()
lp_helper._state_lock = Lock()
lp_helper._state_cache = BoundedCache(max_items=8)
lp_helper.name = "V3 POOL"


known_pools = set(HexBytes(addr) for addr in lp_data.keys())


for start_block in itertools.count(snapshot_last_block + 1, BLOCK_CHUNK_SIZE):
    # if we've reached the end of the data, break
    if start_block > last_event_block:
        break

    end_block = min(
        start_block + BLOCK_CHUNK_SIZE,
        last_event_block + 1,
    )
    liquidity_events_for_chunk = (
        get_liquidity_events(
            start_block=start_block,
            end_block=end_block,
            pool_addresses=list(pools_with_events),
        )
        .collect()
        .with_columns(
            pool_address=polars.col("address"),
        )
        .drop(["address"], strict=True)
    )
    if liquidity_events_for_chunk.is_empty():
        continue

    tick_lower_values = []
    tick_upper_values = []
    liquidity_delta_values = []
    for log_hash, topic2, topic3, event_data in liquidity_events_for_chunk.select(
        polars.col("topic0"),
        polars.col("topic2"),
        polars.col("topic3"),
        polars.col("data"),
    ).iter_rows():
        tick_lower = eth_abi.decode(
            types=["int24"],
            data=topic2,
        )[0]
        tick_upper = eth_abi.decode(
            types=["int24"],
            data=topic3,
        )[0]
        if log_hash == HexBytes(MINT_EVENT_TOPIC):
            _, liquidity_delta, *_ = eth_abi.decode(
                types=["address", "int128", "uint256", "uint256"],
                data=event_data,
            )
        elif log_hash == HexBytes(BURN_EVENT_TOPIC):
            liquidity_delta, *_ = eth_abi.decode(
                types=["int128", "uint256", "uint256"],
                data=event_data,
            )
            liquidity_delta = -liquidity_delta
        else:
            raise ValueError(f"Unknown event hash: {log_hash}")

        tick_lower_values.append(tick_lower)
        tick_upper_values.append(tick_upper)
        liquidity_delta_values.append(liquidity_delta)

    liquidity_events_for_chunk.hstack(
        [
            polars.Series(
                name="tick_lower",
                values=tick_lower_values,
                dtype=polars.Int64,
            ),
            polars.Series(
                name="tick_upper",
                values=tick_upper_values,
                dtype=polars.Int64,
            ),
            polars.Series(
                name="liquidity_delta",
                values=[
                    v[0] if isinstance(v, list) else v for v in liquidity_delta_values
                ],
                dtype=polars.Int128,
            ),
        ],
        in_place=True,
    )

    pool_addresses_in_chunk = set(
        liquidity_events_for_chunk.get_column("pool_address").unique().to_list()
    )

    print(
        f"Processing chunk {start_block} - {end_block} with {len(pool_addresses_in_chunk)} unique pools"
    )

    for pool_address in pool_addresses_in_chunk:
        pool_address = to_checksum_address(pool_address)
        pool_data = lp_data[pool_address]

        try:
            previous_snapshot_tick_data = liquidity_snapshot[pool_address]["tick_data"]
        except KeyError:
            previous_snapshot_tick_data = {}

        try:
            previous_snapshot_tick_bitmap = liquidity_snapshot[pool_address][
                "tick_bitmap"
            ]
        except KeyError:
            previous_snapshot_tick_bitmap = {}

        match pool_data["type"]:
            case "UniswapV3":
                lp_helper.fee = pool_data["fee"]
                lp_helper.tick_spacing = pool_data["tick_spacing"]
            case _ as pool_type:
                raise ValueError(f"Unknown pool type: {pool_type}")

        lp_helper._state_cache.clear()
        lp_helper._state = UniswapV3PoolState(
            address=pool_address,
            block=None,
            liquidity=2**256 - 1,
            sqrt_price_x96=0,
            tick=0,
            tick_bitmap={
                int(k): UniswapV3BitmapAtWord(**v)
                for k, v in previous_snapshot_tick_bitmap.items()
            },
            tick_data={
                int(k): UniswapV3LiquidityAtTick(**v)
                for k, v in previous_snapshot_tick_data.items()
            },
        )
        pool_liquidity_events = liquidity_events_for_chunk.filter(
            polars.col("pool_address") == HexBytes(pool_address)
        )

        for block_number, tick_lower, tick_upper, liquidity_delta in zip(
            pool_liquidity_events.get_column("block_number").to_list(),
            pool_liquidity_events.get_column("tick_lower").to_list(),
            pool_liquidity_events.get_column("tick_upper").to_list(),
            pool_liquidity_events.get_column("liquidity_delta").to_list(),
        ):
            lp_helper.update_liquidity_map(
                update=UniswapV3PoolLiquidityMappingUpdate(
                    block_number=block_number,
                    liquidity=liquidity_delta,
                    tick_lower=tick_lower,
                    tick_upper=tick_upper,
                )
            )

        if pool_address not in liquidity_snapshot:
            liquidity_snapshot[pool_address] = {
                "tick_bitmap": {},
                "tick_data": {},
            }
        liquidity_snapshot[pool_address]["tick_bitmap"] = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_bitmap.items()
        }
        liquidity_snapshot[pool_address]["tick_data"] = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_data.items()
        }

    liquidity_snapshot["snapshot_block"] = end_block - 1  # type: ignore

    # Store to database first
    try:
        print("Storing snapshot to database...")
        store_liquidity_snapshot_to_database(
            liquidity_snapshot=dict(liquidity_snapshot),  # Make a copy
            snapshot_block=end_block - 1,
        )
        print(f"Successfully stored snapshot to database at block {end_block - 1}")
    except Exception as e:
        print(f"Error storing to database: {e}")
        print("Continuing with file storage as backup...")

    # Also save to file as backup
    pathlib.Path(SNAPSHOT_FILENAME).write_bytes(
        pydantic.TypeAdapter(dict).dump_json(
            liquidity_snapshot,
            indent=2,
        )
    )
    print(f"Saved snapshot to disk at block {liquidity_snapshot['snapshot_block']}")
