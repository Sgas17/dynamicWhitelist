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
from degenbot.uniswap.v4_liquidity_pool import UniswapV4Pool
from degenbot.uniswap.v4_types import (
    UniswapV4BitmapAtWord,
    UniswapV4LiquidityAtTick,
    UniswapV4PoolLiquidityMappingUpdate,
    UniswapV4PoolState,
)
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes

# Import our database helper functions
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
EXCHANGE_NAME = "uniswap_v4"
DATA_DIR = pathlib.Path(config.base.DATA_DIR)
CHAIN_PATH = DATA_DIR / CHAIN_NAME
EVENTS_PATH = CHAIN_PATH / f"{EXCHANGE_NAME}_modifyliquidity_events/"
POOL_FILE = DATA_DIR / f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"

# Use centralized protocol config for pool manager address
try:
    v4_config = config.protocol.get_protocol_config("uniswap_v4", CHAIN_NAME)
    POOL_MANAGER_ADDRESS = v4_config.get("pool_manager", "")
    if isinstance(POOL_MANAGER_ADDRESS, list) and POOL_MANAGER_ADDRESS:
        POOL_MANAGER_ADDRESS = POOL_MANAGER_ADDRESS[0]
except Exception:
    # Fallback for development
    POOL_MANAGER_ADDRESS = "0x000000000004444C5dC75CB358380d2e3DE08A90"
LP_TYPE = "UniswapV4"
POOL_FILES = [
    os.path.join(DATA_DIR, f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}.json"),
]
SNAPSHOT_FILENAME = os.path.join(
    DATA_DIR, f"{CHAIN_NAME}_lps_{EXCHANGE_NAME}_snapshot.json"
)
MINT_EVENT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_EVENT_TOPIC = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

# last_event_block = 12_379_621

BLOCK_CHUNK_SIZE = 100000


class MockV4LiquidityPool(UniswapV4Pool):
    def __init__(self):
        self._fee = 0
        self._tick_spacing = 0
        self._initial_state_block = 0
        self._state = UniswapV4PoolState(
            id="0x0",  # type: ignore
            address="0x0",  # type: ignore
            liquidity=0,
            sqrt_price_x96=0,
            tick=0,
            tick_bitmap={},
            tick_data={},
            block=0,  # type: ignore
        )
        self._state_cache = BoundedCache(max_items=8)
        self._subscribers = set()  # type: ignore
        self._state_lock = Lock()
        self.sparse_liquidity_map = False
        self.name = "V4 POOL"

    @property
    def fee(self):
        return self._fee

    @fee.setter
    def fee(self, value):
        self._fee = value

    @property
    def tick_spacing(self):
        return self._tick_spacing

    @tick_spacing.setter
    def tick_spacing(self, value):
        self._tick_spacing = value

    @property
    def state(self):
        return self._state

    @property
    def tick_bitmap(self):
        return self._state.tick_bitmap

    @property
    def tick_data(self):
        return self._state.tick_data

    @property
    def update_block(self) -> int:
        return 0 if self.state.block is None else self.state.block


liquidity_snapshot: dict[str, dict] = {}
lp_data: dict[str, dict] = {}

for path in POOL_FILES:
    try:
        lps: list = pydantic_core.from_json(pathlib.Path(path).read_text())
        for i, lp in enumerate(lps):
            try:
                # Skip metadata entries that don't have proper fields
                if isinstance(lp, dict) and ("pool_id" in lp or "address" in lp):
                    # Ensure numeric fields are properly typed (JSON converts them to strings)
                    pool_data = {
                        "address": lp.get(
                            "address", lp.get("pool_id")
                        ),  # V4 uses pool_id
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
                    # For V4, use pool_id as the key if available, otherwise address
                    pool_key = lp.get("pool_id", lp.get("address"))
                    if pool_key:
                        lp_data[pool_key] = pool_data
                elif isinstance(lp, dict) and (
                    "block_number" in lp or "number_of_pools" in lp
                ):
                    # This is metadata, skip silently
                    continue
            except Exception as e:
                print(f"Error processing pool {i}: {type(e)}: {e} on row {lp}")
                continue
    except Exception as e:
        print(f"Error loading pools from {path}: {type(e)}: {e}")

try:
    # First try to load from database
    print("Attempting to load V4 snapshot from database...")
    liquidity_snapshot = load_liquidity_snapshot_from_database()

    if liquidity_snapshot:
        snapshot_last_block = liquidity_snapshot.pop("snapshot_block")
        print(
            f"Loaded V4 database snapshot: {len(liquidity_snapshot)} pools @ block {snapshot_last_block}"
        )

        # Convert string keys back to integers for processing
        for pool_id, snapshot in liquidity_snapshot.items():
            if isinstance(snapshot, dict):
                liquidity_snapshot[pool_id] = {
                    "tick_bitmap": {
                        int(k): v for k, v in snapshot.get("tick_bitmap", {}).items()
                    },
                    "tick_data": {
                        int(k): v for k, v in snapshot.get("tick_data", {}).items()
                    },
                }
    else:
        # Fallback to file-based snapshot
        print("No V4 database snapshot found, attempting to load from file...")
        try:
            liquidity_snapshot = pydantic_core.from_json(
                pathlib.Path(SNAPSHOT_FILENAME).read_bytes()
            )
            snapshot_last_block = liquidity_snapshot.pop("snapshot_block")
            assert isinstance(snapshot_last_block, int)
            print(
                f"Loaded V4 file snapshot: {len(liquidity_snapshot)} pools @ block {snapshot_last_block}"
            )
            for pool_id, snapshot in liquidity_snapshot.items():
                liquidity_snapshot[pool_id] = {
                    "tick_bitmap": {
                        int(k): v for k, v in snapshot["tick_bitmap"].items()
                    },
                    "tick_data": {int(k): v for k, v in snapshot["tick_data"].items()},
                }
        except FileNotFoundError:
            snapshot_last_block = -1
            liquidity_snapshot = {}
            print("No V4 snapshot found, initializing from empty state at block 0.")

except Exception as e:
    print(f"Error loading V4 snapshots: {e}")
    snapshot_last_block = -1
    liquidity_snapshot = {}
    print("Falling back to empty state at block 0.")


def get_liquidity_events(
    start_block: int = 0, end_block: int | None = None
) -> polars.LazyFrame:
    """
    Fetch the liquidity events for a given block range.
    """
    events = (
        polars.scan_parquet(EVENTS_PATH / "*.parquet")
        .select(
            (
                "address",
                "transaction_index",
                "block_number",
                "data",
                "topic1",
                "topic2",
                "topic3",
            )
        )
        .filter(
            (
                (polars.col("address") == HexBytes(POOL_MANAGER_ADDRESS))
                & (polars.col("block_number") >= start_block)
            )
        )
        .sort(polars.col("block_number"), polars.col("transaction_index"))
        .drop("address", strict=True)
    )

    if end_block is not None:
        events = events.filter(polars.col("block_number") < end_block)

    return events


last_event_block = (
    get_liquidity_events().select(polars.max("block_number")).collect().item()
)

print(f"data contains events up to block {last_event_block}")


lp_helper = MockV4LiquidityPool()
lp_helper.sparse_liquidity_map = False
lp_helper._subscribers = set()  # type: ignore
lp_helper._state_lock = Lock()
lp_helper._state_cache = BoundedCache(max_items=8)
lp_helper.name = "V4 POOL"


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
        get_liquidity_events(start_block=start_block, end_block=end_block)
        .collect()
        .with_columns(
            pool_id=polars.col("topic1"),
        )
        .drop(["topic1"], strict=True)
    )
    if liquidity_events_for_chunk.is_empty():
        continue

    tick_lower_values = []
    tick_upper_values = []
    liquidity_delta_values = []
    salt_values = []
    for event_data, *_ in liquidity_events_for_chunk.select(
        polars.col("data")
    ).iter_rows():
        tick_lower, tick_upper, liquidity_delta, salt = eth_abi.decode(
            types=["int24", "int24", "int128", "uint256"],
            data=event_data,
        )
        tick_lower_values.append(tick_lower)
        tick_upper_values.append(tick_upper)
        liquidity_delta_values.append(liquidity_delta)
        salt_values.append(salt)

    liquidity_events_for_chunk.hstack(
        [
            polars.Series(
                name="tick_lower", values=tick_lower_values, dtype=polars.Int64
            ),
            polars.Series(
                name="tick_upper", values=tick_upper_values, dtype=polars.Int64
            ),
            polars.Series(
                name="liquidity_delta",
                values=liquidity_delta_values,
                dtype=polars.Int128,
            ),
            polars.Series(
                name="salt",
                values=[HexBytes(s).hex() for s in salt_values],
                dtype=polars.Utf8,
            ),
        ],
        in_place=True,
    )

    pool_ids_in_chunk = set(
        liquidity_events_for_chunk.get_column("pool_id").unique().to_list()
    )

    print(
        f"Processing chunk {start_block} - {end_block} with {len(pool_ids_in_chunk)} unique pools"
    )

    for pool_id in pool_ids_in_chunk:
        pool_id_hex = HexBytes(pool_id).to_0x_hex()
        pool_data = lp_data[pool_id_hex]

        try:
            previous_snapshot_tick_data = liquidity_snapshot[pool_id_hex]["tick_data"]
        except KeyError:
            previous_snapshot_tick_data = {}

        try:
            previous_snapshot_tick_bitmap = liquidity_snapshot[pool_id_hex][
                "tick_bitmap"
            ]
        except KeyError:
            previous_snapshot_tick_bitmap = {}

        match pool_data["type"]:
            case "UniswapV4":
                lp_helper.fee = pool_data["fee"]
                lp_helper.tick_spacing = pool_data["tick_spacing"]
            case _ as pool_type:
                raise ValueError(f"Unknown pool type: {pool_type}")

        lp_helper._state_cache.clear()
        lp_helper._state = UniswapV4PoolState(
            address=POOL_MANAGER_ADDRESS,  # type: ignore
            id=pool_id_hex,  # type: ignore
            block=None,
            liquidity=2**256 - 1,
            sqrt_price_x96=0,
            tick=0,
            tick_bitmap={
                int(k): UniswapV4BitmapAtWord(**v)
                for k, v in previous_snapshot_tick_bitmap.items()
            },
            tick_data={
                int(k): UniswapV4LiquidityAtTick(**v)
                for k, v in previous_snapshot_tick_data.items()
            },
        )
        pool_liquidity_events = liquidity_events_for_chunk.filter(
            polars.col("pool_id") == pool_id
        )
        for block_number, tick_lower, tick_upper, liquidity_delta in zip(
            pool_liquidity_events.get_column("block_number").to_list(),
            pool_liquidity_events.get_column("tick_lower").to_list(),
            pool_liquidity_events.get_column("tick_upper").to_list(),
            pool_liquidity_events.get_column("liquidity_delta").to_list(),
        ):
            lp_helper.update_liquidity_map(
                update=UniswapV4PoolLiquidityMappingUpdate(
                    block_number=block_number,
                    liquidity=liquidity_delta,
                    tick_lower=tick_lower,
                    tick_upper=tick_upper,
                )
            )
        if pool_id_hex not in liquidity_snapshot:
            liquidity_snapshot[pool_id_hex] = {
                "tick_bitmap": {},
                "tick_data": {},
            }
        liquidity_snapshot[pool_id_hex]["tick_bitmap"] = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_bitmap.items()
        }
        liquidity_snapshot[pool_id_hex]["tick_data"] = {
            k: v.model_dump(mode="json") for k, v in lp_helper.tick_data.items()
        }

    liquidity_snapshot["snapshot_block"] = end_block - 1  # type: ignore

    # Store to database first
    try:
        print("Storing V4 snapshot to database...")
        store_liquidity_snapshot_to_database(
            liquidity_snapshot=dict(liquidity_snapshot),  # Make a copy
            snapshot_block=end_block - 1,
        )
        print(f"Successfully stored V4 snapshot to database at block {end_block - 1}")
    except Exception as e:
        print(f"Error storing V4 snapshot to database: {e}")
        print("Continuing with file storage as backup...")

    # Also save to file as backup
    pathlib.Path(SNAPSHOT_FILENAME).write_bytes(
        pydantic.TypeAdapter(dict).dump_json(
            liquidity_snapshot,
            indent=2,
        )
    )
    print(f"Saved V4 snapshot to disk at block {liquidity_snapshot['snapshot_block']}")
