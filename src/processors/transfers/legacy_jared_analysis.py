#!/usr/bin/env python3

import polars as pl
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
import ujson
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes

load_dotenv()

DATA_DIR = Path("/home/sam_eth/cryo")
CHAIN = os.getenv("CHAIN", "ethereum")  # Default to ethereum if not set
JARED_CONTRACT_ADDRESS = (
    "0x0000000000000000000000001f2F10D1C40777AE1Da742455c65828FF36Df387".lower()
)
# Convert to bytes for filtering binary data
JARED_CONTRACT_BYTES = bytes.fromhex(JARED_CONTRACT_ADDRESS[2:])
EVENTS_PATH = DATA_DIR / CHAIN / "jared_transfers"
TRANSFERED_TOKENS_FILE = DATA_DIR / f"{CHAIN}_unique_tokens.json"
try:
    with open(TRANSFERED_TOKENS_FILE, "r") as file:
        unique_tokens = ujson.load(file)

        # Check if we have any data and if the last item looks like metadata
        if (
            len(unique_tokens) > 0
            and isinstance(unique_tokens[-1], dict)
            and "block_number" in unique_tokens[-1]
        ):
            token_metadata = unique_tokens.pop(-1)
            last_token_block = token_metadata["block_number"]
            last_token_count = token_metadata["number_of_tokens"]
            print(f"Found {last_token_count} tokens up to block {last_token_block}")
        else:
            # No metadata found, assume fresh start
            print("No valid metadata found in existing file, starting fresh")
            last_token_block = 0

except (FileNotFoundError, ujson.JSONDecodeError, KeyError, IndexError):
    print("No existing token file found or file is corrupted, starting fresh")
    unique_tokens = []
    last_token_block = 0

# redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"))


transfer_events = pl.read_parquet(EVENTS_PATH / "*.parquet")
transfer_events = transfer_events.filter(
    pl.col("block_number") >= last_token_block
).sort(pl.col("block_number"))

print(f"Total transfer events loaded: {len(transfer_events)}")
print(f"Looking for Jared address: {JARED_CONTRACT_ADDRESS}")

# First, let's see what we have
sample_events = transfer_events.head(5)
sample_events = sample_events.select(
    ["block_number", "transaction_hash", "address", "topic1", "topic2", "topic3"]
)
for event in sample_events.rows(named=True):
    # Convert binary topics to hex and extract addresses (last 20 bytes for topic1/topic2)
    tx_hash = event["transaction_hash"].hex() if event["transaction_hash"] else "None"
    address = (event["address"]).lower() if event["address"] else "None"
    topic1_addr = (event["topic1"][-20:].hex()).lower()
    topic2_addr = (event["topic2"][-20:].hex()).lower()
    topic3_hex = event["topic3"].hex() if event["topic3"] else "None"

    print(
        f"block: {event['block_number']}, tx: {tx_hash[:10]}..., address: {address}, topic1: {topic1_addr}, topic2: {topic2_addr}, topic3: {topic3_hex[:10]}..."
    )
print("Sample events:")
print(sample_events)

transfer_events = transfer_events.filter(
    (pl.col("topic1") == JARED_CONTRACT_BYTES)
    | (pl.col("topic2") == JARED_CONTRACT_BYTES)
)
print(f"Filtered events involving Jared: {len(transfer_events)}")
print(transfer_events.head(2))

last_event_block = last_token_block  # Default to last known block

for event in transfer_events.rows(named=True):
    # Convert binary address to hex string
    token_address = to_checksum_address(event["address"]).lower()

    if token_address not in unique_tokens:
        unique_tokens.append(token_address)
        print(f"Found new token: {token_address}")
    last_event_block = event["block_number"]  # Track the last processed block

# Save the results - append metadata as last item in the list
metadata = {"block_number": last_event_block, "number_of_tokens": len(unique_tokens)}
unique_tokens.append(metadata)

with open(TRANSFERED_TOKENS_FILE, "w") as file:
    ujson.dump(unique_tokens, file, indent=2)
