#!/usr/bin/env bash

# Check if number of hours argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <number_of_hours>"
    echo "Example: $0 1    # Fetch transfers for the last hour"
    exit 1
fi

HOURS=$1
BLOCKS_PER_HOUR=300  # Approximately 300 blocks per hour (12s block time)
BLOCKS_TO_FETCH=$((HOURS * BLOCKS_PER_HOUR))

# Use current project data directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"

CHAIN=ethereum
RPC_URL=http://100.104.193.35:8545

# Using an extremely small block range to avoid hitting event limits
BLOCKS_PER_REQUEST=10
CRYO_COMMON_OPTIONS="--rpc ${RPC_URL} \
	--inner-request-size ${BLOCKS_PER_REQUEST} \
	--u256-types binary"

TRANSFER_EVENT_HASH="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

LAST_BLOCK=$(cast block -f number -r $RPC_URL finalized)
START_BLOCK=$((LAST_BLOCK - BLOCKS_TO_FETCH))

echo "Fetching transfers from block ${START_BLOCK} to ${LAST_BLOCK} (approximately last ${HOURS} hours)"

# Create directory structure
DIR="${DATA_DIR}/${CHAIN}/latest_transfers"
mkdir -p $DIR
if [ -d $DIR ]; then rm -f "${DIR}/$(ls ${DIR} | tail -1)" 2>/dev/null; fi

# Function to fetch a specific block range
fetch_block_range() {
    local start=$1
    local end=$2
    echo "Fetching blocks ${start} to ${end}..."
    
    cryo logs \
        $CRYO_COMMON_OPTIONS \
        --blocks "${start}:${end}" \
        --event "${TRANSFER_EVENT_HASH}" \
        --output-dir $DIR
    
    return $?
}

# Split the total range into smaller chunks
CHUNK_SIZE=500
current_start=$START_BLOCK

while [ $current_start -lt $LAST_BLOCK ]; do
    current_end=$((current_start + CHUNK_SIZE))
    if [ $current_end -gt $LAST_BLOCK ]; then
        current_end=$LAST_BLOCK
    fi
    
    # Try to fetch the chunk
    if ! fetch_block_range $current_start $current_end; then
        echo "Initial attempt failed for blocks ${current_start}-${current_end}, retrying with smaller ranges..."
        
        # If failed, try with even smaller ranges
        small_start=$current_start
        while [ $small_start -lt $current_end ]; do
            small_end=$((small_start + BLOCKS_PER_REQUEST))
            if [ $small_end -gt $current_end ]; then
                small_end=$current_end
            fi
            
            fetch_block_range $small_start $small_end
            small_start=$((small_end + 1))
        done
    fi
    
    current_start=$((current_end + 1))
done

# Check if we have any data before processing
if [ -z "$(ls -A $DIR/*.parquet 2>/dev/null)" ]; then
    echo "Error: No data was collected. Exiting."
    exit 1
fi

# Process the transfers
echo "Processing collected transfer data..."
cd "${PROJECT_ROOT}"
uv run python src/scripts/run_transfer_processor.py




