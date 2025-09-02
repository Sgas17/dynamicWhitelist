#!/usr/bin/env bash

set -e  # Exit on error

DATA_DIR="/home/sam_eth/cryo"
PYTHON="/home/sam_eth/cryo/.venv/bin/python"


CHAIN=ethereum
RPC_URL=http://100.104.193.35:8545

# Test RPC connection first


BLOCKS_PER_REQUEST=100  # Reduced from 10000
CRYO_COMMON_OPTIONS="--rpc ${RPC_URL} \
	--inner-request-size ${BLOCKS_PER_REQUEST} \
	--u256-types binary \
    --verbose"  # Added verbose flag

JARED_CONTRACT_ADDRESS=0x0000000000000000000000001f2F10D1C40777AE1Da742455c65828FF36Df387

# JARED_ADDRESS_START_BLOCK=20466717

echo "Getting latest block number..."
LAST_BLOCK=$(cast block -f number -r $RPC_URL finalized)
UNIV4_START_BLOCK=22394734

# Calculate start block for 3 months ago (approximately 90 days * 7200 blocks/day)
BLOCKS_3_MONTHS=648000
START_BLOCK=$((LAST_BLOCK - BLOCKS_3_MONTHS))

DIR="${DATA_DIR}/${CHAIN}/jared_transfers"

# Follow the same pattern as uniswap script - remove last file to prevent duplicates
if [ -d "$DIR" ] && [ "$(ls -A $DIR)" ]; then
    echo "Existing data found. Removing last file to prevent duplicates and ensure completeness..."
    LAST_FILE=$(ls "$DIR" | tail -1)
    if [ -n "$LAST_FILE" ]; then
        echo "Removing: $LAST_FILE"
        rm "${DIR}/${LAST_FILE}"
    fi
fi



echo "Processing blocks from ${START_BLOCK} to ${LAST_BLOCK}"
echo "That's approximately $((LAST_BLOCK - START_BLOCK)) blocks to process"

# Create directory if it doesn't exist
mkdir -p "$DIR"

echo "Fetching ERC20 transfer logs involving Jared contract..."

# ERC20 Transfer event signature: Transfer(address,address,uint256)
TRANSFER_TOPIC="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Fetch all transfers and filter for Jared involvement in post-processing
echo "Fetching transfer from logs with Jared as sender"
cryo logs \
    ${CRYO_COMMON_OPTIONS} \
    --blocks ${START_BLOCK}:${LAST_BLOCK} \
    --topic0 ${TRANSFER_TOPIC} \
	--topic1 ${JARED_CONTRACT_ADDRESS} \
    --output-dir "${DIR}"

echo "Transfer data saved to ${DIR}"


# Analyze the results to extract unique token addresses
echo "Analyzing transfers to find Jared involvement and extract unique token addresses..."
export CHAIN=${CHAIN}
$PYTHON analyze_jared_transfers.py

