#!/usr/bin/env bash

DATA_DIR="/home/sam-sullivan/dynamicWhitelist/data"
PYTHON="/home/sam-sullivan/dynamicWhitelist/.venv/bin/python"
PROJECT_DIR="/home/sam-sullivan/dynamicWhitelist"

CHAIN=ethereum
RPC_URL=http://100.104.193.35:8545

BLOCKS_PER_REQUEST=10000
CRYO_COMMON_OPTIONS="--rpc ${RPC_URL} \
	--inner-request-size ${BLOCKS_PER_REQUEST} \
	--u256-types binary"

UNISWAP_V4_POOL_MANAGER_CONTRACT_ADDRESS=0x000000000004444c5dc75cB358380D2e3dE08A90
UNISWAP_V4_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK=21_688_329

INITIALIZED_EVENT_HASH="0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
MODIFYLIQUIDITY_EVENT_HASH="0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"


LAST_BLOCK=$(cast block -f number -r $RPC_URL finalized)

# UniswapV4 new pools \
DIR="${DATA_DIR}/${CHAIN}/uniswap_v4_initialized_events"
if [ -d $DIR ]; then rm "${DIR}/$(ls ${DIR} | tail -1)"; fi
cryo logs \
	$CRYO_COMMON_OPTIONS \
	--blocks "${UNISWAP_V4_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK}:${LAST_BLOCK}" \
	--contract "${UNISWAP_V4_POOL_MANAGER_CONTRACT_ADDRESS}" \
	--event "${INITIALIZED_EVENT_HASH}" \
	--output-dir $DIR

$PYTHON ${PROJECT_DIR}/processors/uniswap_v4_pool_fetcher_parquet.py

# UniswapV4 liquidity eventss
DIR="${DATA_DIR}/${CHAIN}/uniswap_v4_modifyliquidity_events"
if [ -d $DIR ]; then rm "${DIR}/$(ls ${DIR} | tail -1)"; fi
cryo logs \
	$CRYO_COMMON_OPTIONS \
	--blocks "${UNISWAP_V4_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK}:${LAST_BLOCK}" \
	--contract "${UNISWAP_V4_POOL_MANAGER_CONTRACT_ADDRESS}" \
	--event "${MODIFYLIQUIDITY_EVENT_HASH}" \
	--output-dir $DIR

$PYTHON ${DATA_DIR}/processors/uniswap_v4_liquidity_events_processor_parquet.py

