#!/usr/bin/env bash

DATA_DIR="/home/sam-sullivan/dynamic_whitelist/data"
PROJECT_DIR="/home/sam-sullivan/dynamic_whitelist"
PYTHON="/home/sam-sullivan/dynamic_whitelist/.venv/bin/python"

CHAIN=ethereum
RPC_URL=http://100.104.193.35:8545

BLOCKS_PER_REQUEST=10000
CRYO_COMMON_OPTIONS="--rpc ${RPC_URL} \
	--inner-request-size ${BLOCKS_PER_REQUEST} \
	--u256-types binary"

UNISWAP_V3_FACTORY=0x1F98431c8aD98523631AE4a59f267346ea31F984
SUSHISWAP_V3_FACTORY=0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F
PANCAKESWAP_V3_FACTORY=0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865
UNISWAP_V3_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK=12_369_621
UNISWAP_V3_EXAMPLE_POOL="0x1d42064Fc4Beb5F8aAF85F4617AE8b3b5B8Bd801"

POOL_CREATED_EVENT_HASH="0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
MINT_EVENT_HASH="0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_EVENT_HASH="0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"


LAST_BLOCK=$(cast block -f number -r $RPC_URL finalized)

# UniswapV3 new pools \
DIR="${DATA_DIR}/${CHAIN}/uniswap_v3_poolcreated_events"
if [ -d $DIR ]; then rm "${DIR}/$(ls ${DIR} | tail -1)"; fi
cryo logs \
	$CRYO_COMMON_OPTIONS \
	--blocks "${UNISWAP_V3_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK}:${LAST_BLOCK}" \
	--contract "${UNISWAP_V3_FACTORY}" "${SUSHISWAP_V3_FACTORY}" "${PANCAKESWAP_V3_FACTORY}" \
	--event "${POOL_CREATED_EVENT_HASH}" \
	--output-dir $DIR

$PYTHON ${PROJECT_DIR}/processors/uniswap_v3_pool_fetcher_parquet.py

#UniswapV3 Liquidity Events

DIR="${DATA_DIR}/${CHAIN}/uniswap_v3_modifyliquidity_events"
if [ -d $DIR ]; then rm "${DIR}/$(ls ${DIR} | tail -1)"; fi


cryo logs \
	$CRYO_COMMON_OPTIONS \
	--blocks "${UNISWAP_V3_POOL_MANAGER_CONTRACT_DEPLOYMENT_BLOCK}:${LAST_BLOCK}" \
	--event "${MINT_EVENT_HASH}" "${BURN_EVENT_HASH}" \
	--output-dir $DIR

$PYTHON ${PROJECT_DIR}/processors/uniswap_v3_liquidity_events_processor_parquet.py