#!/bin/bash
#
# Pool Scraping Performance Test
#
# This script tests the actual scraping performance of pool_state_arena
# using a cached whitelist from dynamicWhitelist.
#
# Usage:
#   ./scripts/test_pool_scraping_performance.sh [num_pools]
#
# Args:
#   num_pools: Number of pools to test (default: all pools from cache)

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHITELIST_DIR="$SCRIPT_DIR/../data"
ARENA_DIR="/home/sam-sullivan/defi_platform/pool_state_arena"

# Configuration
WHITELIST_FILE="$WHITELIST_DIR/filtered_pools_ethereum.json"
NUM_POOLS="${1:-all}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Pool Scraping Performance Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if whitelist exists
if [ ! -f "$WHITELIST_FILE" ]; then
    echo -e "${RED}❌ Whitelist file not found: $WHITELIST_FILE${NC}"
    echo -e "${YELLOW}Run the orchestrator first to generate whitelist:${NC}"
    echo -e "  uv run python -m src.whitelist.orchestrator"
    exit 1
fi

# Get pool counts
V2_COUNT=$(jq -r '.pools[] | select(.protocol == "v2")' "$WHITELIST_FILE" | jq -s 'length')
V3_COUNT=$(jq -r '.pools[] | select(.protocol == "v3")' "$WHITELIST_FILE" | jq -s 'length')
V4_COUNT=$(jq -r '.pools[] | select(.protocol == "v4")' "$WHITELIST_FILE" | jq -s 'length')
TOTAL_COUNT=$((V2_COUNT + V3_COUNT + V4_COUNT))

echo -e "${GREEN}✓ Loaded whitelist:${NC}"
echo -e "  V2 pools:    $V2_COUNT"
echo -e "  V3 pools:    $V3_COUNT"
echo -e "  V4 pools:    $V4_COUNT"
echo -e "  Total pools: $TOTAL_COUNT"
echo ""

# Check if pool_state_arena exists
if [ ! -d "$ARENA_DIR" ]; then
    echo -e "${RED}❌ pool_state_arena directory not found: $ARENA_DIR${NC}"
    exit 1
fi

echo -e "${BLUE}Testing scraping performance...${NC}"
echo ""

# For now, let's use Python to test scraping a few pools
# TODO: Replace with actual Rust performance test once implemented

echo -e "${YELLOW}Creating test sample...${NC}"

# Create sample pool lists for testing
mkdir -p "$SCRIPT_DIR/../data/test_samples"

# Sample V2 pools (first 10)
jq '.pools[] | select(.protocol == "v2")' "$WHITELIST_FILE" | jq -s '.[0:10]' > "$SCRIPT_DIR/../data/test_samples/v2_pools_sample.json"

# Sample V3 pools (first 10)
jq '.pools[] | select(.protocol == "v3")' "$WHITELIST_FILE" | jq -s '.[0:10]' > "$SCRIPT_DIR/../data/test_samples/v3_pools_sample.json"

# Sample V4 pools (first 10)
jq '.pools[] | select(.protocol == "v4")' "$WHITELIST_FILE" | jq -s '.[0:10]' > "$SCRIPT_DIR/../data/test_samples/v4_pools_sample.json"

echo -e "${GREEN}✓ Created test samples in data/test_samples/${NC}"
echo ""

echo -e "${YELLOW}Next steps:${NC}"
echo -e "1. Implement Rust scraping performance test in pool_state_arena"
echo -e "2. Measure actual V2/V3/V4 scraping times"
echo -e "3. Compare against estimates:"
echo -e "   - V2: ~10-20 pools/sec (estimated)"
echo -e "   - V3: ~1-2 pools/sec (estimated)"
echo -e "   - V4: ~1-2 pools/sec (estimated)"
echo ""
echo -e "${BLUE}Test samples ready at:${NC}"
echo -e "  V2: data/test_samples/v2_pools_sample.json (10 pools)"
echo -e "  V3: data/test_samples/v3_pools_sample.json (10 pools)"
echo -e "  V4: data/test_samples/v4_pools_sample.json (10 pools)"

