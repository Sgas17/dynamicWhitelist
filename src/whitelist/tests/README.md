# Whitelist Tests

## Running Tests

### Quick Unit Tests (Fast - ~2 seconds)
Run only the fast unit tests, skipping slow integration tests:
```bash
uv run pytest src/whitelist/tests/test_liquidity_filter.py -v -m "not integration"
```

### All Tests Including Integration (Slow - ~5 minutes)
Run all tests including the slow integration test:
```bash
uv run pytest src/whitelist/tests/test_liquidity_filter.py -v
```

### Specific Test Classes
```bash
# Test only V4 native ETH normalization
uv run pytest src/whitelist/tests/test_liquidity_filter.py::TestNormalization -v

# Test only V4 native ETH pool handling
uv run pytest src/whitelist/tests/test_liquidity_filter.py::TestV4NativeETH -v

# Test only Hyperliquid price fetching
uv run pytest src/whitelist/tests/test_liquidity_filter.py::TestHyperliquidPrices -v
```

## Test Coverage

### TestNormalization (4 tests) ✅
Tests the V4 native ETH (zero address) to WETH normalization:
- Zero address → WETH conversion
- WETH address passes through
- Regular tokens pass through
- Case insensitivity

### TestHyperliquidPrices (1 test) ✅
Tests fetching real-time prices from Hyperliquid API

### TestPriceCalculation (1 test) ✅
Tests token price calculation from V2 pool reserves using constant product formula

### TestV2PoolFiltering (1 test) ✅
Tests V2 pool filtering with real on-chain data via RPC

### TestV4NativeETH (1 test) ✅
**Critical test**: Validates that V4 pools with native ETH (zero address) are handled correctly
- Normalizes zero address to WETH for price/decimal lookups
- Does not crash when encountering native ETH pools

### TestFullPipeline (1 test - SLOW) 🐌
End-to-end integration test using WhitelistOrchestrator:
- Builds complete token whitelist
- Filters pools by liquidity
- Tests full pipeline from database to filtered results
- **Takes 5+ minutes** - only run when needed

## Test Structure

All tests use proper pytest structure with:
- `@pytest.mark.asyncio` for async tests
- `@pytest.mark.integration` for slow tests
- Relative imports for proper package structure
- Mock data where possible, real RPC only when needed
