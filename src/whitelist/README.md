# Token Whitelist & Pool Filtering Module

This module builds a comprehensive token whitelist and filters DEX pools through a multi-stage process to enable arbitrage opportunities.

## Project Goal

Create a dynamic token whitelist from multiple sources, then filter DEX pools to identify arbitrage-ready trading paths with verified liquidity.

## Overview

### Stage 0: Whitelist Building

The `TokenWhitelistBuilder` aggregates tokens from four key sources:

1. **Cross-chain tokens** - Tokens present on 2+ chains (Ethereum, Base, Arbitrum)
2. **Hyperliquid tokens** - Perpetual swap tokens from Hyperliquid DEX
3. **Lighter tokens** - Perpetual swap tokens from Lighter DEX
4. **Top transferred tokens** - Most actively transferred tokens on Ethereum

### Stage 1: Whitelist + Trusted Token Pairs

Filter pools containing any whitelisted token paired with a trusted token:
- **Filter criteria**: Pool contains `whitelisted_token + trusted_token`
- **Liquidity threshold**: Lower threshold (Stage 1)
- **Purpose**: Establish baseline prices and verify liquidity for whitelisted tokens
- **Output**: Snapshot token prices for Stage 2 filtering

### Stage 2: Whitelist + Any Token (with Trust Path)

Find arbitrage opportunities through intermediate tokens:
- **Filter criteria**: Pool contains `whitelisted_token + other_token`
- **Liquidity threshold**: Higher than Stage 1
- **Trust path requirement**: `other_token` must have a pool with a `trusted_token` above liquidity threshold
- **Trading path**: `whitelisted_token <-> other_token <-> trusted_token`
- **Purpose**: Enable arbitrage through verified price discovery paths

## Usage

```python
from src.whitelist import TokenWhitelistBuilder
from src.core.storage.postgres import PostgresStorage

# Initialize storage
storage = PostgresStorage(config=db_config)
await storage.connect()

# Build whitelist
builder = TokenWhitelistBuilder(storage)
result = await builder.build_whitelist(top_transfers=100)

# Save to file
builder.save_whitelist("data/token_whitelist.json")
```

## Running as Script

```bash
uv run python -m src.whitelist.builder
```

## Output Format

The whitelist is saved as JSON with the following structure:

```json
{
  "total_tokens": 500,
  "tokens": ["0x...", "0x...", ...],
  "token_sources": {
    "0x...": ["cross_chain", "hyperliquid"],
    "0x...": ["top_transferred"]
  },
  "token_info": {
    "0x...": {
      "coingecko_id": "ethereum",
      "symbol": "ETH",
      "chains": ["ethereum", "base", "arbitrum-one"]
    }
  }
}
```

## Data Sources

### Cross-chain Tokens

Queries the `coingecko_token_platforms` table to find tokens that exist on multiple chains, indicating widespread adoption and liquidity.

### Perp DEX Tokens

Fetches tokens listed on perpetual DEX platforms:
- **Hyperliquid**: Uses CCXT to fetch perpetual swap markets
- **Lighter**: Uses REST API to fetch funding rates data

Symbols are mapped to Ethereum addresses via the `coingecko_token_platforms` table.

### Top Transferred Tokens

Queries the `token_hourly_transfers` table to find tokens with the highest average 24-hour transfer counts, indicating active on-chain usage.

## Configuration

Default parameters can be adjusted:

- `top_transfers` (default: 100) - Number of top transferred tokens to include
- Database connection settings from `ConfigManager`

## Dependencies

- PostgreSQL database with:
  - `coingecko_token_platforms` table
  - `token_hourly_transfers` table (from UnifiedTransferProcessor)
- CCXT library for Hyperliquid
- aiohttp for Lighter API calls
