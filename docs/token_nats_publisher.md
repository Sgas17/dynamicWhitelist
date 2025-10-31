# Token Whitelist NATS Publisher

## Overview

The Token Whitelist NATS Publisher provides dynamic token metadata publishing to NATS topics, enabling real-time tracking of whitelisted tokens across the system. This complements the existing Pool Whitelist NATS Publisher.

## Architecture

### Publisher Class: `TokenWhitelistNatsPublisher`

**Location**: `src/core/storage/token_whitelist_publisher.py`

### NATS Topic Structure

#### 1. Full Whitelist Topic
**Topic**: `whitelist.tokens.{chain}.full`

Publishes the complete token whitelist periodically.

**Message Format**:
```json
{
    "chain": "ethereum",
    "timestamp": "2025-10-31T14:20:06.439002+00:00",
    "tokens": {
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": {
            "symbol": "USDC",
            "decimals": 6,
            "name": "USD Coin",
            "filters": ["cross_chain", "top_transferred"]
        },
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {
            "symbol": "WETH",
            "decimals": 18,
            "name": "Wrapped Ether",
            "filters": ["cross_chain", "hyperliquid"]
        }
    },
    "metadata": {
        "total_count": 150,
        "filter_counts": {
            "cross_chain": 50,
            "hyperliquid": 30,
            "top_transferred": 70
        }
    }
}
```

#### 2. Add Delta Topic
**Topic**: `whitelist.tokens.{chain}.add`

Publishes newly added tokens (delta updates).

**Message Format**:
```json
{
    "chain": "ethereum",
    "timestamp": "2025-10-31T14:20:06.439002+00:00",
    "action": "add",
    "tokens": {
        "0xNewTokenAddress...": {
            "symbol": "NEWTOKEN",
            "decimals": 18,
            "name": "New Token",
            "filters": ["top_transferred"]
        }
    }
}
```

#### 3. Remove Delta Topic
**Topic**: `whitelist.tokens.{chain}.remove`

Publishes removed token addresses (delta updates).

**Message Format**:
```json
{
    "chain": "ethereum",
    "timestamp": "2025-10-31T14:20:06.439002+00:00",
    "action": "remove",
    "token_addresses": ["0xRemovedToken1...", "0xRemovedToken2..."]
}
```

## Token Metadata Structure

Each token includes the following metadata:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | string | Yes | Token symbol (e.g., "USDC") |
| `decimals` | integer | Yes | Token decimals (e.g., 6, 18) |
| `name` | string | No | Full token name (e.g., "USD Coin") |
| `filters` | array[string] | Yes | Filter types that matched this token |

### Filter Types

Tokens can match one or more of the following filters:

- **`cross_chain`**: Token is available on multiple chains
- **`hyperliquid`**: Token is listed on Hyperliquid exchange
- **`lighter`**: Token is listed on Lighter exchange
- **`top_transferred`**: Token is in the top N most transferred tokens

## Integration

### In WhitelistOrchestrator

The token publisher is integrated into `src/whitelist/orchestrator.py` at **Step 5c**:

```python
# Step 5c: Publish token whitelist to NATS (for dynamic token tracking)
async with TokenWhitelistNatsPublisher() as token_publisher:
    token_publish_results = await token_publisher.publish_token_whitelist(
        chain=chain,
        tokens=tokens_for_nats
    )
```

### Data Flow

1. **Token Discovery**: Tokens are collected from multiple sources (cross-chain, Hyperliquid, Lighter, top transferred)
2. **Metadata Enrichment**: Token info (symbol, decimals, name) is fetched from the database
3. **Filter Tracking**: Each token is tagged with its matching filters
4. **NATS Publishing**:
   - Full whitelist published to `full` topic
   - Delta changes published to `add` and `remove` topics
5. **Results Tracking**: Publishing results are logged and returned

## Usage Examples

### Standalone Function

```python
from src.core.storage.token_whitelist_publisher import publish_token_whitelist

tokens = {
    '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48': {
        'symbol': 'USDC',
        'decimals': 6,
        'name': 'USD Coin',
        'filters': ['cross_chain', 'top_transferred']
    }
}

results = await publish_token_whitelist('ethereum', tokens)
# Returns: {'full': True, 'add': True, 'remove': True}
```

### Context Manager

```python
async with TokenWhitelistNatsPublisher() as publisher:
    results = await publisher.publish_token_whitelist(
        chain="ethereum",
        tokens=tokens_dict,
        publish_full=True,
        publish_deltas=True
    )
```

## Consumer Examples

### Subscribing to Full Whitelist

```python
import nats
import json

async def handle_full_whitelist(msg):
    data = json.loads(msg.data.decode())
    chain = data['chain']
    tokens = data['tokens']
    print(f"Received {len(tokens)} tokens for {chain}")

nc = await nats.connect("nats://localhost:4222")
await nc.subscribe("whitelist.tokens.ethereum.full", cb=handle_full_whitelist)
```

### Subscribing to Delta Updates

```python
async def handle_added_tokens(msg):
    data = json.loads(msg.data.decode())
    new_tokens = data['tokens']
    print(f"New tokens added: {list(new_tokens.keys())}")

async def handle_removed_tokens(msg):
    data = json.loads(msg.data.decode())
    removed = data['token_addresses']
    print(f"Tokens removed: {removed}")

await nc.subscribe("whitelist.tokens.ethereum.add", cb=handle_added_tokens)
await nc.subscribe("whitelist.tokens.ethereum.remove", cb=handle_removed_tokens)
```

## Delta Tracking

The publisher maintains state to track changes between runs:

- **First Run**: All tokens are published to both `full` and `add` topics
- **Subsequent Runs**:
  - Only new tokens are published to `add` topic
  - Only removed tokens are published to `remove` topic
  - Full whitelist always published to `full` topic

This enables efficient delta processing for consumers that maintain their own state.

## Error Handling

The publisher includes comprehensive error handling:

- **Connection Failures**: Logged and propagated
- **Publish Failures**: Logged and returns `False` for affected topics
- **Missing Metadata**: Tokens with missing decimals or symbols are skipped with warnings
- **Empty Token Lists**: Returns `False` without attempting to publish

## Testing

Comprehensive test suite available at:
`src/core/storage/tests/test_token_whitelist_publisher.py`

### Test Coverage

- ✅ Full whitelist publishing
- ✅ Add delta publishing
- ✅ Remove delta publishing
- ✅ Filter counting
- ✅ Empty token handling
- ✅ Connection failures
- ✅ Publish failures
- ✅ Multiple chain support
- ✅ Delta tracking across updates
- ✅ Message structure validation

Run tests:
```bash
PYTHONPATH=/home/sam-sullivan/dynamicWhitelist uv run pytest \
  src/core/storage/tests/test_token_whitelist_publisher.py -v
```

## Configuration

### NATS Connection

Default NATS URL: `nats://localhost:4222`

Override via constructor:
```python
publisher = TokenWhitelistNatsPublisher(nats_url="nats://custom-host:4222")
```

## Performance Considerations

- **Message Size**: Full whitelist messages can be large (100KB+ for 500+ tokens)
- **Publishing Frequency**: Delta updates are efficient for frequent runs
- **Network**: Uses async I/O for non-blocking NATS operations
- **Memory**: Maintains minimal state (only previous token addresses per chain)

## Future Enhancements

Potential improvements:

1. **Filter-specific Topics**: Publish to `whitelist.tokens.{chain}.filter.{filter_type}`
2. **Compression**: Compress large full whitelist messages
3. **TTL Metadata**: Include expiration times for cached data
4. **Versioning**: Add schema version for backwards compatibility
5. **Batch Deltas**: Aggregate multiple small updates

## Related Components

- **Pool Whitelist Publisher**: `src/core/storage/pool_whitelist_publisher.py`
- **Whitelist Publisher**: `src/core/storage/whitelist_publisher.py`
- **Orchestrator**: `src/whitelist/orchestrator.py`
- **Token Builder**: `src/whitelist/builder.py`
