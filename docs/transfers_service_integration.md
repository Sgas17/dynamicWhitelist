# Transfers Service Integration

## Overview

As of this refactoring, **all ERC20 transfer event processing has been moved to a separate microservice**. The dynamicWhitelist project now queries the transfers service database directly for top transferred tokens.

## Architecture

```
┌─────────────────────────────────────────┐
│     transfers-service (separate repo)  │
│  https://github.com/Sgas17/transfers-  │
│              service                    │
│                                         │
│  - Processes Transfer events with Cryo │
│  - Aggregates statistics (24h/7d)      │
│  - Calculates ranking scores           │
│  - PostgreSQL (port 5433)              │
└──────────────┬──────────────────────────┘
               │
               │ Query top_transferred_tokens
               │ materialized view
               ▼
┌─────────────────────────────────────────┐
│      dynamicWhitelist (this repo)       │
│                                         │
│  - Queries transfers DB for top tokens │
│  - Builds whitelist from 4 sources:    │
│    1. Cross-chain tokens               │
│    2. Hyperliquid perp DEX             │
│    3. Lighter perp DEX                 │
│    4. Top transferred tokens ← NEW     │
│  - Filters pools by liquidity          │
│  - Publishes whitelist to NATS         │
└─────────────────────────────────────────┘
```

## Setup

### 1. Start the Transfers Service

```bash
# Clone the transfers service repo
cd ~
git clone https://github.com/Sgas17/transfers-service.git
cd transfers-service

# Configure environment
cp .env.example .env
# Edit .env and set RPC_URL

# Start with docker-compose
docker-compose up -d

# Check logs
docker-compose logs -f transfers-service
```

### 2. Verify Transfers Service is Running

```bash
# Check containers
docker-compose ps

# Check database connectivity
docker-compose exec postgres psql -U transfers_user -d transfers \
  -c "SELECT last_processed_block, updated_at FROM sync_state"

# Check top tokens
docker-compose exec postgres psql -U transfers_user -d transfers \
  -c "SELECT * FROM top_transferred_tokens LIMIT 10"
```

### 3. Run dynamicWhitelist

```bash
cd ~/dynamicWhitelist

# The whitelist builder will automatically connect to transfers service
uv run python -m src.whitelist.builder
```

## How It Works

### Token Whitelist Builder

The `TokenWhitelistBuilder.get_top_transferred_tokens()` method now:

1. **Connects to transfers service database** (separate connection):
   - Host: `localhost`
   - Port: `5433` (different from main DB)
   - Database: `transfers`
   - User: `transfers_user`
   - Password: `transfers_pass`

2. **Queries materialized view** for performance:
   ```sql
   SELECT
       token_address,
       transfer_count_24h,
       unique_senders_24h,
       unique_receivers_24h,
       ranking_score,
       last_updated
   FROM top_transferred_tokens
   WHERE rank <= $1
   ORDER BY rank
   ```

3. **Returns top N tokens** based on composite ranking score

### Ranking Algorithm (in transfers service)

The ranking score is calculated as:

```
score = (transfer_count_24h * 0.4) +
        (unique_senders_24h * 0.2) +
        (unique_receivers_24h * 0.2) +
        (transfer_count_7d * 0.1) +
        (unique_senders_7d * 0.05) +
        (unique_receivers_7d * 0.05)
```

This favors:
- Recent activity (24h weighted more than 7d)
- Transfer frequency
- Unique addresses (indicates real usage vs. wash trading)

## Database Configuration

### Transfers Service Database (port 5433)

- **Purpose**: Store transfer events and aggregated statistics
- **Tables**:
  - `erc20_transfers` - Raw transfer events
  - `token_transfer_stats` - Aggregated statistics per token
  - `top_transferred_tokens` - Materialized view (top 500)
  - `sync_state` - Tracks last processed block

### Main Database (port 5432)

- **Purpose**: Store pool data, coingecko data, etc.
- **Tables**: All existing dynamicWhitelist tables

## Error Handling

If the transfers service is not running or unreachable:

```python
logger.warning("Error fetching top transferred tokens")
print("This likely means the transfers service is not running or not reachable")
print("Make sure the transfers-service is running: cd ~/transfers-service && docker-compose up -d")
return set()  # Returns empty set, whitelist continues with other sources
```

The whitelist builder gracefully handles this and continues with the other 3 sources (cross-chain, Hyperliquid, Lighter).

## Monitoring

### Check Transfers Service Status

```bash
cd ~/transfers-service

# View logs
docker-compose logs -f transfers-service

# Check sync progress
docker-compose exec postgres psql -U transfers_user -d transfers \
  -c "SELECT * FROM sync_state"

# Check transfer count
docker-compose exec postgres psql -U transfers_user -d transfers \
  -c "SELECT COUNT(*) FROM erc20_transfers"

# Check unique tokens
docker-compose exec postgres psql -U transfers_user -d transfers \
  -c "SELECT COUNT(DISTINCT token_address) FROM erc20_transfers"
```

### Check dynamicWhitelist Integration

```bash
# Run whitelist builder with verbose logging
cd ~/dynamicWhitelist
uv run python -m src.whitelist.builder

# Look for this log line:
# "📊 Getting top 100 transferred tokens from transfers service..."
```

## Troubleshooting

### Issue: "Error fetching top transferred tokens"

**Cause**: Transfers service is not running or database is not reachable

**Solution**:
```bash
cd ~/transfers-service
docker-compose up -d
docker-compose logs -f transfers-service
```

### Issue: "No transfers found in database"

**Cause**: Transfers service hasn't synced any blocks yet (first run)

**Solution**: Wait for initial backfill (processes last 24 hours):
```bash
docker-compose logs -f transfers-service
# Look for: "Initial backfill complete"
```

### Issue: "Connection refused on port 5433"

**Cause**: PostgreSQL not exposed on host port 5433

**Solution**: Check docker-compose.yml:
```yaml
postgres:
  ports:
    - "5433:5432"  # Must expose port
```

### Issue: "Authentication failed for user transfers_user"

**Cause**: Incorrect credentials in dynamicWhitelist

**Solution**: Update credentials in `src/whitelist/builder.py` to match `.env` in transfers-service

## Migration Notes

### What Was Removed

- ✅ `src/processors/transfers/` - All transfer processing code
- ✅ `src/scripts/run_transfer_processor.py` - Transfer processor script
- ✅ `src/scripts/setup_*transfer_cron.sh` - Cron setup scripts
- ✅ `src/tests/integration/test_full_transfer_pipeline.py` - Transfer tests

### What Was Kept

- ✅ `token_hourly_transfers` table schema - May still exist in DB
- ✅ `token_transfer_stats` table schema - May still exist in DB
- ✅ READMEs mentioning old processor - For historical reference

### What Was Added

- ✅ New query logic in `TokenWhitelistBuilder.get_top_transferred_tokens()`
- ✅ Separate database connection to transfers service
- ✅ Integration documentation (this file)

## Benefits of This Architecture

1. **Separation of Concerns**: Transfer processing is isolated from whitelist logic
2. **Independent Scaling**: Can scale transfers service separately
3. **Independent Deployment**: Deploy/update services independently
4. **Cleaner Codebase**: Removed ~4000 lines of transfer processing code
5. **Reusability**: Other projects can query transfers service database
6. **Better Performance**: Materialized views optimized for fast queries

## Future Enhancements

1. **gRPC API**: Add gRPC service for remote queries (instead of direct DB access)
2. **Multi-Chain Support**: Extend transfers service to process multiple chains
3. **Metrics Export**: Prometheus metrics for monitoring
4. **Webhook Notifications**: Alert on new high-activity tokens
5. **Token Metadata**: Fetch symbol/decimals from RPC and cache
