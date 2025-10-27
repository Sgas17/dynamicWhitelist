# Transfer Processing Cron Setup Guide

Quick reference for setting up automated hourly transfer processing.

## Quick Setup (3 Steps)

### 1. Test Manually First

```bash
# Fetch and process 1 hour of transfer data
./src/scripts/fetch_transfers.sh 1
```

Expected output:
- Cryo fetches ~300 blocks (~1 hour)
- Processes ~90,000+ transfer events
- Stores data in TimescaleDB
- Shows top 10 most transferred tokens

### 2. Verify Data Storage

```bash
uv run python -c "
from src.core.storage.timescaledb import get_database_stats
stats = get_database_stats(chain_id=1)
print(f\"Raw transfers: {stats['raw_data']['total_records']} records\")
print(f\"Hourly transfers: {stats['hourly_data']['total_records']} records\")
"
```

Expected output:
```
Raw transfers: 2545 records
Hourly transfers: 2545 records
```

### 3. Setup Automated Cron Job

```bash
# Set up hourly cron job
./src/scripts/setup_transfer_cron.sh
```

## Cron Schedule

The cron job runs **every hour at 5 minutes past the hour**:

```
1:05 AM → Fetch transfers from 12:00-1:00 AM
2:05 AM → Fetch transfers from 1:00-2:00 AM
3:05 AM → Fetch transfers from 2:00-3:00 AM
...and so on
```

This 5-minute offset allows blocks to fully propagate before fetching.

## Monitoring

### Check Cron Job Status

```bash
# List active cron jobs
crontab -l | grep fetch_transfers

# Expected output:
# 5 * * * * cd /path/to/dynamicWhitelist && ./src/scripts/fetch_transfers.sh 1 >> logs/transfer_cron.log 2>&1
```

### Monitor Real-Time Logs

```bash
# Watch logs in real-time
tail -f logs/transfer_cron.log
```

### Check Recent Processing

```bash
# See last 10 processing runs
grep "Transfer Processing Complete" logs/transfer_cron.log | tail -10

# Check for errors
grep -i error logs/transfer_cron.log | tail -10
```

### View Processing Statistics

```bash
# Get current database stats
uv run python -c "
from src.core.storage.timescaledb import get_database_stats
import json
stats = get_database_stats(chain_id=1)
print(json.dumps({
    'raw_records': stats['raw_data']['total_records'],
    'hourly_records': stats['hourly_data']['total_records'],
    'earliest': str(stats['raw_data']['earliest_record']),
    'latest': str(stats['raw_data']['latest_record'])
}, indent=2))
"
```

## Troubleshooting

### Cron Job Not Running

**Check if cron service is active**:
```bash
sudo systemctl status cron
```

**Restart cron service**:
```bash
sudo systemctl restart cron
```

### No New Data in Logs

**Verify cron job syntax**:
```bash
crontab -l
```

**Test manual execution**:
```bash
# Run exactly as cron would
cd /home/sam-sullivan/dynamicWhitelist && ./src/scripts/fetch_transfers.sh 1
```

### Permission Issues

**Make scripts executable**:
```bash
chmod +x src/scripts/*.sh
chmod +x src/scripts/*.py
```

**Check log directory permissions**:
```bash
mkdir -p logs
chmod 755 logs
```

### Redis Connection Errors

Redis is optional - processing continues without it. To disable Redis updates:

Edit `src/scripts/run_transfer_processor.py:55` and set:
```python
update_cache=False  # Disable Redis cache updates
```

## Managing the Cron Job

### View Current Cron Jobs

```bash
crontab -l
```

### Edit Cron Jobs Manually

```bash
crontab -e
```

### Remove Transfer Cron Job

```bash
crontab -l | grep -v "fetch_transfers.sh" | crontab -
```

### Change Cron Schedule

Edit the minute value in `src/scripts/setup_transfer_cron.sh:17`:

```bash
# Run every hour at minute 5
CRON_JOB="5 * * * * ..."

# Run every hour at minute 15
CRON_JOB="15 * * * * ..."

# Run every 6 hours at minute 5
CRON_JOB="5 */6 * * * ..."

# Run twice a day (midnight and noon)
CRON_JOB="5 0,12 * * * ..."
```

Then re-run:
```bash
./src/scripts/setup_transfer_cron.sh
```

## Data Management

### Check Database Size

```bash
uv run python -c "
from src.core.storage.timescaledb import get_database_stats
stats = get_database_stats(chain_id=1)
for table in stats['table_sizes']:
    print(f\"{table['table']}: {table['size_pretty']}\")
"
```

### Manual Data Cleanup

```bash
# Remove old data per retention policies
uv run python -c "
from src.core.storage.timescaledb import cleanup_old_data
cleanup_old_data(chain_id=1)
"
```

### Query Top Tokens

```bash
uv run python -c "
from src.core.storage.timescaledb import get_top_tokens_by_average
tokens = get_top_tokens_by_average(hours_back=24, limit=20, chain_id=1)
print('Top 20 tokens by 24h average transfers:')
for i, token in enumerate(tokens, 1):
    print(f\"{i:2d}. {token['token_address'][:10]}... \", end='')
    print(f\"transfers={token['transfer_count']:4d} \", end='')
    print(f\"senders={token.get('unique_senders', 0):3d} \", end='')
    print(f\"receivers={token.get('unique_receivers', 0):3d}\")
"
```

## Performance Benchmarks

### Expected Performance

| Metric | Value |
|--------|-------|
| Fetch time (1 hour) | ~10 seconds |
| Processing time | ~2 seconds |
| Transfer events | ~90,000 per hour |
| Unique tokens stored | ~2,500 per hour |
| Disk usage per day | <10 MB (compressed) |
| Total cron execution | ~15 seconds per hour |

### Resource Usage

- **CPU**: Minimal (~1-2% during processing)
- **Memory**: ~200 MB during processing
- **Disk I/O**: Moderate during fetch, minimal during processing
- **Network**: ~10-20 MB per hour (RPC calls)

## Best Practices

1. **Monitor the first 24 hours**: Watch logs to ensure stable operation
2. **Set up alerts**: Monitor for gaps in data collection
3. **Backup database**: Regular backups of TimescaleDB data
4. **Keep logs rotated**: Set up logrotate for `logs/transfer_cron.log`
5. **Test after updates**: Re-run manual test after code changes

## Log Rotation Setup

Create `/etc/logrotate.d/dynamicWhitelist`:

```
/home/sam-sullivan/dynamicWhitelist/logs/transfer_cron.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0644 sam-sullivan sam-sullivan
    missingok
}
```

Test logrotate:
```bash
sudo logrotate -f /etc/logrotate.d/dynamicWhitelist
```

## Next Steps

Once the cron job is running successfully:

1. **Integrate with whitelist builder**: Use transfer data for token filtering
2. **Set up monitoring**: Add alerts for processing failures
3. **Scale to other chains**: Deploy for Base, Arbitrum, etc.
4. **API integration**: Expose top tokens via API endpoints

## References

- [Transfer Processing README](../processors/transfers/README.md) - Full system documentation
- [fetch_transfers.sh](./fetch_transfers.sh) - Data fetching script
- [run_transfer_processor.py](./run_transfer_processor.py) - Processing script
- [setup_transfer_cron.sh](./setup_transfer_cron.sh) - Cron setup script
