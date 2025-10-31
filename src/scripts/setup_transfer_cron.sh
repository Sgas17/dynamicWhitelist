#!/bin/bash
#
# Setup cron job for hourly transfer processing
#
# This script configures a cron job that:
# 1. Fetches the latest hour of transfer events from Ethereum using cryo
# 2. Processes transfers and stores in TimescaleDB
# 3. Aggregates to hourly data with rolling averages
# 4. Updates Redis cache for fast API access

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Define cron job - runs every hour at minute 5
CRON_JOB="5 * * * * cd ${PROJECT_ROOT} && ${SCRIPT_DIR}/fetch_transfers.sh 1 >> ${PROJECT_ROOT}/logs/transfer_cron.log 2>&1"

echo "=" * 80
echo "Transfer Processing Cron Setup"
echo "=" * 80
echo ""
echo "Project root: ${PROJECT_ROOT}"
echo "Script location: ${SCRIPT_DIR}/fetch_transfers.sh"
echo ""
echo "This will set up an hourly cron job that:"
echo "  1. Fetches the last hour of transfer events"
echo "  2. Stores raw 5-minute transfer data in TimescaleDB"
echo "  3. Aggregates to hourly data with rolling averages"
echo "  4. Updates Redis cache with top tokens"
echo ""

# Create logs directory if it doesn't exist
mkdir -p "${PROJECT_ROOT}/logs"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "fetch_transfers.sh"; then
    echo "⚠️  Existing transfer cron job found. Removing..."
    crontab -l 2>/dev/null | grep -v "fetch_transfers.sh" | crontab -
    echo "✅ Removed old cron job"
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo ""
echo "✅ Cron job added successfully!"
echo ""
echo "Schedule: Every hour at 5 minutes past (e.g., 1:05, 2:05, 3:05)"
echo "Log file: ${PROJECT_ROOT}/logs/transfer_cron.log"
echo ""
echo "Current cron jobs:"
crontab -l | grep -E "fetch_transfers|MINUTE"

echo ""
echo "=" * 80
echo "Next Steps:"
echo "=" * 80
echo "1. Verify TimescaleDB is running and configured"
echo "2. Verify Redis is running (for caching)"
echo "3. Test manually first: ${SCRIPT_DIR}/fetch_transfers.sh 1"
echo "4. Monitor logs: tail -f ${PROJECT_ROOT}/logs/transfer_cron.log"
echo ""
echo "To remove the cron job: crontab -e"
echo "=" * 80
