#!/bin/bash

# Script to set up hourly cron job for transfer processing

CRON_JOB="0 * * * * cd /home/sam-sullivan/dynamic_whitelist && ./scripts/fetch_transfers.sh 1 && uv run python processors/latest_transfers_processor.py"

echo "Setting up hourly transfer processing cron job..."
echo "This will run every hour at the top of the hour."

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "fetch_transfers.sh"; then
    echo "Cron job already exists. Removing old entry..."
    crontab -l 2>/dev/null | grep -v "fetch_transfers.sh" | crontab -
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Cron job added successfully!"
echo "Current cron jobs:"
crontab -l

echo ""
echo "The cron job will:"
echo "1. Fetch the last hour of transfer events"
echo "2. Process and store the data in the database"
echo "3. Update hourly averages for all tokens"
echo ""
echo "To remove the cron job later, run: crontab -e" 