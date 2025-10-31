#!/bin/bash
# Setup automated pool creation monitoring
# Run this script to install the hourly cron job

set -e

PROJECT_DIR=$(pwd)
SCRIPT_PATH="$PROJECT_DIR/src/scripts/run_pool_creation_monitor.py"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/pool_creation_cron.log"

# Ensure script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Error: Script not found at $SCRIPT_PATH"
    exit 1
fi

# Create logs directory
mkdir -p "$LOG_DIR"

# Cron job: Run every hour at 10 minutes past
CRON_JOB="10 * * * * cd $PROJECT_DIR && uv run python src/scripts/run_pool_creation_monitor.py >> $LOG_FILE 2>&1"

# Remove existing pool creation monitor job (if any)
echo "Removing existing pool creation monitor cron jobs..."
(crontab -l 2>/dev/null | grep -v "run_pool_creation_monitor.py") | crontab - || true

# Add new job
echo "Installing pool creation monitor cron job..."
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo ""
echo "✅ Pool creation monitoring cron job installed successfully!"
echo ""
echo "Schedule: Every hour at 10 minutes past"
echo "Log file: $LOG_FILE"
echo "Script: $SCRIPT_PATH"
echo ""
echo "To view logs in real-time:"
echo "  tail -f $LOG_FILE"
echo ""
echo "To remove the cron job:"
echo "  crontab -e  # Then delete the line containing 'run_pool_creation_monitor.py'"
echo ""
echo "Current cron jobs:"
crontab -l | grep -E "(run_pool_creation_monitor|run_transfer_processor|run_liquidity_snapshot)" || echo "  (none found)"
echo ""
