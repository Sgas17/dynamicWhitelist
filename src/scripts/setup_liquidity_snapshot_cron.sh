#!/bin/bash

# Liquidity Snapshot Cron Setup
#
# This script sets up a cron job to run the liquidity snapshot generator
# on a daily schedule at 2:00 AM (low traffic time).
#
# The snapshot generator runs in INCREMENTAL mode:
# - Loads existing snapshots from PostgreSQL
# - Only processes NEW events since last snapshot block
# - Fast updates (<1 minute typically)
#
# Usage:
#   bash src/scripts/setup_liquidity_snapshot_cron.sh

set -e

# Get absolute path to project directory
PROJECT_DIR=$(pwd)

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_DIR/logs"

# Define cron job (runs daily at 2:00 AM)
CRON_JOB="0 2 * * * cd $PROJECT_DIR && uv run python src/scripts/run_liquidity_snapshot_generator.py >> logs/liquidity_snapshot_cron.log 2>&1"

echo "Setting up liquidity snapshot cron job..."
echo "Project directory: $PROJECT_DIR"
echo "Schedule: Daily at 2:00 AM"
echo "Mode: Incremental updates (processes only new events)"
echo ""

# Remove existing liquidity snapshot cron jobs
echo "Removing existing liquidity snapshot cron jobs..."
(crontab -l 2>/dev/null | grep -v "run_liquidity_snapshot_generator.py") | crontab - || true

# Add new cron job
echo "Adding new cron job..."
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

# Verify cron job was added
echo ""
echo "Cron job installed successfully!"
echo ""
echo "Current cron jobs:"
crontab -l | grep "run_liquidity_snapshot_generator.py"
echo ""
echo "Logs will be written to: $PROJECT_DIR/logs/liquidity_snapshot_cron.log"
echo ""
echo "To view logs:"
echo "  tail -f logs/liquidity_snapshot_cron.log"
echo ""
echo "To manually run the snapshot generator (incremental):"
echo "  uv run python src/scripts/run_liquidity_snapshot_generator.py"
echo ""
echo "To rebuild snapshots from scratch (one-time operation):"
echo "  uv run python -c 'import asyncio; from src.processors.pools.unified_liquidity_processor import UnifiedLiquidityProcessor; p = UnifiedLiquidityProcessor(chain=\"ethereum\"); asyncio.run(p.process_liquidity_snapshots(protocol=\"uniswap_v3\", force_rebuild=True))'"
echo ""
echo "To remove the cron job:"
echo "  crontab -l | grep -v 'run_liquidity_snapshot_generator.py' | crontab -"
