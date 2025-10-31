#!/usr/bin/env python3
"""
Run the UnifiedTransferProcessor to process latest transfer events.

This script is designed to be run as a cron job to continuously update
the token transfer database with the latest data.
"""

import asyncio
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.processors.transfers.unified_transfer_processor import UnifiedTransferProcessor
from src.config import ConfigManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Run transfer processor."""
    try:
        logger.info("=" * 80)
        logger.info("Starting Transfer Processor")
        logger.info("=" * 80)

        # Initialize processor
        processor = UnifiedTransferProcessor(chain="ethereum")

        # Validate configuration
        if not processor.validate_config():
            logger.error("Configuration validation failed")
            return 1

        # Get data directory from config
        config = ConfigManager()
        data_dir = Path(config.base.DATA_DIR) / "ethereum" / "latest_transfers"

        logger.info(f"Processing transfers from: {data_dir}")

        # Process with parameters suitable for hourly cron job
        result = await processor.process(
            data_dir=str(data_dir),
            hours_back=2,  # Look back 2 hours to catch any late arrivals
            min_transfers=50,  # Minimum transfers to include token
            store_raw=True,  # Store raw 5-minute data
            update_cache=True  # Update Redis cache
        )

        if result.success:
            logger.info("=" * 80)
            logger.info("Transfer Processing Complete")
            logger.info("=" * 80)
            logger.info(f"Processed: {result.processed_count} records")
            logger.info(f"Top tokens found: {result.metadata.get('total_tokens', 0)}")
            logger.info(f"Hours analyzed: {result.metadata.get('hours_back', 0)}")
            logger.info(f"Min transfers threshold: {result.metadata.get('min_transfers', 0)}")

            # Show top 10 tokens
            if result.data:
                logger.info("\nTop 10 most transferred tokens:")
                for i, token in enumerate(result.data[:10], 1):
                    logger.info(
                        f"{i:2d}. {token['token_address'][:10]}... "
                        f"transfers={token['transfer_count']} "
                        f"senders={token.get('unique_senders', 0)} "
                        f"receivers={token.get('unique_receivers', 0)}"
                    )

            return 0
        else:
            logger.error(f"Processing failed: {result.error}")
            return 1

    except Exception as e:
        logger.exception(f"Transfer processor failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
