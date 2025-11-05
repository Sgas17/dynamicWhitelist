"""
Pool Token Metadata Processor

Borrows proven patterns from scrape_new_token_metadata.py but adapted for:
- Processing 485,119 missing pool tokens
- Using project's ConfigManager and modular architecture
- Scalable batch processing with progress tracking
- Resume capability for large-scale processing
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from eth_abi.abi import encode
from sqlalchemy import create_engine, text
from web3 import Web3

from src.config.manager import ConfigManager
from src.processors.base import BaseProcessor, ProcessorResult

logger = logging.getLogger(__name__)


@dataclass
class TokenProcessingStats:
    """Track processing statistics."""

    total_tokens: int = 0
    processed: int = 0
    successful: int = 0
    failed: int = 0
    batches_completed: int = 0

    @property
    def success_rate(self) -> float:
        return (self.successful / self.processed * 100) if self.processed > 0 else 0.0

    @property
    def progress_percent(self) -> float:
        return (
            (self.processed / self.total_tokens * 100) if self.total_tokens > 0 else 0.0
        )


class PoolTokenMetadataProcessor(BaseProcessor):
    """
    Scalable processor for pool token metadata using proven patterns.

    Based on scrape_new_token_metadata.py but optimized for:
    - Large scale (485K+ tokens)
    - Project integration
    - Resume capability
    - Better error handling
    """

    def __init__(self, chain: str = "ethereum"):
        """Initialize processor."""
        super().__init__(chain, "pool_token_metadata")
        self.config = ConfigManager()

        # Get chain config and setup Web3
        chain_config = self.config.chains.get_chain_config(chain)
        self.rpc_url = chain_config["rpc_url"]
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_url))

        # Database connection (using same pattern as existing scraper)
        # Use environment variables like the original scraper
        import os

        self.db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'whitelist')}"
        self.engine = create_engine(self.db_uri)

        # Processing state
        self.stats = TokenProcessingStats()
        self.new_token_data = []

        logger.info(f"Initialized for {chain} using RPC: {self.rpc_url}")

    def validate_config(self) -> bool:
        """Validate processor configuration."""
        try:
            if not self.web3.is_connected():
                logger.error(f"Web3 connection failed to {self.rpc_url}")
                return False

            # Test database connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            return True
        except Exception as e:
            logger.error(f"Config validation failed: {e}")
            return False

    async def _process_single_event(self, event_data: Dict) -> Optional[Dict]:
        """Required by BaseProcessor - not used for this token processor."""
        return None

    async def process(
        self,
        token_file: str = "/tmp/claude/missing_token_metadata.txt",
        batch_size: int = 50,  # Same as existing scraper
        start_index: int = 0,
        max_tokens: Optional[int] = None,
        save_progress_every: int = 10,  # Save progress every N batches
        **kwargs,
    ) -> ProcessorResult:
        """
        Process missing token metadata using proven patterns.

        Args:
            token_file: File with missing token addresses
            batch_size: Tokens per batch (50 works well from existing scraper)
            start_index: Resume from this index
            max_tokens: Limit processing (for testing)
            save_progress_every: Save progress every N batches
        """
        try:
            logger.info("Starting pool token metadata processing")

            # Load missing tokens
            missing_tokens = self._load_missing_tokens(token_file)
            if not missing_tokens:
                return ProcessorResult(
                    success=True,
                    processed_count=0,
                    metadata={"message": "No tokens to process"},
                )

            # Apply start index and limits
            if start_index > 0:
                missing_tokens = missing_tokens[start_index:]
                logger.info(f"Resuming from index {start_index}")

            if max_tokens:
                missing_tokens = missing_tokens[:max_tokens]
                logger.info(f"Limited to {max_tokens} tokens")

            # Initialize stats
            self.stats.total_tokens = len(missing_tokens)
            logger.info(
                f"Processing {self.stats.total_tokens:,} tokens in batches of {batch_size}"
            )

            # Setup database tables (borrowed pattern)
            self._setup_database_tables()

            # Process in batches
            for i in range(0, len(missing_tokens), batch_size):
                batch = missing_tokens[i : i + batch_size]
                batch_num = self.stats.batches_completed + 1
                total_batches = (len(missing_tokens) + batch_size - 1) // batch_size

                logger.info(
                    f"Processing batch {batch_num}/{total_batches} ({len(batch)} tokens)"
                )

                # Process batch using on-chain calls (borrowed pattern)
                batch_success = await self._process_token_batch(batch)

                # Update stats
                self.stats.processed += len(batch)
                self.stats.successful += batch_success
                self.stats.failed += len(batch) - batch_success
                self.stats.batches_completed += 1

                # Log progress
                logger.info(
                    f"Batch {batch_num} complete - "
                    f"Success: {batch_success}/{len(batch)}, "
                    f"Overall: {self.stats.successful:,}/{self.stats.processed:,} "
                    f"({self.stats.success_rate:.1f}%), "
                    f"Progress: {self.stats.progress_percent:.1f}%"
                )

                # Save progress periodically
                if batch_num % save_progress_every == 0:
                    await self._save_progress_checkpoint(
                        start_index + self.stats.processed
                    )

                # Respectful delay between batches (borrowed pattern)
                await asyncio.sleep(3)

            # Final save and summary
            if self.new_token_data:
                self._insert_token_data()

            await self._save_final_results()

            metadata = {
                "total_processed": self.stats.processed,
                "successful": self.stats.successful,
                "failed": self.stats.failed,
                "success_rate": self.stats.success_rate,
                "batches_completed": self.stats.batches_completed,
                "start_index": start_index,
            }

            logger.info(f"Processing complete: {metadata}")

            return ProcessorResult(
                success=True,
                data=self.new_token_data,
                processed_count=self.stats.successful,
                metadata=metadata,
            )

        except Exception as e:
            error_msg = f"Pool token metadata processing failed: {e}"
            logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)

    def _load_missing_tokens(self, token_file: str) -> List[str]:
        """Load missing tokens from file."""
        try:
            with open(token_file, "r") as f:
                tokens = [line.strip().lower() for line in f if line.strip()]

            # Filter valid addresses (borrowed validation pattern)
            valid_tokens = []
            for token in tokens:
                if (
                    self.web3.is_address(token)
                    and token != "0x0000000000000000000000000000000000000000"
                ):
                    valid_tokens.append(token)

            logger.info(f"Loaded {len(valid_tokens):,} valid tokens from {token_file}")
            return valid_tokens

        except Exception as e:
            logger.error(f"Failed to load tokens from {token_file}: {e}")
            return []

    def _setup_database_tables(self):
        """Setup database tables (borrowed pattern from existing scraper)."""
        try:
            # Use same table structure as existing scraper
            platforms_table = "missing_coingecko_tokens_platforms"
            ethereum_table = "missing_coingecko_tokens_ethereum"

            # Create platforms table (matches coingecko_token_platforms)
            create_platforms_sql = f"""
            CREATE TABLE IF NOT EXISTS {platforms_table} (
                address VARCHAR(255) NOT NULL,
                token_id VARCHAR(255) NOT NULL,
                decimals INTEGER,
                platform VARCHAR(255),
                total_supply NUMERIC(78, 0) DEFAULT 0,
                PRIMARY KEY (address, token_id)
            );
            """

            # Create ethereum table (matches network_1_erc20_metadata)
            create_ethereum_sql = f"""
            CREATE TABLE IF NOT EXISTS {ethereum_table} (
                address VARCHAR(42) PRIMARY KEY,
                symbol VARCHAR(50),
                decimals INTEGER,
                name VARCHAR(255)
            );
            """

            with self.engine.connect() as conn:
                conn.execute(text(create_platforms_sql))
                conn.execute(text(create_ethereum_sql))
                conn.commit()

            logger.info("Database tables setup completed")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    async def _process_token_batch(self, token_addresses: List[str]) -> int:
        """
        Process batch of tokens using on-chain calls with fallback for problematic tokens.
        Borrowed pattern from get_token_info_from_blockchain().
        """
        successful_count = 0

        # First try batch processing
        try:
            successful_count = await self._try_batch_processing(token_addresses)
            if successful_count > 0:
                return successful_count
        except Exception as e:
            logger.warning(f"Batch processing failed: {e}, trying individual calls")

        # Fallback to individual processing for problematic tokens
        for address in token_addresses:
            try:
                if await self._process_single_token(address):
                    successful_count += 1
            except Exception as e:
                logger.debug(f"Individual token {address} failed: {e}")
                continue

        return successful_count

    async def _try_batch_processing(self, token_addresses: List[str]) -> int:
        """Try batch processing first (borrowed pattern)."""
        # Load BatchTokenInfo bytecode (borrowed pattern)
        batch_token_info_path = (
            Path.home() / "dynamic_whitelist" / "data" / "BatchTokenInfo.json"
        )

        if not batch_token_info_path.exists():
            logger.error("BatchTokenInfo.json not found")
            return 0

        with open(batch_token_info_path, "r") as f:
            batch_token_info = json.load(f)
            contract_bytecode = batch_token_info["bytecode"]["object"]

        # Encode addresses for batch call (borrowed pattern)
        tokens_encoded = encode(["address[]"], [token_addresses])
        input_data = contract_bytecode + tokens_encoded.hex()

        # Make the batch call
        encoded_return_data = self.web3.eth.call({"data": input_data})

        # Process results (borrowed decoding pattern)
        successful_count = 0
        for i, address in enumerate(token_addresses):
            try:
                # Each token info is 4 x 32 bytes = 128 bytes
                start_idx = i * 128
                name_bytes = encoded_return_data[start_idx : start_idx + 32]
                symbol_bytes = encoded_return_data[start_idx + 32 : start_idx + 64]
                decimals_bytes = encoded_return_data[start_idx + 64 : start_idx + 96]
                total_supply_bytes = encoded_return_data[
                    start_idx + 96 : start_idx + 128
                ]

                # Decode token info (borrowed pattern)
                token_info = {}
                try:
                    token_info["name"] = name_bytes.split(b"\x00")[0].decode("utf-8")
                except (UnicodeDecodeError, IndexError):
                    token_info["name"] = "Unknown Token"

                try:
                    token_info["symbol"] = symbol_bytes.split(b"\x00")[0].decode(
                        "utf-8"
                    )
                except (UnicodeDecodeError, IndexError):
                    token_info["symbol"] = "UNK"

                token_info["decimals"] = (
                    int(decimals_bytes[-1]) if decimals_bytes else 0
                )
                token_info["total_supply"] = (
                    int.from_bytes(total_supply_bytes, "big")
                    if total_supply_bytes
                    else 0
                )

                # Skip tokens with no valid data
                if (
                    not token_info["name"]
                    or not token_info["symbol"]
                    or token_info["symbol"] == "UNK"
                ):
                    continue

                # Format for insertion (borrowed pattern)
                token_data = {
                    "name": token_info["name"],
                    "symbol": token_info["symbol"].upper(),
                    "decimals": token_info["decimals"],
                    "total_supply": str(token_info["total_supply"]),
                    "platforms": {"ethereum": address.lower()},
                }

                self.new_token_data.append(token_data)
                successful_count += 1

            except Exception as e:
                logger.debug(f"Error processing token {address} in batch: {e}")
                continue

        return successful_count

    async def _process_single_token(self, address: str) -> bool:
        """Fallback: process individual token with direct contract calls."""
        try:
            # Try individual contract calls
            name = await self._call_token_function(address, "name()")
            symbol = await self._call_token_function(address, "symbol()")
            decimals = await self._call_token_function(address, "decimals()")

            if not name or not symbol:
                return False

            # Format for insertion
            token_data = {
                "name": name,
                "symbol": symbol.upper(),
                "decimals": decimals or 18,  # Default to 18 if not available
                "total_supply": "0",  # Skip total supply for individual calls
                "platforms": {"ethereum": address.lower()},
            }

            self.new_token_data.append(token_data)
            return True

        except Exception as e:
            logger.debug(f"Individual token processing failed for {address}: {e}")
            return False

    async def _call_token_function(
        self, address: str, function_signature: str
    ) -> Optional[str]:
        """Make individual contract function calls."""
        try:
            from web3 import Web3

            if function_signature == "name()":
                function_selector = "0x06fdde03"
            elif function_signature == "symbol()":
                function_selector = "0x95d89b41"
            elif function_signature == "decimals()":
                function_selector = "0x313ce567"
            else:
                return None

            result = self.web3.eth.call({"to": address, "data": function_selector})

            if function_signature == "decimals()":
                return int.from_bytes(result, "big") if result else 18

            # Decode string result
            if len(result) >= 64:
                # Skip the first 32 bytes (offset) and next 32 bytes (length)
                length = int.from_bytes(result[32:64], "big")
                string_data = result[64 : 64 + length]
                return string_data.decode("utf-8").strip()

            return None

        except Exception as e:
            logger.debug(
                f"Function call {function_signature} failed for {address}: {e}"
            )
            return None

    def _insert_token_data(self):
        """Insert token data using borrowed pattern from existing scraper."""
        if not self.new_token_data:
            return

        logger.info(f"Inserting {len(self.new_token_data)} tokens into database")

        try:
            with self.engine.connect() as conn:
                for token in self.new_token_data:
                    # Insert into ethereum table (borrowed pattern)
                    for platform, platform_address in token["platforms"].items():
                        platform_table = f"missing_coingecko_tokens_{platform}"
                        platform_sql = f"""
                        INSERT INTO {platform_table}
                        (address, symbol, decimals, name)
                        VALUES (:address, :symbol, :decimals, :name)
                        ON CONFLICT (address) DO UPDATE SET
                            symbol = EXCLUDED.symbol,
                            decimals = EXCLUDED.decimals,
                            name = EXCLUDED.name;
                        """

                        conn.execute(
                            text(platform_sql),
                            {
                                "address": platform_address,
                                "symbol": token.get("symbol", "UNK"),
                                "decimals": token.get("decimals", 0),
                                "name": token.get("name", "Unknown Token"),
                            },
                        )

                        # Insert into platforms table (borrowed pattern)
                        platforms_sql = """
                        INSERT INTO missing_coingecko_tokens_platforms
                        (address, token_id, decimals, platform, total_supply)
                        VALUES (:address, :token_id, :decimals, :platform, :total_supply)
                        ON CONFLICT (address, token_id) DO UPDATE SET
                            decimals = EXCLUDED.decimals,
                            platform = EXCLUDED.platform,
                            total_supply = EXCLUDED.total_supply;
                        """

                        total_supply = token.get("total_supply", "0")
                        if isinstance(total_supply, str):
                            try:
                                total_supply = int(total_supply)
                            except ValueError:
                                total_supply = 0

                        conn.execute(
                            text(platforms_sql),
                            {
                                "address": platform_address,
                                "token_id": token["symbol"],
                                "decimals": token.get("decimals", 0),
                                "platform": platform,
                                "total_supply": total_supply,
                            },
                        )

                conn.commit()
                logger.info("Successfully inserted all token data")

        except Exception as e:
            logger.error(f"Error inserting token data: {e}")
            raise

    async def _save_progress_checkpoint(self, current_index: int):
        """Save progress checkpoint for resumability."""
        try:
            checkpoint = {
                "timestamp": time.time(),
                "current_index": current_index,
                "stats": {
                    "processed": self.stats.processed,
                    "successful": self.stats.successful,
                    "failed": self.stats.failed,
                    "success_rate": self.stats.success_rate,
                },
            }

            with open("/tmp/claude/token_processing_checkpoint.json", "w") as f:
                json.dump(checkpoint, f, indent=2)

            logger.info(f"Progress checkpoint saved at index {current_index}")

        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    async def _save_final_results(self):
        """Save final results (borrowed pattern)."""
        try:
            results = {
                "total_tokens": self.stats.total_tokens,
                "processed": self.stats.processed,
                "successful": self.stats.successful,
                "failed": self.stats.failed,
                "success_rate": self.stats.success_rate,
                "batches_completed": self.stats.batches_completed,
                "token_data_count": len(self.new_token_data),
            }

            with open("/tmp/claude/pool_token_processing_results.json", "w") as f:
                json.dump(results, f, indent=2)

            logger.info("Final results saved")

        except Exception as e:
            logger.warning(f"Failed to save results: {e}")


# Convenience function for CLI usage
async def process_pool_token_metadata(
    chain: str = "ethereum",
    batch_size: int = 50,
    start_index: int = 0,
    max_tokens: Optional[int] = None,
) -> ProcessorResult:
    """
    Convenience function to process pool token metadata.

    Args:
        chain: Chain name
        batch_size: Tokens per batch
        start_index: Resume from index
        max_tokens: Limit for testing
    """
    processor = PoolTokenMetadataProcessor(chain)
    return await processor.process(
        batch_size=batch_size, start_index=start_index, max_tokens=max_tokens
    )
