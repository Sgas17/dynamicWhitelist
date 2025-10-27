"""
Unified Transfer Processor combining TimescaleDB storage with BaseProcessor pattern.

This consolidates the functionality from:
- token_transfer_timeseries.py (TimescaleDB core)  
- transfer_processors.py (BaseProcessor pattern)
- latest_transfers_processor.py (Redis caching)
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import redis.asyncio as redis

from ..base import BaseProcessor, ProcessorResult
from ...config import ConfigManager
from ...core.storage.timescaledb import (
    setup_timescale_tables,
    store_raw_transfers,
    aggregate_raw_to_hourly,
    get_top_tokens_by_average,
    get_timescale_engine,
    get_database_stats as get_timescale_stats
)
from ...utils.token_blacklist_manager import TokenBlacklistManager
from sqlalchemy import text


class UnifiedTransferProcessor(BaseProcessor):
    """
    Unified processor for token transfers using TimescaleDB storage.
    
    Features:
    - Processes Parquet files with transfer data
    - Stores raw 5-minute data in TimescaleDB
    - Aggregates to hourly data with rolling averages
    - Caches results in Redis for fast access
    - Compatible with BaseProcessor pattern
    """
    
    def __init__(self, chain: str = "ethereum", enable_blacklist: bool = True):
        """
        Initialize unified transfers processor.

        Args:
            chain: Chain name (e.g., "ethereum", "base")
            enable_blacklist: Enable phishing/scam token filtering
        """
        super().__init__(chain, "transfers")
        self.config = ConfigManager()
        # Get chain_id from config based on chain name
        self.chain_id = self.config.chains.get_chain_id(self.chain)
        self._setup_timescale()

        # Initialize blacklist manager for filtering phishing/scam tokens
        self.enable_blacklist = enable_blacklist
        self.blacklist_manager = None
        if enable_blacklist:
            self._setup_blacklist()

    def _setup_timescale(self):
        """Setup TimescaleDB tables on initialization."""
        try:
            setup_timescale_tables(self.chain_id)
            self.logger.info(f"TimescaleDB tables initialized for chain {self.chain_id}")
        except Exception as e:
            self.logger.error(f"Failed to setup TimescaleDB: {e}")
            raise

    def _setup_blacklist(self):
        """Setup token blacklist manager."""
        try:
            self.blacklist_manager = TokenBlacklistManager(
                blacklist_file=f"data/token_blacklist_{self.chain}.json",
                cache_file=f"data/etherscan_cache/etherscan_labels_{self.chain}.json",
                auto_update=False  # Manual updates only
            )
            blacklist_count = len(self.blacklist_manager.blacklist)
            self.logger.info(
                f"Token blacklist initialized with {blacklist_count} entries for {self.chain}"
            )
        except Exception as e:
            self.logger.warning(f"Failed to setup blacklist manager: {e}")
            self.enable_blacklist = False
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        try:
            # Test TimescaleDB connection
            engine = get_timescale_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    async def process(
        self,
        data_dir: Optional[str] = None,
        hours_back: int = 24,
        min_transfers: int = 100,
        store_raw: bool = True,
        update_cache: bool = True
    ) -> ProcessorResult:
        """
        Process transfer events from Parquet files.
        
        Args:
            data_dir: Directory containing transfer parquet files
            hours_back: Hours to look back for processing
            min_transfers: Minimum transfers to include token
            store_raw: Whether to store raw 5-minute data
            update_cache: Whether to update Redis cache
            
        Returns:
            ProcessorResult with processing statistics
        """
        try:
            # Setup data directory
            if not data_dir:
                data_dir = Path(self.config.base.DATA_DIR)
            else:
                data_dir = Path(data_dir)
            
            if not data_dir.exists():
                return ProcessorResult(
                    success=False,
                    error=f"Data directory not found: {data_dir}"
                )
            
            # Find and process transfer files
            transfer_data = await self._load_transfer_data(data_dir, hours_back)
            
            if not transfer_data:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No transfer data found"}
                )
            
            processed_count = 0
            
            # Store raw data if requested
            if store_raw:
                raw_count = await self._store_raw_data(transfer_data)
                processed_count += raw_count
            
            # Aggregate to hourly and get top tokens
            top_tokens = await self._aggregate_and_get_top_tokens(
                transfer_data, min_transfers
            )
            
            # Update Redis cache if requested
            if update_cache and top_tokens:
                await self._update_redis_cache(top_tokens)
            
            return ProcessorResult(
                success=True,
                data=top_tokens,
                processed_count=processed_count,
                metadata={
                    "hours_back": hours_back,
                    "min_transfers": min_transfers,
                    "total_tokens": len(top_tokens),
                    "raw_stored": store_raw,
                    "cache_updated": update_cache
                }
            )
            
        except Exception as e:
            error_msg = f"Transfer processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _load_transfer_data(
        self, 
        data_dir: Path, 
        hours_back: int
    ) -> List[Dict[str, Any]]:
        """Load and filter transfer data from Parquet files."""
        
        # Look for multiple file patterns to support different data sources
        transfer_files = []
        
        # Pattern 1: Our expected pattern
        transfer_files.extend(list(data_dir.glob("**/transfers_*.parquet")))
        
        # Pattern 2: Cryo log files (ethereum__logs__*.parquet)
        cryo_log_files = list(data_dir.glob("**/ethereum__logs__*.parquet"))
        
        # Pattern 3: Any logs files that might contain transfers
        generic_log_files = list(data_dir.glob("**/*logs*.parquet"))
        
        # Combine all patterns, removing duplicates
        all_potential_files = set(transfer_files + cryo_log_files + generic_log_files)
        
        if not all_potential_files:
            self.logger.warning("No transfer or log parquet files found")
            self.logger.info(f"Searched in: {data_dir}")
            self.logger.info(f"Patterns tried: transfers_*.parquet, ethereum__logs__*.parquet, *logs*.parquet")
            return []
        
        self.logger.info(f"Found {len(all_potential_files)} potential transfer data files")
        
        # Read and validate transfer files
        all_transfers_list = []
        for file in all_potential_files:
            try:
                df = pl.read_parquet(file)
                
                # Check if this looks like transfer data
                if self._is_transfer_data(df):
                    self.logger.info(f"Using transfer data from: {file.name}")
                    
                    # Normalize data format - convert binary fields to hex strings
                    df = self._normalize_data_format(df)
                    
                    # Check if this is raw log data that needs parsing
                    if "token_address" not in df.columns and "topic1" in df.columns:
                        self.logger.info(f"Parsing raw transfer logs from: {file.name}")
                        df = self._parse_raw_transfer_logs(df)
                    
                    # Filter by time if timestamp exists
                    if "timestamp" in df.columns:
                        cutoff_time = datetime.now() - timedelta(hours=hours_back)
                        df = df.filter(pl.col("timestamp") >= cutoff_time)
                    
                    # Convert to dict format to avoid schema conflicts
                    transfers = df.to_dicts()
                    all_transfers_list.extend(transfers)
                    self.logger.debug(f"Added {len(transfers)} transfers from {file.name}")
                else:
                    self.logger.debug(f"Skipping non-transfer data: {file.name}")
                    
            except Exception as e:
                self.logger.warning(f"Failed to read {file}: {e}")
        
        if not all_transfers_list:
            self.logger.warning("No valid transfer data found in parquet files")
            return []
        
        self.logger.info(f"Loaded {len(all_transfers_list)} total transfer records")
        return all_transfers_list

    def _is_transfer_data(self, df: pl.DataFrame) -> bool:
        """
        Check if a DataFrame contains ERC20 transfer event data.
        
        Args:
            df: Polars DataFrame to check
            
        Returns:
            True if the DataFrame appears to contain transfer events
        """
        # Check for transfer event signature (topic0)
        transfer_topic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        
        # Must have the basic log structure
        required_cols = {"address", "topic0", "data"}
        if not required_cols.issubset(set(df.columns)):
            return False
        
        # Check if any rows have the Transfer event topic0
        if "topic0" in df.columns:
            try:
                # Handle both hex string and binary formats
                if df["topic0"].dtype == pl.Binary:
                    # Convert binary to hex for comparison
                    transfer_rows = df.filter(
                        pl.col("topic0").bin.encode("hex").str.to_lowercase() == transfer_topic0[2:].lower()
                    )
                else:
                    # String format (hex)
                    transfer_rows = df.filter(
                        pl.col("topic0").str.to_lowercase() == transfer_topic0.lower()
                    )
                
                if len(transfer_rows) > 0:
                    self.logger.debug(f"Found {len(transfer_rows)} transfer events in file")
                    return True
                    
            except Exception as e:
                self.logger.debug(f"Error checking transfer data: {e}")
        
        return False

    def _normalize_data_format(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize data format to ensure compatibility between different data sources.
        
        Converts binary fields to hex strings and handles data type inconsistencies.
        Key issue: File 1 has Binary types, File 2 has String types - need to standardize.
        
        Args:
            df: Polars DataFrame to normalize
            
        Returns:
            Normalized DataFrame with consistent data types
        """
        try:
            # Fields that should be hex strings, not binary
            hex_fields = ["transaction_hash", "address", "topic0", "topic1", "topic2", "topic3", "data"]
            
            df_normalized = df.clone()
            
            for field in hex_fields:
                if field in df_normalized.columns:
                    current_dtype = df_normalized[field].dtype
                    
                    if current_dtype == pl.Binary:
                        # Convert binary to hex string with 0x prefix
                        df_normalized = df_normalized.with_columns([
                            pl.concat_str([
                                pl.lit("0x"), 
                                pl.col(field).bin.encode("hex")
                            ]).str.to_lowercase().alias(field)
                        ])
                        self.logger.debug(f"Converted {field} from Binary to hex String")
                        
                    elif current_dtype == pl.Utf8:
                        # Ensure hex strings are lowercase and properly formatted
                        df_normalized = df_normalized.with_columns([
                            pl.when(pl.col(field).is_not_null())
                            .then(
                                pl.when(pl.col(field).str.starts_with("0x"))
                                .then(pl.col(field).str.to_lowercase())
                                .otherwise(pl.concat_str([pl.lit("0x"), pl.col(field).str.to_lowercase()]))
                            )
                            .otherwise(pl.lit(None))
                            .alias(field)
                        ])
                        self.logger.debug(f"Normalized {field} String format")
                    
                    else:
                        # Cast other types to string and add 0x prefix if needed
                        df_normalized = df_normalized.with_columns([
                            pl.col(field).cast(pl.Utf8, strict=False).alias(field)
                        ])
                        self.logger.debug(f"Cast {field} from {current_dtype} to String")
            
            # Ensure numeric fields are consistent integers
            numeric_fields = ["block_number", "transaction_index", "log_index", "n_data_bytes", "chain_id"]
            for field in numeric_fields:
                if field in df_normalized.columns:
                    df_normalized = df_normalized.with_columns([
                        pl.col(field).cast(pl.Int64, strict=False).alias(field)
                    ])
            
            self.logger.debug(f"Normalized DataFrame schema: {dict(zip(df_normalized.columns, df_normalized.dtypes))}")
            return df_normalized
            
        except Exception as e:
            self.logger.warning(f"Failed to normalize data format: {e}", exc_info=True)
            return df

    def _parse_raw_transfer_logs(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Parse raw ERC20 transfer logs into structured transfer data.
        
        Based on legacy_jared_analysis.py parsing logic.
        ERC20 Transfer event structure:
        - topic0: Transfer event signature (0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef)
        - topic1: from address (32 bytes, address in last 20 bytes)  
        - topic2: to address (32 bytes, address in last 20 bytes)
        - data: amount (32 bytes, uint256)
        - address: token contract address
        """
        try:
            self.logger.info(f"Starting to parse {len(df)} raw transfer logs")
            self.logger.info(f"Original columns: {df.columns}")
            
            # Filter to only transfer events - handle None values
            transfer_topic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            
            transfer_df = df.filter(
                (pl.col("topic0").is_not_null()) & 
                (pl.col("topic0").str.to_lowercase() == transfer_topic0.lower())
            )
            
            if len(transfer_df) == 0:
                self.logger.warning("No transfer events found after filtering")
                return transfer_df
            
            self.logger.info(f"Found {len(transfer_df)} transfer events to parse")
            
            # Parse transfer fields from raw log data 
            parsed_df = transfer_df.with_columns([
                # Token address from the contract that emitted the log (already has 0x prefix)
                pl.col("address").str.to_lowercase().alias("token_address"),
                
                # From address - last 20 bytes (40 hex chars) of topic1, removing 0x prefix first
                pl.when(pl.col("topic1").is_not_null())
                .then(
                    pl.concat_str([
                        pl.lit("0x"),
                        pl.col("topic1").str.replace("0x", "").str.slice(-40)
                    ]).str.to_lowercase()
                )
                .otherwise(pl.lit("0x0000000000000000000000000000000000000000"))
                .alias("from_address"),
                
                # To address - last 20 bytes (40 hex chars) of topic2, removing 0x prefix first  
                pl.when(pl.col("topic2").is_not_null())
                .then(
                    pl.concat_str([
                        pl.lit("0x"),
                        pl.col("topic2").str.replace("0x", "").str.slice(-40)
                    ]).str.to_lowercase()
                )
                .otherwise(pl.lit("0x0000000000000000000000000000000000000000"))
                .alias("to_address"),
                
                # Amount from data field - convert hex to decimal string, handle None
                pl.when(pl.col("data").is_not_null())
                .then(
                    pl.col("data").str.replace("0x", "").map_elements(
                        lambda x: str(int(x, 16)) if x and len(x) > 0 else "0",
                        return_dtype=pl.Utf8
                    )
                )
                .otherwise(pl.lit("0"))
                .alias("amount")
            ])
            
            self.logger.info(f"Parsed columns: {parsed_df.columns}")
            
            # Show sample parsed data for verification
            if len(parsed_df) > 0:
                sample = parsed_df.select(['token_address', 'from_address', 'to_address', 'amount']).head(2)
                self.logger.info(f"Sample parsed data:\n{sample}")
            
            return parsed_df
            
        except Exception as e:
            self.logger.error(f"Failed to parse raw transfer logs: {e}", exc_info=True)
            return df
    
    async def _store_raw_data(self, transfer_data: List[Dict[str, Any]]) -> int:
        """Store raw transfer data in TimescaleDB."""
        # Group transfers by 5-minute intervals
        current_time = datetime.now()
        interval_start = current_time.replace(
            minute=(current_time.minute // 5) * 5,
            second=0,
            microsecond=0
        )
        
        # Debug: Check data consistency before creating DataFrame
        self.logger.info(f"Attempting to store {len(transfer_data)} transfer records")
        
        if not transfer_data:
            return 0
            
        # Check for schema inconsistencies in critical fields
        critical_fields = ["token_address", "from_address", "to_address", "amount", "transaction_hash"]
        field_types = {}
        
        for field in critical_fields:
            types_seen = set()
            for record in transfer_data[:100]:  # Sample first 100 records
                if field in record and record[field] is not None:
                    types_seen.add(type(record[field]).__name__)
            field_types[field] = types_seen
        
        self.logger.info(f"Field type analysis: {field_types}")
        
        # Create DataFrame with explicit schema handling
        try:
            # Use infer_schema_length to handle mixed types better
            df = pl.DataFrame(transfer_data, infer_schema_length=len(transfer_data))
            self.logger.info("Created DataFrame successfully with normalized schema")
        except Exception as e:
            self.logger.error(f"Failed to create DataFrame, trying manual schema normalization: {e}")
            
            # Manual normalization approach
            normalized_data = []
            for record in transfer_data:
                normalized_record = {}
                for key, value in record.items():
                    # Ensure all critical fields are strings
                    if key in critical_fields and value is not None:
                        normalized_record[key] = str(value)
                    else:
                        normalized_record[key] = value
                normalized_data.append(normalized_record)
            
            # Try again with normalized data
            df = pl.DataFrame(normalized_data, infer_schema_length=len(normalized_data))
            self.logger.info("Created DataFrame with manual normalization")
        
        if df.height == 0:
            return 0

        # Load MEV addresses for tracking
        mev_addresses = self._load_mev_addresses()

        # Add MEV flag column
        df = df.with_columns([
            (
                pl.col("from_address").is_in(mev_addresses) |
                pl.col("to_address").is_in(mev_addresses)
            ).alias("is_mev_transfer")
        ])

        # Group by token and aggregate - use "amount" not "value"
        # Cast amount to numeric before summing (it's stored as string from hex conversion)
        aggregated = (
            df.group_by("token_address")
            .agg([
                pl.count("transaction_hash").alias("transfer_count"),
                pl.n_unique("from_address").alias("unique_senders"),
                pl.n_unique("to_address").alias("unique_receivers"),
                # Cast string amount to float for summing, handle any invalid values
                pl.col("amount").cast(pl.Float64, strict=False).sum().fill_null(0).alias("total_volume"),
                # Count MEV transfers
                pl.col("is_mev_transfer").sum().alias("mev_transfers")
            ])
        )

        # Convert to format expected by TimescaleDB
        raw_data = []
        for row in aggregated.rows(named=True):
            raw_data.append({
                "token_address": row["token_address"],
                "transfer_count": row["transfer_count"],
                "unique_senders": row["unique_senders"],
                "unique_receivers": row["unique_receivers"],
                "total_volume": row["total_volume"],
                "mev_transfers": row["mev_transfers"]
            })

        # Filter out blacklisted tokens before storing
        if self.enable_blacklist and self.blacklist_manager:
            before_count = len(raw_data)
            token_addresses = [row["token_address"] for row in raw_data]
            clean_addresses = set(self.blacklist_manager.filter_addresses(token_addresses))
            raw_data = [row for row in raw_data if row["token_address"] in clean_addresses]
            filtered_count = before_count - len(raw_data)
            if filtered_count > 0:
                self.logger.info(
                    f"Filtered {filtered_count} blacklisted tokens before storing"
                )
        
        # Store in TimescaleDB with chain_id
        store_raw_transfers(raw_data, interval_start, self.chain_id)

        self.logger.info(f"Stored {len(raw_data)} raw transfer records for chain {self.chain_id}")
        return len(raw_data)
    
    async def _aggregate_and_get_top_tokens(
        self,
        transfer_data: List[Dict[str, Any]],
        min_transfers: int
    ) -> List[Dict[str, Any]]:
        """Aggregate transfers and get top tokens."""
        # Trigger hourly aggregation if needed
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

        try:
            # Run aggregation from raw to hourly and store it
            hourly_data = aggregate_raw_to_hourly(current_hour, self.chain_id)
            if hourly_data:
                from src.core.storage.timescaledb import store_hourly_transfers
                store_hourly_transfers(hourly_data, current_hour, self.chain_id)
                self.logger.info(f"Stored {len(hourly_data)} hourly aggregations for chain {self.chain_id}")

                # Return the hourly data directly as top tokens
                filtered_tokens = [
                    token for token in hourly_data
                    if token.get('transfer_count', 0) >= min_transfers
                ]
                self.logger.info(f"Found {len(filtered_tokens)} tokens meeting criteria from hourly data")
                return filtered_tokens

            # If no hourly data, try getting from hourly table
            top_tokens = get_top_tokens_by_average(limit=1000, chain_id=self.chain_id)
            if top_tokens:
                # Filter by minimum transfers - use avg_transfers_24h field
                filtered_tokens = [
                    token for token in top_tokens
                    if token.get('avg_transfers_24h', 0) >= min_transfers
                ]

                self.logger.info(f"Found {len(filtered_tokens)} tokens meeting criteria from stored data")
                return filtered_tokens

            # No hourly data found, fall through to direct calculation
            self.logger.info("No hourly data found, using direct calculation from raw data")

        except Exception as e:
            self.logger.warning(f"Aggregation failed, using direct calculation: {e}")

        # Fallback: calculate directly from transfer_data with schema fix
        if not transfer_data:
            self.logger.warning("No transfer data available for aggregation")
            return []

        try:
            # Apply the same schema normalization as in _store_raw_data
            df = pl.DataFrame(transfer_data, infer_schema_length=len(transfer_data))
            self.logger.info("Created DataFrame successfully for aggregation")
        except Exception as e2:
            self.logger.error(f"DataFrame creation failed, trying manual normalization: {e2}")

            # Manual normalization approach
            critical_fields = ["token_address", "from_address", "to_address", "amount", "transaction_hash"]
            normalized_data = []
            for record in transfer_data:
                normalized_record = {}
                for key, value in record.items():
                    # Ensure all critical fields are strings
                    if key in critical_fields and value is not None:
                        normalized_record[key] = str(value)
                    else:
                        normalized_record[key] = value
                normalized_data.append(normalized_record)

            # Try again with normalized data
            df = pl.DataFrame(normalized_data, infer_schema_length=len(normalized_data))
            self.logger.info("Created DataFrame with manual normalization for aggregation")

        if df.height == 0:
            return []

        token_counts = (
            df.group_by("token_address")
            .agg([
                pl.count("transaction_hash").alias("transfer_count"),
                pl.n_unique("from_address").alias("unique_senders"),
                pl.n_unique("to_address").alias("unique_receivers")
            ])
            .filter(pl.col("transfer_count") >= min_transfers)
            .sort("transfer_count", descending=True)
        )

        return [
                {
                    "token_address": row["token_address"],
                    "transfer_count": row["transfer_count"],
                    "unique_senders": row["unique_senders"],
                    "unique_receivers": row["unique_receivers"],
                    "avg_transfers": float(row["transfer_count"]),  # No history
                    "chain": self.chain
                }
                for row in token_counts.rows(named=True)
            ]
    
    async def _update_redis_cache(self, tokens: List[Dict[str, Any]]) -> None:
        """Update Redis cache with token data."""
        try:
            redis_client = await self._get_redis_client()
            
            # Cache top tokens list
            top_tokens_key = f"top_tokens:{self.chain}:24h"
            await redis_client.set(
                top_tokens_key,
                pl.DataFrame(tokens).write_json(),
                ex=3600  # 1 hour expiry
            )
            
            # Cache individual token data
            for token in tokens:
                key = f"token_transfers:{self.chain}:{token['token_address']}"
                await redis_client.hset(key, mapping={
                    "transfer_count": str(token["transfer_count"]),
                    "unique_senders": str(token.get("unique_senders", 0)),
                    "unique_receivers": str(token.get("unique_receivers", 0)),
                    "avg_transfers_24h": str(token.get("avg_transfers", 0)),
                    "last_updated": str(datetime.now())
                })
                await redis_client.expire(key, 3600)
            
            await redis_client.close()
            self.logger.info(f"Updated Redis cache for {len(tokens)} tokens")
            
        except Exception as e:
            self.logger.exception(f"Failed to update Redis cache: {e}")
    
    async def _get_redis_client(self):
        """Get async Redis client."""
        return redis.Redis(
            host=self.config.database.REDIS_HOST,
            port=self.config.database.REDIS_PORT,
            db=self.config.database.REDIS_DB,
            password=getattr(self.config.database, 'REDIS_PASSWORD', None),
            decode_responses=True
        )
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process single transfer event (not used in batch processor)."""
        return None
    
    # MEV Analysis Methods (replacing separate MEVTransferProcessor)
    
    def _load_mev_addresses(self) -> set:
        """Load MEV addresses from libmev.com leaderboard."""
        # TODO: Replace with your original 4 MEV addresses in 32-byte form
        # Currently only have Jared's address - need the other 3 you had before
        return {
            "0x0000000000000000000000001f2f10d1c40777ae1da742455c65828ff36df387",
            "0x000000000000000000000000d4bc53434c5e12cb41381a556c3c47e1a86e80e3",
            "0x00000000000000000000000000000000009e50a7ddb7a7b0e2ee6604fd120e49",
            "0x000000000000000000000000e8c060f8052e07423f71d445277c61ac5138a2e5"  # jared
            
        }
    
    async def get_mev_active_tokens(
        self, 
        hours_back: int = 1,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get tokens with potentially high MEV activity by analyzing transfer patterns.
        
        Since we work with aggregated data (no individual from/to addresses), we identify 
        MEV-active tokens by:
        1. High transfer frequency (more than average)
        2. High unique sender/receiver ratio (indicates bot activity)
        3. Large total volume
        
        Note: This is an approximation since we don't have individual transfer data 
        with from/to addresses in the aggregated tables.
        """
        try:
            from src.core.storage.timescaledb import get_table_names
            tables = get_table_names(self.chain_id)
            engine = get_timescale_engine()
            with engine.connect() as conn:
                cutoff_time = datetime.now() - timedelta(hours=hours_back)

                result = conn.execute(text(f"""
                    SELECT
                        token_address,
                        SUM(transfer_count) as total_transfers,
                        SUM(unique_senders) as total_unique_senders,
                        SUM(unique_receivers) as total_unique_receivers,
                        SUM(total_volume) as cumulative_volume,
                        MIN(timestamp) as first_seen,
                        MAX(timestamp) as last_seen,
                        COUNT(*) as time_periods,
                        -- MEV activity indicators
                        AVG(transfer_count::float / GREATEST(unique_senders, 1)) as avg_transfers_per_sender,
                        AVG(total_volume::float / GREATEST(transfer_count, 1)) as avg_volume_per_transfer
                    FROM {tables['raw']}
                    WHERE timestamp >= :cutoff_time
                    GROUP BY token_address
                    HAVING SUM(transfer_count) >= 10  -- Minimum activity threshold
                    ORDER BY
                        -- Prioritize tokens with high activity and potential bot patterns
                        (SUM(transfer_count)::float / COUNT(*)) *
                        (AVG(transfer_count::float / GREATEST(unique_senders, 1))) DESC
                    LIMIT :limit
                """), {"cutoff_time": cutoff_time, "limit": limit})
                
                mev_tokens = []
                for row in result:
                    # Calculate MEV activity score based on patterns
                    mev_score = (row.avg_transfers_per_sender or 0) * (row.total_transfers / max(row.time_periods, 1))
                    
                    mev_tokens.append({
                        "token_address": row.token_address,
                        "total_transfers": row.total_transfers,
                        "total_unique_senders": row.total_unique_senders, 
                        "total_unique_receivers": row.total_unique_receivers,
                        "cumulative_volume": float(row.cumulative_volume) if row.cumulative_volume else 0,
                        "first_seen": row.first_seen,
                        "last_seen": row.last_seen,
                        "time_periods": row.time_periods,
                        "avg_transfers_per_sender": float(row.avg_transfers_per_sender) if row.avg_transfers_per_sender else 0,
                        "avg_volume_per_transfer": float(row.avg_volume_per_transfer) if row.avg_volume_per_transfer else 0,
                        "mev_activity_score": float(mev_score),
                        "chain": self.chain
                    })
                
                self.logger.info(f"Found {len(mev_tokens)} tokens with potential MEV activity patterns")
                return mev_tokens

        except Exception as e:
            self.logger.exception(f"Failed to get MEV active tokens: {e}")
            return []

    # Blacklist Management Methods

    async def scan_and_update_blacklist(
        self,
        limit: int = 100,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """
        Scan recently seen tokens and update blacklist from Etherscan.

        This should be run periodically (e.g., daily) to check new tokens
        for phishing/scam labels on Etherscan.

        Args:
            limit: Maximum number of tokens to check
            hours_back: Hours to look back for recent tokens

        Returns:
            Dict with scanning results
        """
        if not self.enable_blacklist or not self.blacklist_manager:
            return {
                "success": False,
                "error": "Blacklist manager not enabled"
            }

        try:
            # Get recent tokens from database
            from src.core.storage.timescaledb import get_table_names
            tables = get_table_names(self.chain_id)
            engine = get_timescale_engine()

            with engine.connect() as conn:
                cutoff_time = datetime.now() - timedelta(hours=hours_back)

                result = conn.execute(text(f"""
                    SELECT DISTINCT token_address
                    FROM {tables['raw']}
                    WHERE timestamp >= :cutoff_time
                    ORDER BY timestamp DESC
                    LIMIT :limit
                """), {
                    "cutoff_time": cutoff_time,
                    "limit": limit
                })

                token_addresses = [row.token_address for row in result]

            self.logger.info(
                f"Scanning {len(token_addresses)} recent tokens for blacklist updates"
            )

            # Check each token on Etherscan
            added_count = self.blacklist_manager.check_and_add_from_etherscan(
                token_addresses
            )

            return {
                "success": True,
                "tokens_scanned": len(token_addresses),
                "new_blacklisted": added_count,
                "total_blacklisted": len(self.blacklist_manager.blacklist)
            }

        except Exception as e:
            self.logger.error(f"Failed to scan and update blacklist: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def get_blacklist_stats(self) -> Dict[str, Any]:
        """Get blacklist statistics."""
        if not self.enable_blacklist or not self.blacklist_manager:
            return {
                "enabled": False
            }

        stats = self.blacklist_manager.get_stats()
        stats["enabled"] = True
        stats["chain"] = self.chain
        return stats
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database size and retention statistics using centralized TimescaleDB storage."""
        try:
            return get_timescale_stats(self.chain_id)
        except Exception as e:
            self.logger.exception(f"Failed to get database stats: {e}")
            return {"error": str(e)}
    
    async def get_cached_top_tokens(self, period: str = '24h') -> List[Dict[str, Any]]:
        """Get cached top tokens from Redis."""
        try:
            redis_client = await self._get_redis_client()
            key = f"top_tokens:{self.chain}:{period}"
            
            cached_data = await redis_client.get(key)
            await redis_client.close()
            
            if cached_data:
                return pl.read_json(cached_data).to_dicts()
            
            return []
            
        except Exception as e:
            self.logger.exception(f"Failed to get cached tokens: {e}")
            return []
    
    async def get_token_stats(self, token_address: str) -> Optional[Dict[str, Any]]:
        """Get cached statistics for a specific token."""
        try:
            redis_client = await self._get_redis_client()
            key = f"token_transfers:{self.chain}:{token_address}"
            
            stats = await redis_client.hgetall(key)
            await redis_client.close()
            
            if stats:
                return {
                    "token_address": token_address,
                    "transfer_count": int(stats.get("transfer_count", 0)),
                    "unique_senders": int(stats.get("unique_senders", 0)),
                    "unique_receivers": int(stats.get("unique_receivers", 0)),
                    "avg_transfers_24h": float(stats.get("avg_transfers_24h", 0)),
                    "last_updated": stats.get("last_updated")
                }
            
            return None
            
        except Exception as e:
            self.logger.exception(f"Failed to get token stats: {e}")
            return None