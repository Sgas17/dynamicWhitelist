import os
import glob
import polars as pl
from eth_utils.address import to_checksum_address
from hexbytes import HexBytes
from pathlib import Path
import redis
import json
from datetime import datetime, timezone
import logging
from token_transfer_timeseries import (
    setup_timescale_tables, 
    store_hourly_transfers, 
    update_token_averages,
    get_top_tokens_by_average,
    get_timescale_engine
)
from hexbytes import HexBytes
from eth_abi.abi import decode 

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use the same data directory as the fetch script
DATA_DIR = "/home/sam-sullivan/dynamic_whitelist/data/ethereum/latest_transfers"

# Redis connection for storing top tokens
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
platform = os.getenv('PLATFORM', 'ethereum')

def decode_amount_with_logging(data_bytes):
    """Decode transfer amount with error logging for debugging"""
    try:
        if len(data_bytes) != 32:
            raise ValueError(f"Expected 32 bytes for uint256, got {len(data_bytes)}")
        return int.from_bytes(data_bytes, byteorder='big')
    except Exception as e:
        logger.error(f"Failed to decode amount from data: {data_bytes.hex() if data_bytes else 'None'}, length: {len(data_bytes) if data_bytes else 0}, error: {e}")
        raise

def get_redis_client():
    """Get Redis client connection"""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_connect_timeout=5)

def cleanup_processed_files(transfers_dir):
    """Clean up processed parquet files"""
    transfers_path = Path(transfers_dir)
    if not transfers_path.exists():
        logger.warning(f"Directory {transfers_path} does not exist")
        return
    
    parquet_files = list(transfers_path.glob("*.parquet"))
    for file_path in parquet_files:
        try:
            file_path.unlink()
            logger.info(f"Deleted processed file: {file_path}")
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")

def get_top_tokens_by_transfers(limit=100):
    """Get top tokens by transfer count from Redis"""
    r = get_redis_client()
    try:
        # Get top tokens from Redis sorted set
        top_tokens = r.zrevrange('top_tokens_by_transfers', 0, limit-1, withscores=True)  # type: ignore
        
        result = []
        for token_address, score in top_tokens:  # type: ignore
            # Get token data from Redis hash
            token_data = r.hgetall(f"token:{token_address}")  # type: ignore
            if token_data:
                result.append({
                    "token_address": token_address,
                    "transfer_count": int(score),
                    "avg_transfers_per_hour": float(token_data.get('avg_transfers_per_hour', 0)),  # type: ignore
                    "last_updated": token_data.get('last_updated', '')  # type: ignore
                })
        
        return result
    except Exception as e:
        logger.error(f"Error getting top tokens: {e}")
        return []

def update_redis_cache(top_tokens_data):
    """Update Redis cache with top tokens from TimescaleDB"""
    r = get_redis_client()
    try:
        # Clear existing datab b
        r.delete('top_tokens_by_transfers')
        
        for token_data in top_tokens_data:
            token_address = token_data['token_address']
            avg_transfers = token_data['avg_transfers']
            
            # Store in sorted set for ranking
            r.zadd(f'{platform}:top_tokens_by_transfers', {token_address: avg_transfers})
            
            # Store detailed data in hash
            token_details = {
                'avg_transfers_per_hour': avg_transfers,
                'transfer_count': token_data['transfer_count'],
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            r.hset(f"{platform}:{token_address}:transfers", mapping=token_details)
        
        logger.info(f"Updated Redis cache with {len(top_tokens_data)} top tokens")
        
    except Exception as e:
        logger.error(f"Error updating Redis cache: {e}")

def process_transfer_events():
    """Process transfer events and store in TimescaleDB + Redis cache."""
    # Use the data directory where fetch_transfers.sh saves the parquet files
    transfers_dir = Path(DATA_DIR)

    # Check if the directory exists
    if not transfers_dir.exists():
        print(f"Directory {transfers_dir} does not exist. Creating it...")
        transfers_dir.mkdir(parents=True, exist_ok=True)
        print("No parquet files found. Please add transfer event parquet files to the latest_transfers directory.")
        return None

    # Check if there are any parquet files
    parquet_files = list(transfers_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {transfers_dir}")
        print("Please add transfer event parquet files to the latest_transfers directory.")
        return None

    print(f"Reading {len(parquet_files)} parquet files...")
    
    try:
        # Setup TimescaleDB tables
        engine = get_timescale_engine()
        setup_timescale_tables(engine)
        
        # Read all parquet files in the directory
        df = pl.scan_parquet(f"{transfers_dir}/*.parquet")

        print("Processing transfer events...")
        
        # First, collect the data to work with it in Python
        df_collected = df.collect()
        
        # Filter out empty data fields
        df_filtered = df_collected.filter(pl.col("data").map_elements(lambda x: len(x) if x is not None else 0) > 0)
        
        # Decode amounts in Python to avoid Polars type inference issues
        amounts = []
        for data_bytes in df_filtered["data"]:
            try:
                if len(data_bytes) != 32:
                    amounts.append(0)  # Skip invalid data
                else:
                    amounts.append(int.from_bytes(data_bytes, byteorder='big'))
            except Exception as e:
                logger.error(f"Failed to decode amount from data: {data_bytes.hex() if data_bytes else 'None'}, error: {e}")
                amounts.append(0)
        
        # Create a new DataFrame with decoded amounts
        result = (
            df_filtered
            .with_columns([
                pl.col("address").alias("token_address"),
                pl.col("topic1").alias("from_address"),
                pl.col("topic2").alias("to_address"),
            ])
            .with_columns([
                pl.Series("amount", amounts, dtype=pl.Int128)
            ])
            .filter(pl.col("amount") > 0)
            .group_by("token_address")
            .agg([
                pl.len().alias("transfer_count"),
                pl.col("from_address").n_unique().alias("unique_senders"),
                pl.col("to_address").n_unique().alias("unique_receivers"),
                pl.col("amount").sum().alias("total_volume"),
            ])
            .with_columns([
                pl.col("transfer_count").cast(pl.Int128),
                pl.col("unique_senders").cast(pl.Int128),
                pl.col("unique_receivers").cast(pl.Int128),
                pl.col("total_volume").cast(pl.Int128),
            ])
            .sort("transfer_count", descending=True)
        )

        # Convert binary addresses to hex strings
        result = result.with_columns(
            [
                pl.col("token_address")
                .map_elements(lambda x: "0x" + x.hex(), return_dtype=pl.Utf8)
                .alias("token_address"),
            ]
        )

        # Get current hour timestamp (rounded to hour)
        current_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        
        # Convert to list of dictionaries for TimescaleDB
        transfers_data = []
        for row in result.iter_rows(named=True):
            transfers_data.append({
                'token_address': row['token_address'],
                'transfer_count': row['transfer_count'],
                'unique_senders': row['unique_senders'],
                'unique_receivers': row['unique_receivers'],
                'total_volume': row['total_volume']
            })

        # Store in TimescaleDB
        print(f"Storing {len(transfers_data)} token records in TimescaleDB...")
        store_hourly_transfers(transfers_data, current_hour)
        
        # Update averages in TimescaleDB
        print("Calculating and updating token averages...")
        update_token_averages(current_hour)
        
        # Get top 100 tokens by 24h average
        print("Getting top 100 tokens by 24h average...")
        top_tokens = get_top_tokens_by_average(limit=100, period='24h')
        
        # Update Redis cache
        print("Updating Redis cache...")
        update_redis_cache(top_tokens)

        # Clean up processed files
        print("Cleaning up processed files...")
        cleanup_processed_files(transfers_dir)

        # Save CSV backup - ensure all data is string-based for CSV export
        script_dir = Path(__file__).parent
        output_file = script_dir / "top_tokens_by_transfers.csv"
        
        # Convert result to CSV-safe format
        csv_data = result.head(100).select([
            pl.col("token_address").cast(pl.Utf8),
            pl.col("transfer_count").cast(pl.Int128),
            pl.col("unique_senders").cast(pl.Int128),
            pl.col("unique_receivers").cast(pl.Int128),
            pl.col("total_volume").cast(pl.Int128)
        ])
        csv_data.write_csv(output_file)

        print(f"\nProcessed {len(transfers_data)} tokens")
        print(f"Top 100 tokens stored in Redis cache")
        print(f"Results also saved to: {output_file}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing transfer events: {e}")
        raise


if __name__ == "__main__":
    process_transfer_events()
