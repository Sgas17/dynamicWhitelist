"""
Modular transfer event processors.

KISS: Extract transfer processing logic into reusable components.
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import asyncio
from datetime import datetime
import redis.asyncio as redis

from .base import BaseProcessor, ProcessorResult, ProcessorError
from ..config.manager import ConfigManager


class LatestTransfersProcessor(BaseProcessor):
    """
    Process latest token transfer events and cache results.
    
    Refactored from latest_transfers_processor.py
    """
    
    def __init__(self, chain: str = "ethereum"):
        """Initialize transfers processor."""
        super().__init__(chain, "transfers")
        self.config = ConfigManager()
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        return True
    
    async def process(
        self,
        data_dir: Optional[str] = None,
        hours_back: int = 24,
        min_transfers: int = 100
    ) -> ProcessorResult:
        """
        Process latest transfer events.
        
        Args:
            data_dir: Directory containing transfer parquet files
            hours_back: Hours to look back for transfers
            min_transfers: Minimum transfers to include token
            
        Returns:
            ProcessorResult: Processing results with top tokens
        """
        try:
            # Setup data directory
            if not data_dir:
                data_dir = Path(self.config.base.DATA_DIR)
            else:
                data_dir = Path(data_dir)
            
            # Process transfer events
            top_tokens = await self._get_top_tokens_by_transfers(
                data_dir, hours_back, min_transfers
            )
            
            if not top_tokens:
                return ProcessorResult(
                    success=True,
                    data=[],
                    processed_count=0,
                    metadata={"message": "No qualifying tokens found"}
                )
            
            # Update Redis cache
            await self._update_redis_cache(top_tokens)
            
            return ProcessorResult(
                success=True,
                data=top_tokens,
                processed_count=len(top_tokens),
                metadata={
                    "hours_back": hours_back,
                    "min_transfers": min_transfers,
                    "total_tokens": len(top_tokens)
                }
            )
            
        except Exception as e:
            error_msg = f"Transfer processing failed: {str(e)}"
            self.logger.exception(error_msg)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _get_top_tokens_by_transfers(
        self, 
        data_dir: Path, 
        hours_back: int,
        min_transfers: int
    ) -> List[Dict[str, Any]]:
        """Get top tokens by transfer count in last N hours."""
        try:
            # Find transfer parquet files
            transfer_files = list(data_dir.glob("**/transfers_*.parquet"))
            
            if not transfer_files:
                self.logger.warning("No transfer parquet files found")
                return []
            
            # Read and process transfer data
            dfs = []
            for file in transfer_files:
                try:
                    df = pl.read_parquet(file)
                    dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to read {file}: {e}")
            
            if not dfs:
                return []
            
            # Combine all transfer data
            all_transfers = pl.concat(dfs)
            
            # Filter by time (last N hours)
            if "timestamp" in all_transfers.columns:
                cutoff_time = pl.datetime.now() - pl.duration(hours=hours_back)
                all_transfers = all_transfers.filter(
                    pl.col("timestamp") >= cutoff_time
                )
            
            # Group by token and count transfers
            token_counts = (
                all_transfers
                .group_by("token_address")
                .agg([
                    pl.count("transaction_hash").alias("transfer_count"),
                    pl.n_unique("from_address").alias("unique_senders"),
                    pl.n_unique("to_address").alias("unique_receivers")
                ])
                .filter(pl.col("transfer_count") >= min_transfers)
                .sort("transfer_count", descending=True)
            )
            
            # Convert to list of dictionaries
            top_tokens = []
            for row in token_counts.rows(named=True):
                top_tokens.append({
                    "token_address": row["token_address"],
                    "transfer_count": row["transfer_count"],
                    "unique_senders": row["unique_senders"],
                    "unique_receivers": row["unique_receivers"],
                    "chain": self.chain
                })
            
            self.logger.info(f"Found {len(top_tokens)} tokens with {min_transfers}+ transfers")
            return top_tokens
            
        except Exception as e:
            self.logger.exception(f"Failed to process transfer data: {e}")
            return []
    
    async def _update_redis_cache(self, tokens: List[Dict[str, Any]]) -> None:
        """Update Redis cache with token data."""
        try:
            redis_client = await self._get_redis_client()
            
            # Cache each token's data
            for token in tokens:
                key = f"token_transfers:{self.chain}:{token['token_address']}"
                await redis_client.hset(key, mapping={
                    "transfer_count": str(token["transfer_count"]),
                    "unique_senders": str(token["unique_senders"]),
                    "unique_receivers": str(token["unique_receivers"]),
                    "last_updated": str(datetime.now())
                })
            
            # Set expiry for each token
            for token in tokens:
                key = f"token_transfers:{self.chain}:{token['token_address']}"
                await redis_client.expire(key, 3600)  # 1 hour expiry
            
            await redis_client.close()
            self.logger.info(f"Updated Redis cache for {len(tokens)} tokens")
            
        except Exception as e:
            self.logger.exception(f"Failed to update Redis cache: {e}")
    
    async def _get_redis_client(self):
        """Get Redis client."""
        return redis.Redis(
            host=self.config.database.REDIS_HOST,
            port=self.config.database.REDIS_PORT,
            db=self.config.database.REDIS_DB,
            password=self.config.database.REDIS_PASSWORD,
            decode_responses=True
        )