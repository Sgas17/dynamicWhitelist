"""
Base chain-specific data fetcher with cryo wrapper.

KISS: Base L2 chain configuration and optimizations.
"""

from typing import Optional
from pathlib import Path

from .cryo_fetcher import CryoFetcher
from .base import FetchResult
from ..config.manager import ConfigManager


class BaseFetcher(CryoFetcher):
    """
    Base chain-specific data fetcher.
    
    Configured with Base L2-specific parameters and optimizations.
    """
    
    def __init__(self, rpc_url: Optional[str] = None, protocol_config: Optional['ProtocolConfig'] = None):
    """Initialize Base fetcher."""
    # Get RPC URL from config if not provided
    if not rpc_url:
        config = ConfigManager()
        chain_config = config.chains.get_chain_config("base")
        rpc_url = chain_config["rpc_url"]
    
    super().__init__("base", rpc_url)
    
    # Initialize protocol config
    if protocol_config:
        self.protocol_config = protocol_config
    else:
        from ..config.protocols import ProtocolConfig
        self.protocol_config = ProtocolConfig()
    
    # Base L2-specific configurations
    self.blocks_per_minute = 30  # ~2 second block time on Base
    self.blocks_per_request = 10000  # Same as Ethereum for compatibility
    
    # Update cryo options for Base
    self.cryo_options = [
        "--rpc", rpc_url,
        "--inner-request-size", str(self.blocks_per_request),
        "--u256-types", "binary"
    ]
    
    async def calculate_block_range(self, hours_back: int) -> tuple[int, int]:
        """Calculate block range for Base L2."""
        return await super().calculate_block_range(hours_back, self.blocks_per_minute)
    
    async def fetch_uniswap_v3_pools(
        self, 
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V3 pool creation events on Base.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V3 deployment on Base - different from Ethereum
            deployment_block = 1371680  # Base mainnet V3 deployment
            factory_contracts = [
                "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",  # Uniswap V3 on Base
                "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",  # Panacake V3 on Base
            ]
            
            # PoolCreated event signature (same across chains)
            pool_created_event = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
            
            # Determine start block
            start_block = deployment_block
            if from_checkpoint:
                checkpoint_block = await self._get_last_processed_block("uniswap_v3_pools", output_dir)
                if checkpoint_block:
                    start_block = checkpoint_block
                    
            # Get latest block
            latest_block = await self.get_latest_block()
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "uniswap_v3_poolcreated_events"
            
            self.logger.info(f"Fetching Base Uniswap V3 pools from block {start_block} to {latest_block}")
            
            # Clean last file if resuming (handles partial data)
            if from_checkpoint:
                await self._cleanup_last_file(output_dir)
                
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=latest_block,
                contracts=factory_contracts,
                events=[pool_created_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": "uniswap_v3",
                    "event_type": "pool_created",
                    "checkpoint_used": from_checkpoint and checkpoint_block is not None
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Base Uniswap V3 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_aerodrome_v3_pools(
        self, 
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Aerodrome V3 pool creation events on Base.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Aerodrome V3 deployment on Base
            deployment_block = 13843704  # Approximate deployment block (to be updated with actual)
            factory_contract = "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"
            
            # Aerodrome V3 pool creation event signature
            pool_created_event = "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2"
            
            # Determine start block
            start_block = deployment_block
            if from_checkpoint:
                checkpoint_block = await self._get_last_processed_block("aerodrome_v3_pools", output_dir)
                if checkpoint_block:
                    start_block = checkpoint_block
                    
            # Get latest block
            latest_block = await self.get_latest_block()
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "aerodrome_v3_poolcreated_events"
            
            self.logger.info(f"Fetching Aerodrome V3 pools from block {start_block} to {latest_block}")
            
            # Clean last file if resuming (handles partial data)
            if from_checkpoint:
                await self._cleanup_last_file(output_dir)
                
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=latest_block,
                contracts=[factory_contract],
                events=[pool_created_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": "aerodrome_v3",
                    "event_type": "pool_created",
                    "checkpoint_used": from_checkpoint and checkpoint_block is not None
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Aerodrome V3 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_uniswap_v4_pools(
        self,
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V4 pool initialization events on Base.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V4 deployment on Base - same pool manager as Ethereum
            deployment_block = 18000000  # Approximate V4 deployment on Base (to be updated)
            pool_manager_contract = "0x000000000004444c5dc75cB358380D2e3dE08A90"
            
            # Initialize event signature (same across chains)
            initialized_event = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
            
            # Determine start block
            start_block = deployment_block
            if from_checkpoint:
                checkpoint_block = await self._get_last_processed_block("uniswap_v4_pools", output_dir)
                if checkpoint_block:
                    start_block = checkpoint_block
                    
            # Get latest block
            latest_block = await self.get_latest_block()
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "uniswap_v4_initialized_events"
            
            self.logger.info(f"Fetching Base Uniswap V4 pools from block {start_block} to {latest_block}")
            
            # Clean last file if resuming (handles partial data)
            if from_checkpoint:
                await self._cleanup_last_file(output_dir)
                
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=latest_block,
                contracts=[pool_manager_contract],
                events=[initialized_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": "uniswap_v4",
                    "event_type": "initialized",
                    "checkpoint_used": from_checkpoint and checkpoint_block is not None
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Base Uniswap V4 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_uniswap_v4_liquidity_events(
        self, 
        output_dir: Optional[str] = None,
        hours_back: int = 24,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V4 liquidity events (ModifyLiquidity) on Base.
        
        Args:
            output_dir: Output directory for data
            hours_back: Hours to look back from latest block
            from_checkpoint: If True, resume from last checkpoint, else use hours_back
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V4 pool manager contract (same as Ethereum)
            pool_manager_contract = "0x000000000004444c5dc75cB358380D2e3dE08A90"
            
            # V4 ModifyLiquidity event signature (same across chains)
            modify_liquidity_event = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
            
            # Determine start and end blocks
            if from_checkpoint:
                # Get last processed block from existing files
                checkpoint_block = await self._get_last_processed_block("uniswap_v4_liquidity", output_dir)
                if checkpoint_block:
                    start_block = checkpoint_block
                    end_block = await self.get_latest_block()
                else:
                    # No checkpoint, use V4 deployment block + hours_back
                    start_block, end_block = await self.calculate_block_range(hours_back)
                    # Ensure we don't go before V4 deployment on Base
                    v4_deployment = 18000000  # Approximate V4 deployment on Base
                    start_block = max(start_block, v4_deployment)
            else:
                # Use hours_back from latest block
                start_block, end_block = await self.calculate_block_range(hours_back)
                # Ensure we don't go before V4 deployment on Base
                v4_deployment = 18000000
                start_block = max(start_block, v4_deployment)
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "uniswap_v4_liquidity_events"
            
            self.logger.info(f"Fetching Base Uniswap V4 liquidity events from block {start_block} to {end_block}")
            
            # Clean last file if resuming from checkpoint
            if from_checkpoint:
                await self._cleanup_last_file(output_dir)
            
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=end_block,
                contracts=[pool_manager_contract],
                events=[modify_liquidity_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": "uniswap_v4",
                    "event_type": "modify_liquidity",
                    "hours_back": hours_back,
                    "checkpoint_used": from_checkpoint and checkpoint_block is not None
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Base Uniswap V4 liquidity events fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_recent_transfers(
        self, 
        hours_back: int = 24,
        output_dir: Optional[str] = None
    ) -> FetchResult:
        """
        Fetch recent ERC20 transfers on Base.
        
        Args:
            hours_back: Hours to look back
            output_dir: Output directory for data
            
        Returns:
            FetchResult: Fetch operation result
        """
        # Use smaller chunk size for transfers to avoid hitting limits
        return await self.fetch_transfers(
            hours_back=hours_back,
            output_dir=output_dir,
            chunk_size=500  # Smaller chunks for transfer events
        )
    
    async def _get_last_processed_block(
        self, 
        event_type: str, 
        output_dir: Optional[str] = None
    ) -> Optional[int]:
        """
        Get the last processed block from existing parquet files.
        
        Same logic as Ethereum fetcher - reusable across chains.
        
        Args:
            event_type: Type of event (e.g., 'uniswap_v3_pools')
            output_dir: Output directory to check
            
        Returns:
            Last processed block number or None if no files found
        """
        try:
            if not output_dir:
                return None
                
            output_path = Path(output_dir)
            if not output_path.exists():
                return None
                
            # Find parquet files and extract block ranges
            parquet_files = list(output_path.glob("*.parquet"))
            if not parquet_files:
                return None
                
            # Extract end block from latest file name
            # Expected format: base__logs__START_to_END.parquet
            latest_end_block = 0
            for file_path in parquet_files:
                filename = file_path.stem
                if "_to_" in filename:
                    try:
                        end_block_str = filename.split("_to_")[-1]
                        end_block = int(end_block_str)
                        latest_end_block = max(latest_end_block, end_block)
                    except (ValueError, IndexError):
                        continue
                        
            return latest_end_block if latest_end_block > 0 else None
            
        except Exception as e:
            self.logger.warning(f"Could not determine last processed block: {e}")
            return None
    
    async def _cleanup_last_file(self, output_dir: Optional[str]) -> bool:
        """
        Remove the last (most recent) parquet file to handle partial/corrupted data.
        
        Same logic as Ethereum fetcher - reusable across chains.
        
        Args:
            output_dir: Output directory to clean
            
        Returns:
            True if file was removed, False otherwise
        """
        try:
            if not output_dir:
                return False
                
            output_path = Path(output_dir)
            if not output_path.exists():
                return False
                
            # Find all parquet files
            parquet_files = list(output_path.glob("*.parquet"))
            if not parquet_files:
                return False
                
            # Sort by modification time and get the latest
            parquet_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            latest_file = parquet_files[0]
            
            # Remove the latest file
            latest_file.unlink()
            self.logger.info(f"Cleaned up last file: {latest_file}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Could not cleanup last file: {e}")
            return False