"""
Ethereum-specific data fetcher with cryo wrapper.

KISS: Ethereum chain configuration and optimizations.
"""

from typing import Dict, List, Any, Optional
from pathlib import Path

from .cryo_fetcher import CryoFetcher
from .base import FetchResult
from ..config.manager import ConfigManager


class EthereumFetcher(CryoFetcher):
    """
    Ethereum-specific data fetcher.
    
    Configured with Ethereum-specific parameters and optimizations.
    """
    
    def __init__(self, rpc_url: Optional[str] = None):
        """Initialize Ethereum fetcher."""
        # Get RPC URL from config if not provided
        if not rpc_url:
            config = ConfigManager()
            chain_config = config.chains.get_chain_config("ethereum")
            rpc_url = chain_config["rpc_url"]
        
        super().__init__("ethereum", rpc_url)
        
        # Ethereum-specific configurations
        self.blocks_per_minute = 5  # ~12 second block time
        self.blocks_per_request = 10000  # Larger chunks for Ethereum
        
        # Update cryo options for Ethereum
        self.cryo_options = [
            "--rpc", rpc_url,
            "--inner-request-size", str(self.blocks_per_request),
            "--u256-types", "binary"
        ]
    
    async def calculate_block_range(self, hours_back: int) -> tuple[int, int]:
        """Calculate block range for Ethereum."""
        return await super().calculate_block_range(hours_back, self.blocks_per_minute)
    
    async def fetch_uniswap_v2_pools(
        self, 
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V2 pair creation events on Ethereum.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V2 deployment block and factory contracts
            deployment_block = 10000835  # Uniswap V2 factory deployment
            factory_contracts = [
                "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",  # Uniswap V2
                "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",  # Sushiswap V2
            ]
            
            # PairCreated event signature (V2)
            pair_created_event = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
            
            # Determine start block
            start_block = deployment_block
            if from_checkpoint:
                checkpoint_block = await self._get_last_processed_block("uniswap_v2_pools", output_dir)
                if checkpoint_block:
                    start_block = checkpoint_block
                    
            # Get latest block
            latest_block = await self.get_latest_block()
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "uniswap_v2_paircreated_events"
            
            self.logger.info(f"Fetching Uniswap V2 pools from block {start_block} to {latest_block}")
            
            # Clean last file if resuming (handles partial data)
            if from_checkpoint:
                await self._cleanup_last_file(output_dir)
                
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=latest_block,
                contracts=factory_contracts,
                events=[pair_created_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": "uniswap_v2",
                    "event_type": "pair_created",
                    "checkpoint_used": from_checkpoint and checkpoint_block is not None
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Uniswap V2 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_uniswap_v3_pools(
        self, 
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V3 pool creation events on Ethereum.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V3 deployment block and contracts
            deployment_block = 12369621
            factory_contracts = [
                "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # Uniswap V3
                "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",  # Sushiswap V3  
                "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"   # PancakeSwap V3
            ]
            
            # PoolCreated event signature
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
            
            self.logger.info(f"Fetching Uniswap V3 pools from block {start_block} to {latest_block}")
            
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
                    "factory_addresses": factory_contracts,
                    "deployment_block": deployment_block
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Uniswap V3 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_uniswap_v4_pools(
        self,
        output_dir: Optional[str] = None,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V4 pool initialization events on Ethereum.
        
        Args:
            output_dir: Output directory for data
            from_checkpoint: If True, resume from last checkpoint, else from deployment
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V4 deployment block and pool manager
            deployment_block = 21688329
            pool_manager_contract = "0x000000000004444c5dc75cB358380D2e3dE08A90"
            
            # Initialize event signature (V4 uses Initialize instead of PoolCreated)
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
            
            self.logger.info(f"Fetching Uniswap V4 pools from block {start_block} to {latest_block}")
            
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
                    "pool_manager_address": pool_manager_contract,
                    "deployment_block": deployment_block
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Uniswap V4 pool fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def fetch_recent_transfers(
        self, 
        hours_back: int = 24,
        output_dir: Optional[str] = None
    ) -> FetchResult:
        """
        Fetch recent ERC20 transfers on Ethereum.
        
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
    
    async def fetch_liquidity_events(
        self,
        protocol: str = "uniswap_v3", 
        hours_back: int = 24,
        output_dir: Optional[str] = None
    ) -> FetchResult:
        """
        Fetch liquidity modification events (mint/burn).
        
        Args:
            protocol: Protocol to fetch events for
            hours_back: Hours to look back
            output_dir: Output directory for data
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Calculate block range
            start_block, end_block = await self.calculate_block_range(hours_back)
            
            # Liquidity event signatures
            mint_event = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
            burn_event = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
            
            # Setup output directory
            if not output_dir:
                from pathlib import Path
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / f"{protocol}_liquidity_events"
            
            self.logger.info(f"Fetching {protocol} liquidity events from block {start_block} to {end_block}")
            
            result = await self.fetch_logs(
                start_block=start_block,
                end_block=end_block,
                events=[mint_event, burn_event],
                output_dir=str(output_dir)
            )
            
            if result.success:
                result.metadata.update({
                    "protocol": protocol,
                    "event_types": ["mint", "burn"],
                    "hours_back": hours_back
                })
            
            return result
            
        except Exception as e:
            error_msg = f"Liquidity events fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
    
    async def _get_last_processed_block(
        self, 
        event_type: str, 
        output_dir: Optional[str] = None
    ) -> Optional[int]:
        """
        Get the last processed block from existing parquet files.
        
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
            # Expected format: ethereum__logs__START_to_END.parquet
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
        
        This matches the behavior in the shell scripts:
        `if [ -d $DIR ]; then rm "${DIR}/$(ls ${DIR} | tail -1)"; fi`
        
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
    
    async def fetch_uniswap_v4_liquidity_events(
        self, 
        output_dir: Optional[str] = None,
        hours_back: int = 24,
        from_checkpoint: bool = True
    ) -> FetchResult:
        """
        Fetch Uniswap V4 liquidity events (ModifyLiquidity).
        
        V4 uses a single ModifyLiquidity event instead of separate mint/burn events.
        
        Args:
            output_dir: Output directory for data
            hours_back: Hours to look back from latest block
            from_checkpoint: If True, resume from last checkpoint, else use hours_back
            
        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Uniswap V4 pool manager contract
            pool_manager_contract = "0x000000000004444c5dc75cB358380D2e3dE08A90"
            
            # V4 ModifyLiquidity event signature from shell script
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
                    # Ensure we don't go before V4 deployment
                    v4_deployment = 21688329
                    start_block = max(start_block, v4_deployment)
            else:
                # Use hours_back from latest block
                start_block, end_block = await self.calculate_block_range(hours_back)
                # Ensure we don't go before V4 deployment
                v4_deployment = 21688329
                start_block = max(start_block, v4_deployment)
            
            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "uniswap_v4_liquidity_events"
            
            self.logger.info(f"Fetching Uniswap V4 liquidity events from block {start_block} to {end_block}")
            
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
            error_msg = f"Uniswap V4 liquidity events fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)