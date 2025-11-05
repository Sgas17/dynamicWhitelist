"""
Cryo wrapper for blockchain data fetching.

KISS: Simple wrapper around cryo command-line tool.
"""

import asyncio
import subprocess
from pathlib import Path
from typing import List, Optional

from ..config.manager import ConfigManager
from .base import BaseFetcher, FetchError, FetchResult


class CryoFetcher(BaseFetcher):
    """
    Cryo command-line tool wrapper for blockchain data fetching.

    Provides async interface to cryo with error handling and retry logic.
    """

    def __init__(self, chain: str, rpc_url: str):
        """Initialize cryo fetcher."""
        super().__init__(chain, rpc_url)
        self.config = ConfigManager()

        # Cryo common options based on existing scripts
        self.blocks_per_request = 10000
        self.cryo_options = [
            "--rpc",
            rpc_url,
            "--inner-request-size",
            str(self.blocks_per_request),
            "--u256-types",
            "binary",
        ]

    def validate_config(self) -> bool:
        """Validate cryo is available and RPC is accessible."""
        try:
            # Check if cryo command is available
            result = subprocess.run(
                ["cryo", "--version"], capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                self.logger.error("Cryo command not found")
                return False

            self.logger.info(f"Cryo version: {result.stdout.strip()}")
            return True

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            self.logger.error(f"Cryo validation failed: {e}")
            return False

    async def get_latest_block(self) -> int:
        """Get latest finalized block using cast."""
        try:
            cmd = ["cast", "block", "-f", "number", "-r", self.rpc_url, "finalized"]

            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                block_number = int(stdout.decode().strip())
                self.logger.debug(f"Latest block: {block_number}")
                return block_number
            else:
                error = stderr.decode().strip()
                raise FetchError(f"Failed to get latest block: {error}")

        except Exception as e:
            raise FetchError(f"Error getting latest block: {str(e)}")

    async def fetch_logs(
        self,
        start_block: int,
        end_block: int,
        contracts: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
    ) -> FetchResult:
        """
        Fetch logs using cryo command.

        Args:
            start_block: Starting block number
            end_block: Ending block number
            contracts: Contract addresses to filter
            events: Event signatures to filter
            output_dir: Output directory for parquet files

        Returns:
            FetchResult: Fetch operation result
        """
        try:
            if not self.validate_config():
                return FetchResult(
                    success=False, error="Cryo configuration validation failed"
                )

            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "logs"

            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Build cryo command
            cmd = ["cryo", "logs"] + self.cryo_options
            cmd.extend(["--blocks", f"{start_block}:{end_block}"])
            cmd.extend(["--output-dir", str(output_path)])

            # Add contract filters
            if contracts:
                cmd.extend(["--contract"] + contracts)

            # Add event filters
            if events:
                cmd.extend(["--event"] + events)

            self.logger.info(f"Executing cryo command: {' '.join(cmd)}")

            # Execute cryo command
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                # Check for output files
                parquet_files = list(output_path.glob("*.parquet"))

                return FetchResult(
                    success=True,
                    data_path=str(output_path),
                    fetched_blocks=end_block - start_block + 1,
                    start_block=start_block,
                    end_block=end_block,
                    metadata={
                        "output_files": [str(f) for f in parquet_files],
                        "contracts": contracts or [],
                        "events": events or [],
                    },
                )
            else:
                error_msg = stderr.decode().strip()
                return FetchResult(
                    success=False,
                    start_block=start_block,
                    end_block=end_block,
                    error=f"Cryo command failed: {error_msg}",
                )

        except Exception as e:
            error_msg = f"Cryo fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(
                success=False,
                start_block=start_block,
                end_block=end_block,
                error=error_msg,
            )

    async def fetch_transfers(
        self,
        hours_back: int = 24,
        output_dir: Optional[str] = None,
        chunk_size: int = 500,
    ) -> FetchResult:
        """
        Fetch ERC20 transfer events for specified time period.

        Args:
            hours_back: Hours to look back
            output_dir: Output directory
            chunk_size: Block chunk size for requests

        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Get block range
            latest_block = await self.get_latest_block()
            blocks_per_minute = 5  # Ethereum average
            minutes = hours_back * 60
            blocks_to_fetch = minutes * blocks_per_minute
            start_block = latest_block - blocks_to_fetch

            # ERC20 Transfer event signature from protocol config
            transfer_event = self.config.protocols.get_event_hash("erc20_transfer")

            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / "latest_transfers"

            # Clean previous data
            output_path = Path(output_dir)
            if output_path.exists():
                for f in output_path.glob("*.parquet"):
                    f.unlink()

            self.logger.info(
                f"Fetching transfers from block {start_block} to {latest_block}"
            )

            # Fetch in chunks to handle large ranges
            all_results = []
            current_start = start_block

            while current_start < latest_block:
                current_end = min(current_start + chunk_size, latest_block)

                result = await self.fetch_logs(
                    start_block=current_start,
                    end_block=current_end,
                    events=[transfer_event],
                    output_dir=str(output_dir),
                )

                if not result.success:
                    self.logger.warning(
                        f"Chunk {current_start}-{current_end} failed: {result.error}"
                    )
                else:
                    all_results.append(result)

                current_start = current_end + 1

            # Aggregate results
            if all_results:
                total_blocks = sum(r.fetched_blocks for r in all_results)
                return FetchResult(
                    success=True,
                    data_path=str(output_dir),
                    fetched_blocks=total_blocks,
                    start_block=start_block,
                    end_block=latest_block,
                    metadata={
                        "chunks_processed": len(all_results),
                        "hours_back": hours_back,
                        "event_type": "transfers",
                    },
                )
            else:
                return FetchResult(
                    success=False, error="No chunks were successfully processed"
                )

        except Exception as e:
            error_msg = f"Transfer fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)

    async def fetch_pool_events(
        self,
        protocol: str = "uniswap_v3",
        deployment_block: Optional[int] = None,
        output_dir: Optional[str] = None,
    ) -> FetchResult:
        """
        Fetch pool creation events for specified protocol.

        Args:
            protocol: Protocol name (uniswap_v3, sushiswap_v3, etc.)
            deployment_block: Protocol deployment block
            output_dir: Output directory

        Returns:
            FetchResult: Fetch operation result
        """
        try:
            # Get protocol configuration using centralized config
            factory_addresses = self.config.protocols.get_factory_addresses(
                protocol, self.chain
            )
            if not deployment_block:
                deployment_block = self.config.protocols.get_deployment_block(
                    protocol, self.chain
                )

            if not factory_addresses:
                return FetchResult(
                    success=False,
                    error=f"No factory addresses configured for {protocol} on {self.chain}",
                )

            # Pool created event signature from protocol config
            pool_created_event = self.config.protocols.get_event_hash(
                f"{protocol}_pool_created"
            )

            # Get latest block
            latest_block = await self.get_latest_block()

            # Setup output directory
            if not output_dir:
                data_dir = Path(self.config.base.DATA_DIR)
                output_dir = data_dir / self.chain / f"{protocol}_poolcreated_events"

            self.logger.info(
                f"Fetching {protocol} pool events from block {deployment_block} to {latest_block}"
            )

            result = await self.fetch_logs(
                start_block=deployment_block,
                end_block=latest_block,
                contracts=factory_addresses,
                events=[pool_created_event],
                output_dir=str(output_dir),
            )

            if result.success:
                result.metadata.update(
                    {
                        "protocol": protocol,
                        "factory_addresses": factory_addresses,
                        "deployment_block": deployment_block,
                    }
                )

            return result

        except Exception as e:
            error_msg = f"Pool event fetch failed: {str(e)}"
            self.logger.exception(error_msg)
            return FetchResult(success=False, error=error_msg)
