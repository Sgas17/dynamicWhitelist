"""
Simple task executors for common pipeline operations.
KISS: Only implement what we actually need right now.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from .base import Task, TaskResult, TaskStatus, TaskExecutor, TaskError
from ..storage import StorageManager

logger = logging.getLogger(__name__)


class DataFetchTask(TaskExecutor):
    """
    Enhanced data fetching task executor using chain-specific fetchers.
    Integrates with the new fetcher framework.
    """
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def can_handle(self, task: Task) -> bool:
        return task.task_type == "data_fetch"
    
    async def execute(self, task: Task) -> TaskResult:
        """Execute data fetch task using chain-specific fetchers."""
        start_time = datetime.utcnow()
        
        try:
            # Get fetch parameters
            fetch_type = task.parameters.get('fetch_type')
            chain = task.parameters.get('chain', 'ethereum')
            fetch_params = task.parameters.get('fetch_params', {})
            
            if not fetch_type:
                # Fallback to simple fetching for backward compatibility
                return await self._simple_fetch(task, start_time)
            
            # Use chain-specific fetcher
            from ...fetchers import get_fetcher
            
            try:
                FetcherClass = get_fetcher(chain)
                rpc_url = task.parameters.get('rpc_url')  # Allow override
                fetcher = FetcherClass(rpc_url) if rpc_url else FetcherClass()
                
                # Execute appropriate fetch method
                if fetch_type == "transfers":
                    result = await fetcher.fetch_recent_transfers(**fetch_params)
                elif fetch_type == "uniswap_v3_pools":
                    result = await fetcher.fetch_uniswap_v3_pools(**fetch_params)
                elif fetch_type == "liquidity_events":
                    result = await fetcher.fetch_liquidity_events(**fetch_params)
                elif fetch_type == "logs":
                    result = await fetcher.fetch_logs(**fetch_params)
                else:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        error=f"Unknown fetch type: {fetch_type}"
                    )
                
                if result.success:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.COMPLETED,
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        output={
                            'fetch_type': fetch_type,
                            'chain': chain,
                            'data_path': result.data_path,
                            'fetched_blocks': result.fetched_blocks,
                            'start_block': result.start_block,
                            'end_block': result.end_block,
                            'metadata': result.metadata
                        }
                    )
                else:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        error=result.error
                    )
                    
            except ValueError as e:
                logger.error(f"Unknown chain or fetcher: {chain}")
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    start_time=start_time,
                    end_time=datetime.utcnow(),
                    error=str(e)
                )
            
        except Exception as e:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error=str(e)
            )
    
    async def _simple_fetch(self, task: Task, start_time: datetime) -> TaskResult:
        """Simple fetch fallback for backward compatibility."""
        chain = task.parameters.get('chain', 'unknown')
        protocol = task.parameters.get('protocol', 'unknown')
        
        logger.info(f"Fetching {protocol} data for {chain} (simple mode)")
        
        # Simulate work
        result_data = {
            'chain': chain,
            'protocol': protocol,
            'fetched_at': start_time.isoformat(),
            'status': 'simulated'
        }
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            start_time=start_time,
            end_time=datetime.utcnow(),
            output=result_data
        )


class ProcessorTask(TaskExecutor):
    """
    Enhanced processor task executor that uses modular processors.
    Integrates with the new processor framework.
    """
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def can_handle(self, task: Task) -> bool:
        return task.task_type == "process"
    
    async def execute(self, task: Task) -> TaskResult:
        """Execute processing task using modular processors."""
        start_time = datetime.utcnow()
        
        try:
            # Get processor type and parameters
            processor_type = task.parameters.get('processor_type')
            processor_params = task.parameters.get('processor_params', {})
            
            if not processor_type:
                # Fallback to simple processing for backward compatibility
                return await self._simple_process(task, start_time)
            
            # Use modular processor
            from ...processors import get_processor
            
            try:
                ProcessorClass = get_processor(processor_type)
                chain = task.parameters.get('chain', 'ethereum')
                processor = ProcessorClass(chain=chain)
                
                # Execute processor
                result = await processor.process(**processor_params)
                
                if result.success:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.COMPLETED,
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        output={
                            'processor_type': processor_type,
                            'processed_count': result.processed_count,
                            'metadata': result.metadata,
                            'data_sample': result.data[:3] if result.data else []  # First 3 items
                        }
                    )
                else:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        start_time=start_time,
                        end_time=datetime.utcnow(),
                        error=result.error
                    )
                    
            except ValueError as e:
                logger.error(f"Unknown processor type: {processor_type}")
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    start_time=start_time,
                    end_time=datetime.utcnow(),
                    error=str(e)
                )
            
        except Exception as e:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error=str(e)
            )
    
    async def _simple_process(self, task: Task, start_time: datetime) -> TaskResult:
        """Simple processing fallback for backward compatibility."""
        input_data = task.parameters.get('input_data', {})
        
        logger.info(f"Processing data for task {task.name} (simple mode)")
        
        processed_data = {
            'processed_at': start_time.isoformat(),
            'input_count': len(input_data),
            'status': 'processed'
        }
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            start_time=start_time,
            end_time=datetime.utcnow(),
            output=processed_data
        )


class StorageTask(TaskExecutor):
    """
    Simple executor for storage tasks.
    Saves data to database/cache/files.
    """
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def can_handle(self, task: Task) -> bool:
        return task.task_type == "store"
    
    async def execute(self, task: Task) -> TaskResult:
        """Execute storage task."""
        start_time = datetime.utcnow()
        
        try:
            data = task.parameters.get('data', [])
            chain = task.parameters.get('chain', 'ethereum')
            storage_type = task.parameters.get('type', 'json')
            
            logger.info(f"Storing {len(data)} items to {storage_type}")
            
            stored_count = 0
            
            # Store based on type
            if storage_type == 'json':
                # Save to JSON
                self.storage.json.save(f"{task.name}_output.json", data)
                stored_count = len(data)
                
            elif storage_type == 'database':
                # Store to database (simplified)
                if isinstance(data, list) and data:
                    stored_count = await self.storage.postgres.store_tokens_batch(data, chain)
            
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                output={'stored_count': stored_count}
            )
            
        except Exception as e:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error=str(e)
            )


class PublishTask(TaskExecutor):
    """
    Simple executor for publishing tasks.
    Publishes results to NATS or other systems.
    """
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def can_handle(self, task: Task) -> bool:
        return task.task_type == "publish"
    
    async def execute(self, task: Task) -> TaskResult:
        """Execute publish task."""
        start_time = datetime.utcnow()
        
        try:
            data = task.parameters.get('data', {})
            destination = task.parameters.get('destination', 'nats')
            
            logger.info(f"Publishing to {destination}")
            
            # Simple publish simulation
            # In real implementation, this would use NATS client
            
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                output={'published': True, 'destination': destination}
            )
            
        except Exception as e:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error=str(e)
            )