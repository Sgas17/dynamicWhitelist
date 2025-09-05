"""
Main task orchestrator implementation.
"""

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Callable, Any
from concurrent.futures import ThreadPoolExecutor
import heapq

from .base import (
    Task,
    TaskStatus,
    TaskResult,
    TaskError,
    DependencyError,
    TaskExecutor,
    TaskListener
)

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """
    Simple task orchestrator for coordinating data pipeline tasks.
    
    KISS principle: Just run tasks in the right order with basic error handling.
    """
    
    def __init__(self):
        """Initialize the orchestrator."""
        self.tasks: Dict[str, Task] = {}
        self.executors: Dict[str, TaskExecutor] = {}
        self.results: Dict[str, TaskResult] = {}
        
    def add_task(self, task: Task) -> None:
        """Add a task to run."""
        self.tasks[task.task_id] = task
        logger.info(f"Added task: {task.name}")
    
    def add_executor(self, task_type: str, executor: TaskExecutor) -> None:
        """Add an executor for a task type."""
        self.executors[task_type] = executor
        logger.info(f"Added executor for: {task_type}")
    
    async def run(self) -> Dict[str, Any]:
        """
        Run all tasks in dependency order.
        
        Returns:
            Dict with task results and basic stats
        """
        logger.info(f"Running {len(self.tasks)} tasks")
        
        # Simple approach: sort by dependencies and run sequentially
        ordered_tasks = self._resolve_dependencies()
        
        completed = 0
        failed = 0
        
        for task in ordered_tasks:
            try:
                result = await self._run_task(task)
                self.results[task.task_id] = result
                
                if result.success:
                    completed += 1
                    logger.info(f"✅ {task.name} completed")
                else:
                    failed += 1
                    logger.error(f"❌ {task.name} failed: {result.error}")
                    
            except Exception as e:
                failed += 1
                error_result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow(),
                    error=str(e)
                )
                self.results[task.task_id] = error_result
                logger.error(f"❌ {task.name} error: {e}")
        
        return {
            'completed': completed,
            'failed': failed,
            'total': len(self.tasks),
            'results': self.results
        }
    
    def _resolve_dependencies(self) -> List[Task]:
        """Simple dependency resolution - just sort by dependencies."""
        tasks = list(self.tasks.values())
        
        # Simple topological sort
        ordered = []
        remaining = tasks.copy()
        
        while remaining:
            # Find tasks with no unresolved dependencies
            ready = [
                task for task in remaining 
                if all(dep_id in [t.task_id for t in ordered] for dep_id in task.depends_on)
            ]
            
            if not ready:
                # Circular dependency or missing dependency
                logger.warning("Dependency resolution issue, running remaining tasks in order")
                ordered.extend(remaining)
                break
                
            ordered.extend(ready)
            for task in ready:
                remaining.remove(task)
        
        return ordered
    
    async def _run_task(self, task: Task) -> TaskResult:
        """Run a single task."""
        executor = self.executors.get(task.task_type)
        if not executor:
            raise TaskError(f"No executor for task type: {task.task_type}")
        
        task.mark_started()
        
        try:
            result = await executor.execute(task)
            task.mark_completed(result.output)
            return result
            
        except Exception as e:
            task.mark_failed(str(e))
            raise