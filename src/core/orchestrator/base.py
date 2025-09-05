"""
Base classes and types for the task orchestrator.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Awaitable
import uuid

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class TaskError(Exception):
    """Base exception for task-related errors."""
    pass


class DependencyError(TaskError):
    """Raised when task dependencies cannot be resolved."""
    pass


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate task execution duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED


@dataclass
class Task:
    """
    Base task definition for the orchestrator.
    
    Tasks are the atomic units of work in the pipeline.
    Each task has a unique ID, dependencies, and execution logic.
    """
    name: str
    task_type: str
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    # Task configuration
    chain: Optional[str] = None
    protocol: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Execution settings
    priority: int = 0  # Higher priority = executed first
    timeout: int = 300  # Timeout in seconds
    retry_count: int = 3
    retry_delay: int = 5  # Delay between retries in seconds
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)  # List of task IDs
    required_outputs: List[str] = field(default_factory=list)  # Required output keys
    
    # State
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Runtime data
    output: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    attempt: int = 0
    
    def __post_init__(self):
        """Post-initialization setup."""
        if not self.task_id:
            self.task_id = str(uuid.uuid4())
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get task execution duration."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    @property
    def is_complete(self) -> bool:
        """Check if task is in a final state."""
        return self.status in [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.SKIPPED
        ]
    
    @property
    def success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED
    
    def can_run(self, completed_tasks: Set[str]) -> bool:
        """
        Check if task can run based on dependencies.
        
        Args:
            completed_tasks: Set of completed task IDs
            
        Returns:
            bool: True if all dependencies are satisfied
        """
        if not self.depends_on:
            return True
        return all(dep_id in completed_tasks for dep_id in self.depends_on)
    
    def mark_started(self) -> None:
        """Mark task as started."""
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.utcnow()
        self.attempt += 1
    
    def mark_completed(self, output: Optional[Dict[str, Any]] = None) -> None:
        """Mark task as completed successfully."""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        if output:
            self.output.update(output)
    
    def mark_failed(self, error: str) -> None:
        """Mark task as failed."""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error = error
    
    def mark_cancelled(self) -> None:
        """Mark task as cancelled."""
        self.status = TaskStatus.CANCELLED
        self.completed_at = datetime.utcnow()
    
    def should_retry(self) -> bool:
        """Check if task should be retried."""
        return (
            self.status == TaskStatus.FAILED and
            self.attempt < self.retry_count
        )
    
    def reset_for_retry(self) -> None:
        """Reset task state for retry."""
        self.status = TaskStatus.PENDING
        self.started_at = None
        self.completed_at = None
        self.error = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary representation."""
        return {
            'task_id': self.task_id,
            'name': self.name,
            'task_type': self.task_type,
            'chain': self.chain,
            'protocol': self.protocol,
            'parameters': self.parameters,
            'priority': self.priority,
            'timeout': self.timeout,
            'retry_count': self.retry_count,
            'retry_delay': self.retry_delay,
            'depends_on': self.depends_on,
            'required_outputs': self.required_outputs,
            'status': self.status.value,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'output': self.output,
            'error': self.error,
            'attempt': self.attempt,
            'duration_seconds': self.duration.total_seconds() if self.duration else None
        }


class TaskExecutor(ABC):
    """
    Abstract base class for task executors.
    
    Task executors contain the actual business logic for different task types.
    """
    
    @abstractmethod
    async def execute(self, task: Task) -> TaskResult:
        """
        Execute a task and return the result.
        
        Args:
            task: Task to execute
            
        Returns:
            TaskResult: Result of task execution
            
        Raises:
            TaskError: If task execution fails
        """
        pass
    
    @abstractmethod
    def can_handle(self, task: Task) -> bool:
        """
        Check if this executor can handle the given task.
        
        Args:
            task: Task to check
            
        Returns:
            bool: True if this executor can handle the task
        """
        pass
    
    async def validate_task(self, task: Task) -> None:
        """
        Validate task before execution.
        
        Args:
            task: Task to validate
            
        Raises:
            TaskError: If task validation fails
        """
        # Default validation - override in subclasses
        if not task.name:
            raise TaskError("Task name is required")
        
        if not task.task_type:
            raise TaskError("Task type is required")


class TaskListener(ABC):
    """
    Abstract base class for task event listeners.
    
    Listeners can be registered to receive notifications about task events.
    """
    
    @abstractmethod
    async def on_task_started(self, task: Task) -> None:
        """Called when a task starts execution."""
        pass
    
    @abstractmethod
    async def on_task_completed(self, task: Task, result: TaskResult) -> None:
        """Called when a task completes successfully."""
        pass
    
    @abstractmethod
    async def on_task_failed(self, task: Task, result: TaskResult) -> None:
        """Called when a task fails."""
        pass
    
    @abstractmethod
    async def on_task_retrying(self, task: Task, attempt: int) -> None:
        """Called when a task is being retried."""
        pass


# Type aliases for better readability
TaskExecutorFunc = Callable[[Task], Awaitable[TaskResult]]
TaskValidatorFunc = Callable[[Task], Awaitable[None]]
TaskListenerFunc = Callable[[Task, TaskResult], Awaitable[None]]