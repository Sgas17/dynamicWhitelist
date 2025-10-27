#!/usr/bin/env python3
"""
Simple test for the task orchestrator.
KISS: Just test basic functionality.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../../'))

from src.core.orchestrator import TaskOrchestrator, Task
from src.core.orchestrator.tasks import DataFetchTask, ProcessorTask, StorageTask
from src.core.storage import StorageManager


async def test_simple_pipeline():
    """Test a simple data pipeline."""
    print("=== Testing Simple Task Orchestrator ===")
    
    # Create orchestrator and storage
    orchestrator = TaskOrchestrator()
    
    async with StorageManager() as storage:
        # Add executors
        orchestrator.add_executor("data_fetch", DataFetchTask(storage))
        orchestrator.add_executor("process", ProcessorTask(storage))
        orchestrator.add_executor("store", StorageTask(storage))
        
        # Create simple pipeline tasks
        fetch_task = Task(
            name="fetch_uniswap_pools",
            task_type="data_fetch",
            parameters={
                'chain': 'ethereum',
                'protocol': 'uniswap_v3'
            }
        )
        
        process_task = Task(
            name="process_pool_data",
            task_type="process",
            depends_on=[fetch_task.task_id],
            parameters={
                'input_data': {}
            }
        )
        
        store_task = Task(
            name="store_processed_data",
            task_type="store",
            depends_on=[process_task.task_id],
            parameters={
                'data': [{'test': 'data'}],
                'type': 'json'
            }
        )
        
        # Add tasks
        orchestrator.add_task(fetch_task)
        orchestrator.add_task(process_task)
        orchestrator.add_task(store_task)
        
        # Run pipeline
        results = await orchestrator.run()
        
        # Show results
        print(f"\n✅ Pipeline completed!")
        print(f"   Total tasks: {results['total']}")
        print(f"   Completed: {results['completed']}")
        print(f"   Failed: {results['failed']}")
        
        # Show task details
        for task_id, result in results['results'].items():
            task_name = orchestrator.tasks[task_id].name
            status = "✅" if result.success else "❌"
            print(f"   {status} {task_name}")
        
        return results['failed'] == 0


async def test_dependency_resolution():
    """Test that tasks run in correct dependency order."""
    print("\n=== Testing Dependency Resolution ===")
    
    orchestrator = TaskOrchestrator()
    
    async with StorageManager() as storage:
        orchestrator.add_executor("data_fetch", DataFetchTask(storage))
        
        # Create tasks with dependencies
        task_c = Task(name="task_c", task_type="data_fetch")
        task_b = Task(name="task_b", task_type="data_fetch", depends_on=[task_c.task_id])
        task_a = Task(name="task_a", task_type="data_fetch", depends_on=[task_b.task_id])
        
        # Add in wrong order to test dependency resolution
        orchestrator.add_task(task_a)
        orchestrator.add_task(task_c) 
        orchestrator.add_task(task_b)
        
        results = await orchestrator.run()
        
        print(f"✅ Dependency test completed!")
        print(f"   All tasks: {results['completed']} completed, {results['failed']} failed")
        
        return results['failed'] == 0


async def main():
    """Run all tests."""
    print("=" * 50)
    print("Task Orchestrator Test Suite")
    print("=" * 50)
    
    tests = [
        ("Simple Pipeline", test_simple_pipeline),
        ("Dependency Resolution", test_dependency_resolution)
    ]
    
    passed = 0
    for name, test_func in tests:
        try:
            success = await test_func()
            if success:
                print(f"\n✅ {name}: PASSED")
                passed += 1
            else:
                print(f"\n❌ {name}: FAILED")
        except Exception as e:
            print(f"\n❌ {name}: ERROR - {e}")
    
    print(f"\n" + "=" * 50)
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All orchestrator tests passed!")
    else:
        print("⚠️  Some tests failed")
    
    return passed == len(tests)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)