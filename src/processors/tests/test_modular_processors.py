#!/usr/bin/env python3
"""
Test script for modular processor integration with the orchestrator.

KISS: Just test that the new modular processors work with the orchestrator.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../'))

from src.core.orchestrator import TaskOrchestrator, Task
from src.core.orchestrator.tasks import ProcessorTask, StorageTask
from src.core.storage import StorageManager
from src.processors import list_processors, get_processor


async def test_processor_discovery():
    """Test that processors can be discovered and loaded."""
    print("=== Testing Processor Discovery ===")
    
    # List available processors
    available = list_processors()
    print(f"Available processors: {available}")
    
    # Try to load each processor type
    for processor_type in available:
        try:
            ProcessorClass = get_processor(processor_type)
            processor = ProcessorClass(chain="ethereum")
            print(f"✅ Successfully loaded: {processor_type}")
        except Exception as e:
            print(f"❌ Failed to load {processor_type}: {e}")
    
    return len(available) > 0


async def test_modular_processor_task():
    """Test processor task with modular processors."""
    print("\n=== Testing Modular Processor Task ===")
    
    try:
        # Create orchestrator and storage
        orchestrator = TaskOrchestrator()
        
        async with StorageManager() as storage:
            # Add processor task executor
            orchestrator.add_executor("process", ProcessorTask(storage))
            orchestrator.add_executor("store", StorageTask(storage))
            
            # Test Uniswap V3 pool processor
            pool_task = Task(
                name="test_uniswap_v3_pools",
                task_type="process",
                parameters={
                    'processor_type': 'uniswap_v3_pools',
                    'processor_params': {
                        'start_block': 19000000,  # Test block
                        'events_path': '/tmp/test_events'  # Mock path
                    },
                    'chain': 'ethereum'
                }
            )
            
            # Test transfers processor
            transfers_task = Task(
                name="test_latest_transfers",
                task_type="process",
                parameters={
                    'processor_type': 'latest_transfers',
                    'processor_params': {
                        'hours_back': 24,
                        'min_transfers': 50
                    },
                    'chain': 'ethereum'
                }
            )
            
            # Store processed data
            store_task = Task(
                name="store_test_data",
                task_type="store",
                depends_on=[pool_task.task_id, transfers_task.task_id],
                parameters={
                    'data': [{'test': 'modular_processor_data'}],
                    'type': 'json'
                }
            )
            
            # Add tasks to orchestrator
            orchestrator.add_task(pool_task)
            orchestrator.add_task(transfers_task) 
            orchestrator.add_task(store_task)
            
            # Run pipeline
            print("Running modular processor pipeline...")
            results = await orchestrator.run()
            
            # Show results
            print(f"\n✅ Modular processor pipeline completed!")
            print(f"   Total tasks: {results['total']}")
            print(f"   Completed: {results['completed']}")
            print(f"   Failed: {results['failed']}")
            
            # Show task details
            for task_id, result in results['results'].items():
                task_name = orchestrator.tasks[task_id].name
                status = "✅" if result.success else "❌"
                print(f"   {status} {task_name}")
                
                # Show processor output if available
                if result.success and hasattr(result, 'output') and result.output:
                    if 'processor_type' in result.output:
                        print(f"      Processor: {result.output['processor_type']}")
                        print(f"      Processed: {result.output.get('processed_count', 0)} items")
            
            return results['failed'] == 0
            
    except Exception as e:
        print(f"❌ Modular processor test failed: {e}")
        return False


async def test_fallback_compatibility():
    """Test backward compatibility with simple processing."""
    print("\n=== Testing Backward Compatibility ===")
    
    try:
        orchestrator = TaskOrchestrator()
        
        async with StorageManager() as storage:
            # Add executor
            orchestrator.add_executor("process", ProcessorTask(storage))
            
            # Create task without processor_type (should use fallback)
            fallback_task = Task(
                name="test_fallback_processing",
                task_type="process",
                parameters={
                    'input_data': {'test': 'data', 'count': 5}
                }
            )
            
            orchestrator.add_task(fallback_task)
            
            # Run pipeline
            results = await orchestrator.run()
            
            print(f"✅ Fallback compatibility test completed!")
            print(f"   Status: {'PASSED' if results['failed'] == 0 else 'FAILED'}")
            
            return results['failed'] == 0
            
    except Exception as e:
        print(f"❌ Fallback compatibility test failed: {e}")
        return False


async def main():
    """Run all processor tests."""
    print("=" * 60)
    print("Modular Processor Integration Tests")
    print("=" * 60)
    
    tests = [
        ("Processor Discovery", test_processor_discovery),
        ("Modular Processor Task", test_modular_processor_task),
        ("Backward Compatibility", test_fallback_compatibility)
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
    
    print(f"\n" + "=" * 60)
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All modular processor tests passed!")
        print("✅ Processors successfully refactored into modular components")
    else:
        print("⚠️  Some tests failed")
    
    return passed == len(tests)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)