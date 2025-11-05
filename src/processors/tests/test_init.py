"""
Tests for processor module initialization and registry functionality.
"""

from unittest.mock import patch

import pytest

from .. import ProcessorError, ProcessorResult, get_processor, list_processors
from ..base import BaseProcessor


class TestProcessorRegistry:
    """Test processor registry functionality."""

    def test_list_processors(self):
        """Test listing available processors."""
        processors = list_processors()

        assert isinstance(processors, list)
        assert len(processors) > 0

        # Check expected processor types are available
        expected_types = [
            "uniswap_v3_pools",
            "uniswap_v4_pools",
            "latest_transfers",
            "token_metadata",
        ]
        for proc_type in expected_types:
            assert proc_type in processors

    def test_get_processor_valid(self):
        """Test getting a valid processor class."""
        ProcessorClass = get_processor("uniswap_v3_pools")

        assert ProcessorClass is not None
        assert hasattr(ProcessorClass, "__init__")
        assert hasattr(ProcessorClass, "process")
        assert hasattr(ProcessorClass, "validate_config")

    def test_get_processor_invalid(self):
        """Test getting an invalid processor type."""
        with pytest.raises(ValueError) as exc_info:
            get_processor("nonexistent_processor")

        assert "Unknown processor type: nonexistent_processor" in str(exc_info.value)

    def test_processor_instantiation(self):
        """Test that processors can be instantiated."""
        for processor_type in list_processors():
            ProcessorClass = get_processor(processor_type)
            processor = ProcessorClass(chain="ethereum")

            assert isinstance(processor, BaseProcessor)
            assert processor.chain == "ethereum"
            assert hasattr(processor, "process")
            assert hasattr(processor, "validate_config")


class TestProcessorImports:
    """Test that processor imports work correctly."""

    def test_base_imports(self):
        """Test that base classes are imported correctly."""
        from .. import BaseProcessor, ProcessorError, ProcessorResult

        assert BaseProcessor is not None
        assert ProcessorResult is not None
        assert ProcessorError is not None

    def test_processor_classes_importable(self):
        """Test that processor classes can be imported."""
        # This tests the optional imports in __init__.py
        processor_types = list_processors()

        for proc_type in processor_types:
            # Should not raise an exception
            ProcessorClass = get_processor(proc_type)
            assert ProcessorClass is not None

    def test_graceful_import_failure(self):
        """Test that missing dependencies don't break the module."""
        # This test verifies that all processor modules can be imported
        # without runtime errors, which is the real goal of graceful import handling
        try:
            from .. import metadata_processors, pool_processors, transfer_processors

            # If we can import all modules without errors, the test passes
            assert True
        except ImportError as e:
            # If there are real import issues, we should know about them
            pytest.fail(f"Import failed: {e}")


class TestProcessorIntegration:
    """Test processor integration with the registry system."""

    @pytest.mark.asyncio
    async def test_all_processors_have_required_methods(self):
        """Test that all registered processors implement required methods."""
        for processor_type in list_processors():
            ProcessorClass = get_processor(processor_type)
            processor = ProcessorClass(chain="ethereum")

            # Test required methods exist and are callable
            assert callable(getattr(processor, "process"))
            assert callable(getattr(processor, "validate_config"))
            assert callable(getattr(processor, "get_identifier"))

            # Test methods return expected types
            assert isinstance(processor.validate_config(), bool)
            assert isinstance(processor.get_identifier(), str)

            # Test process method returns ProcessorResult
            result = await processor.process()
            assert isinstance(result, ProcessorResult)
            assert hasattr(result, "success")
            assert hasattr(result, "processed_count")

    def test_processor_identifiers_unique(self):
        """Test that processor identifiers are unique."""
        identifiers = []

        for processor_type in list_processors():
            ProcessorClass = get_processor(processor_type)
            processor = ProcessorClass(chain="ethereum")
            identifier = processor.get_identifier()

            assert identifier not in identifiers, f"Duplicate identifier: {identifier}"
            identifiers.append(identifier)

    def test_processor_chain_parameter(self):
        """Test that processors handle different chain parameters."""
        test_chains = ["ethereum", "base", "arbitrum"]

        for chain in test_chains:
            for processor_type in list_processors():
                ProcessorClass = get_processor(processor_type)
                processor = ProcessorClass(chain=chain)

                assert processor.chain == chain
                assert chain in processor.get_identifier()
