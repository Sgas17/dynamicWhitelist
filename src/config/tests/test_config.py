"""
Test suite for the centralized configuration system.

Tests configuration loading, validation, and integration between components.
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_config


class TestConfigurationSystem:
    """Test suite for configuration system."""
    
    @pytest.fixture(scope="class")
    def config(self):
        """Provide configuration instance for tests."""
        return get_config()
    
    def test_config_loads_successfully(self, config):
        """Test that configuration loads without errors."""
        assert config is not None
        assert hasattr(config, 'environment')
        assert config.environment in ['development', 'production', 'test', 'local']
    
    def test_database_configuration(self, config):
        """Test database configuration is properly loaded."""
        db_config = config.database
        
        # Test required attributes exist
        assert hasattr(db_config, 'postgres_url')
        assert hasattr(db_config, 'REDIS_HOST')
        assert hasattr(db_config, 'REDIS_PORT')
        assert hasattr(db_config, 'pools_table_name')
        
        # Test values are reasonable
        assert isinstance(db_config.REDIS_PORT, int)
        assert db_config.REDIS_PORT > 0
        assert len(db_config.pools_table_name) > 0
        
    def test_chain_configurations(self, config):
        """Test chain configurations for supported chains."""
        supported_chains = ["ethereum", "base", "arbitrum"]
        
        for chain_name in supported_chains:
            chain_config = config.chains.get_chain_config(chain_name)
            
            # Test required fields exist
            assert 'rpc_url' in chain_config
            assert 'chain_id' in chain_config
            assert 'blocks_per_minute' in chain_config
            
            # Test values are reasonable
            assert isinstance(chain_config['chain_id'], int)
            assert chain_config['chain_id'] > 0
            assert isinstance(chain_config['blocks_per_minute'], int)
            assert chain_config['blocks_per_minute'] > 0
            assert chain_config['rpc_url'].startswith(('http://', 'https://'))
    
    def test_chain_config_invalid_chain(self, config):
        """Test that invalid chain names raise appropriate errors."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            config.chains.get_chain_config("invalid_chain")
    
    @pytest.mark.parametrize("protocol,chain", [
        ("uniswap_v3", "ethereum"),
        ("uniswap_v3", "base"),
        ("uniswap_v2", "ethereum"),
        ("aerodrome_v3", "base"),
    ])
    def test_protocol_configurations(self, config, protocol, chain):
        """Test protocol configurations for various protocol/chain combinations."""
        factories = config.protocols.get_factory_addresses(protocol, chain)
        deployment_block = config.protocols.get_deployment_block(protocol, chain)
        
        # Test factory addresses
        assert isinstance(factories, list)
        assert len(factories) > 0
        for factory in factories:
            assert factory.startswith('0x')
            assert len(factory) == 42  # Ethereum address length
            
        # Test deployment block
        assert isinstance(deployment_block, int)
        assert deployment_block > 0
    
    def test_protocol_config_invalid_protocol(self, config):
        """Test that invalid protocol names raise appropriate errors."""
        with pytest.raises(ValueError, match="Unsupported protocol"):
            config.protocols.get_protocol_config("invalid_protocol", "ethereum")
    
    def test_event_hashes(self, config):
        """Test that event hashes are properly configured."""
        # These are the event types that get_event_hash() actually supports
        event_types = [
            "erc20_transfer",
            "uniswap_v2_pair_created", 
            "uniswap_v3_pool_created",
            "uniswap_v3_mint",
            "uniswap_v3_burn",
            "uniswap_v4_initialized",
            "uniswap_v4_modify_liquidity",
            "aerodrome_v2_pool_created",
            "aerodrome_v3_pool_created",
            "aerodrome_v3_mint",
            "aerodrome_v3_burn"
        ]
        
        for event_type in event_types:
            event_hash = config.protocols.get_event_hash(event_type)
            assert event_hash.startswith('0x')
            assert len(event_hash) == 66  # 0x + 64 hex chars
    
    def test_nats_configuration(self, config):
        """Test NATS configuration."""
        nats_config = config.nats
        
        # Test required attributes
        assert hasattr(nats_config, 'NATS_ENABLED')
        assert hasattr(nats_config, 'STREAM_NAME')
        assert hasattr(nats_config, 'whitelist_subjects')
        
        # Test NATS URL generation
        nats_url = nats_config.get_nats_url()
        assert isinstance(nats_url, str)
        if nats_config.NATS_ENABLED:
            assert nats_url.startswith('nats://')
            
        # Test subjects configuration - it's actually a list
        if hasattr(nats_config, 'whitelist_subjects'):
            assert isinstance(nats_config.whitelist_subjects, (dict, list))
    
    def test_combined_configurations(self, config):
        """Test combined configuration methods."""
        # Test chain + database config
        eth_db_config = config.get_chain_database_config("ethereum")
        assert 'chain_id' in eth_db_config
        # Check for database fields that should be present
        assert any(key.endswith('table_name') or key.startswith('redis') or 'postgres' in key 
                  for key in eth_db_config.keys())
        
        # Test protocol + chain config  
        uniswap_config = config.get_protocol_chain_config("uniswap_v3", "ethereum")
        assert 'factory_addresses' in uniswap_config
        assert 'deployment_block' in uniswap_config
        assert len(uniswap_config['factory_addresses']) > 0
        # Chain info might be in different key names
        assert any(key in uniswap_config for key in ['chain_id', 'chain', 'chain_name'])
        
        # Test NATS publishing config if method exists
        if hasattr(config, 'get_nats_publishing_config'):
            nats_token_config = config.get_nats_publishing_config(chain="ethereum")
            assert isinstance(nats_token_config, dict)
    
    def test_configuration_validation(self, config):
        """Test that configuration validation passes."""
        # This should not raise any exceptions
        config.validate_configuration()
    
    def test_base_configuration(self, config):
        """Test base configuration paths."""
        base_config = config.base
        
        assert hasattr(base_config, 'PROJECT_ROOT')
        assert hasattr(base_config, 'DATA_DIR')
        
        # Verify paths exist or can be created
        project_root = Path(base_config.PROJECT_ROOT)
        assert project_root.exists()
        
        data_dir = Path(base_config.DATA_DIR)
        # Data dir might not exist yet, but its parent should
        assert data_dir.parent.exists() or data_dir.exists()
    
    @pytest.mark.parametrize("chain", ["ethereum", "base", "arbitrum"])
    def test_supported_protocols_per_chain(self, config, chain):
        """Test that each chain has appropriate protocols configured."""
        supported_protocols = config.protocols.supported_protocols
        
        for protocol in supported_protocols:
            if protocol == "aerodrome_v2" and chain != "base":
                continue  # Aerodrome is Base-only
            if protocol == "aerodrome_v3" and chain != "base":
                continue  # Aerodrome is Base-only
                
            # Should not raise an exception for valid protocol/chain combinations
            factories = config.protocols.get_factory_addresses(protocol, chain)
            assert isinstance(factories, list)


class TestConfigurationErrors:
    """Test suite for configuration error handling."""
    
    def test_invalid_event_type(self):
        """Test that invalid event types raise appropriate errors."""
        config = get_config()
        with pytest.raises(ValueError, match="Unknown event type"):
            config.protocols.get_event_hash("invalid_event_type")
    
    def test_unsupported_protocol_chain_combination(self):
        """Test error handling for unsupported protocol/chain combinations."""
        config = get_config()
        
        # Aerodrome should only work on Base - it should return empty list for other chains
        factories = config.protocols.get_factory_addresses("aerodrome_v3", "ethereum")
        assert isinstance(factories, list)
        # Should be empty for unsupported chains
        assert len(factories) == 0


@pytest.mark.integration
class TestConfigurationIntegration:
    """Integration tests for configuration system."""
    
    def test_config_singleton_behavior(self):
        """Test that get_config() returns the same instance."""
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2
    
    def test_configuration_completeness(self):
        """Test that all required configuration sections are present."""
        config = get_config()
        
        required_sections = [
            'base', 'database', 'chains', 'protocols', 'nats'
        ]
        
        for section in required_sections:
            assert hasattr(config, section), f"Missing configuration section: {section}"
    
    def test_environment_specific_behavior(self):
        """Test configuration behavior in different environments."""
        config = get_config()
        
        # Test that environment is properly detected
        assert config.environment in ['development', 'production', 'test', 'local']
        
        # In development/test/local, some values might be different
        if config.environment in ['development', 'test', 'local']:
            # Should still have valid configuration
            assert config.database.REDIS_HOST
            assert config.chains.get_chain_config("ethereum")['rpc_url']


if __name__ == "__main__":
    # Allow running as script for backward compatibility
    pytest.main([__file__, "-v"])