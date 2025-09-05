#!/usr/bin/env python3
"""
Test script for the centralized configuration system.
This script demonstrates how to use the configuration system and validates it works correctly.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_config, ConfigError

def main():
    """Test the configuration system."""
    print("🧪 Testing dynamicWhitelist Configuration System")
    print("=" * 60)
    
    try:
        # Load configuration
        print("📋 Loading configuration...")
        config = get_config()
        print(f"✅ Configuration loaded for environment: {config.environment}")
        
        # Test database configuration
        print("\n🗄️  Database Configuration:")
        print(f"  PostgreSQL URL: {config.database.postgres_url}")
        print(f"  Redis Host: {config.database.REDIS_HOST}:{config.database.REDIS_PORT}")
        print(f"  Pools Table: {config.database.pools_table_name}")
        
        # Test chain configuration
        print("\n⛓️  Chain Configuration:")
        for chain_name in ["ethereum", "base", "arbitrum"]:
            try:
                chain_config = config.chains.get_chain_config(chain_name)
                print(f"  {chain_name.title()}:")
                print(f"    RPC URL: {chain_config['rpc_url']}")
                print(f"    Chain ID: {chain_config['chain_id']}")
                print(f"    Blocks/min: {chain_config['blocks_per_minute']}")
            except ValueError as e:
                print(f"  {chain_name.title()}: ❌ {e}")
        
        # Test protocol configuration
        print("\n🔄 Protocol Configuration:")
        protocols = ["uniswap_v3", "sushiswap_v2"]
        for protocol in protocols:
            print(f"  {protocol.title()}:")
            for chain in ["ethereum", "base"]:
                try:
                    factories = config.protocols.get_factory_addresses(protocol, chain)
                    deployment_block = config.protocols.get_deployment_block(protocol, chain)
                    print(f"    {chain}: {len(factories)} factories, block {deployment_block}")
                except (ValueError, KeyError):
                    print(f"    {chain}: Not configured")
        
        # Test NATS configuration
        print("\n📡 NATS Configuration:")
        print(f"  Enabled: {config.nats.NATS_ENABLED}")
        print(f"  URL: {config.nats.get_nats_url()}")
        print(f"  Stream: {config.nats.STREAM_NAME}")
        print(f"  Subjects: {len(config.nats.whitelist_subjects)} configured")
        
        # Test combined configurations
        print("\n🔗 Combined Configuration Examples:")
        
        # Chain + Database config
        eth_db_config = config.get_chain_database_config("ethereum")
        print(f"  Ethereum DB Config: Chain ID {eth_db_config['chain_id']}")
        
        # Protocol + Chain config
        uniswap_config = config.get_protocol_chain_config("uniswap_v3", "ethereum")
        print(f"  Uniswap V3 on Ethereum: {len(uniswap_config['factory_addresses'])} factories")
        
        # NATS publishing config
        nats_token_config = config.get_nats_publishing_config(chain="ethereum")
        print(f"  NATS Token Publishing: {nats_token_config['token_subject']}")
        
        # Validate configuration
        print("\n✅ Running configuration validation...")
        config.validate_configuration()
        print("✅ All configurations validated successfully!")
        
        print(f"\n🎉 Configuration system test completed successfully!")
        print(f"Environment: {config.environment}")
        print(f"Project Root: {config.base.PROJECT_ROOT}")
        print(f"Data Directory: {config.base.DATA_DIR}")
        
        return True
        
    except ConfigError as e:
        print(f"❌ Configuration Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)