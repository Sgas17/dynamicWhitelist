"""
Live unit test for V4 using direct StateView contract calls (degenbot approach).
"""

import pytest
from web3 import Web3
from eth_abi import encode, decode
from src.config import ConfigManager


class TestV4DirectStateView:
    """Test V4 using direct StateView contract calls."""

    @pytest.fixture(scope="class")
    def web3_connection(self):
        """Setup Web3 connection."""
        config_manager = ConfigManager()
        chain_config = config_manager.chains.get_chain_config("ethereum")
        web3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))
        return web3

    @pytest.fixture(scope="class") 
    def state_view_address(self):
        """StateView contract address from degenbot."""
        return "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"

    @pytest.fixture(scope="class")
    def test_pool_id(self):
        """Test V4 pool ID."""
        return "0x72331fcb696b0151904c03584b66dc8365bc63f8a144d89a773384e3a579ca73"

    def encode_function_call(self, function_signature: str, args: list) -> str:
        """Encode function call data."""
        # Get function selector
        function_selector = Web3.keccak(text=function_signature)[:4]
        
        # Extract parameter types
        param_start = function_signature.find("(") + 1
        param_end = function_signature.find(")")
        param_types = function_signature[param_start:param_end].split(",") if param_start < param_end else []
        param_types = [p.strip() for p in param_types if p.strip()]
        
        # Encode parameters
        encoded_params = encode(param_types, args) if param_types and args else b""
        
        return (function_selector + encoded_params).hex()

    @pytest.mark.asyncio
    async def test_contract_existence(self, web3_connection, state_view_address):
        """Test if StateView contract exists."""
        print(f"\n🔍 Testing StateView at: {state_view_address}")
        
        code = web3_connection.eth.get_code(state_view_address)
        print(f"   Contract code length: {len(code)} bytes")
        
        if len(code) > 0:
            print(f"   ✅ Contract exists")
        else:
            print(f"   ❌ No contract found")
            
        assert len(code) > 0, "StateView contract should exist"

    @pytest.mark.asyncio 
    async def test_get_slot0_direct(self, web3_connection, state_view_address, test_pool_id):
        """Test getting slot0 data directly."""
        print(f"\n🔍 Testing getSlot0 for pool: {test_pool_id[:10]}...")
        
        pool_id_bytes = bytes.fromhex(test_pool_id.replace("0x", ""))
        call_data = self.encode_function_call("getSlot0(bytes32)", [pool_id_bytes])
        
        result = web3_connection.eth.call({
            "to": state_view_address,
            "data": "0x" + call_data,
            "gas": 1000000
        })
        
        print(f"   Raw result: {result.hex()}")
        
        # Decode result
        sqrt_price_x96, tick, protocol_fee, lp_fee = decode(["uint160", "int24", "uint24", "uint24"], result)
        
        print(f"   ✅ sqrt Price: {sqrt_price_x96}")
        print(f"   ✅ Current Tick: {tick}")
        print(f"   ✅ Protocol Fee: {protocol_fee}")
        print(f"   ✅ LP Fee: {lp_fee}")
        
        assert sqrt_price_x96 > 0, "Should have positive sqrt price"
        assert abs(tick) < 1000000, "Tick should be reasonable"

    @pytest.mark.asyncio
    async def test_get_tick_bitmap_direct(self, web3_connection, state_view_address, test_pool_id):
        """Test getting tick bitmap directly."""
        print(f"\n🔍 Testing getTickBitmap for pool: {test_pool_id[:10]}...")
        
        pool_id_bytes = bytes.fromhex(test_pool_id.replace("0x", ""))
        
        # Test different word positions
        test_words = [-13, -12, 0, 1, 12, 75]
        
        for word_pos in test_words:
            print(f"\n   📍 Word {word_pos}:")
            call_data = self.encode_function_call("getTickBitmap(bytes32,int16)", [pool_id_bytes, word_pos])
            
            result = web3_connection.eth.call({
                "to": state_view_address,
                "data": "0x" + call_data,
                "gas": 1000000
            })
            
            (bitmap,) = decode(["uint256"], result)
            initialized_count = bin(bitmap).count("1") if bitmap > 0 else 0
            
            if bitmap > 0:
                print(f"     ✅ 0x{bitmap:016x} ({initialized_count} bits set)")
                
                # Show sample ticks
                sample_bits = [i for i in range(256) if bitmap & (1 << i)][:3]
                if sample_bits:
                    sample_ticks = [((word_pos << 8) + bit_pos) * 60 for bit_pos in sample_bits]
                    print(f"     Sample ticks: {sample_ticks}")
            else:
                print(f"     ❌ Empty (0x{bitmap:016x})")
