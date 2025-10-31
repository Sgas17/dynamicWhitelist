"""
Token matching processor for whitelist generation.

Processes exchange token data and matches against CoinGecko database to generate
comprehensive token whitelists.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from .base import BaseProcessor, ProcessorResult
from ..fetchers.exchange_fetchers import ExchangeToken


@dataclass
class TokenMatch:
    """Represents a matched token between exchange and database."""
    symbol: str
    exchange_token: ExchangeToken
    chain_address: str
    chain: str
    confidence: float  # 0.0 to 1.0 matching confidence
    match_type: str  # 'exact_symbol', 'mapped_symbol', 'fuzzy_match'
    coingecko_id: Optional[str] = None
    token_name: Optional[str] = None
    decimals: Optional[int] = None
    additional_data: Optional[Dict[str, Any]] = None


class TokenMatchingProcessor(BaseProcessor):
    """
    Processes exchange tokens and matches them against database tokens.
    
    KISS: Focused on matching exchange tokens with on-chain tokens using CoinGecko data.
    """
    
    def __init__(self, chain: str = "multi", protocol: str = "token_matching"):
        """Initialize token matching processor."""
        super().__init__(chain, protocol)
        
        # Chain platform mappings for CoinGecko
        self.chain_platform_mapping = {
            'ethereum': 'ethereum',
            'base': 'base',
            'arbitrum': 'arbitrum-one',
            'optimism': 'optimistic-ethereum',
            'polygon': 'polygon-pos',
            'avalanche': 'avalanche',
            'fantom': 'fantom',
            'binance': 'binance-smart-chain'
        }
        
        # Manual symbol mappings for common exchange differences
        self.symbol_mappings = {
            'BTC': 'WBTC',  # Most exchanges list BTC but chains have WBTC
            'ETH': 'WETH',  # Similar for ETH -> WETH on some chains
        }
    
    def validate_config(self) -> bool:
        """Validate processor configuration."""
        return True  # Token matching doesn't require specific chain config
    
    def _process_single_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single event (not used for token matching).
        
        Token matching operates on batches of exchange tokens rather than individual events.
        This method is implemented to satisfy BaseProcessor interface but returns None.
        """
        return None
    
    async def process(
        self,
        exchange_tokens: List[ExchangeToken],
        target_chains: Optional[List[str]] = None,
        min_confidence: float = 0.7,
        output_file: Optional[str] = None,
        **kwargs
    ) -> ProcessorResult:
        """
        Process exchange tokens and match against database.
        
        Args:
            exchange_tokens: List of tokens from exchange fetchers
            target_chains: Chains to match against (default: ethereum, base, arbitrum)
            min_confidence: Minimum confidence threshold for matches
            output_file: Optional output file for results
            
        Returns:
            ProcessorResult with matched tokens
        """
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Use default chains if not specified
            if target_chains is None:
                target_chains = ['ethereum', 'base', 'arbitrum']
            
            self.logger.info(f"Processing {len(exchange_tokens)} exchange tokens for {len(target_chains)} chains")
            
            # Load token metadata from CoinGecko database
            token_metadata = await self._load_coingecko_metadata(target_chains)
            
            if not token_metadata:
                return ProcessorResult(
                    success=False,
                    error="Failed to load CoinGecko token metadata"
                )
            
            # Perform token matching
            matched_tokens = []
            unmatched_tokens = []
            
            for exchange_token in exchange_tokens:
                matches = await self._find_token_matches(exchange_token, token_metadata)
                
                # Take the best match above confidence threshold
                best_match = None
                for match in matches:
                    if match.confidence >= min_confidence:
                        if best_match is None or match.confidence > best_match.confidence:
                            best_match = match
                
                if best_match:
                    matched_tokens.append(best_match)
                else:
                    unmatched_tokens.append(exchange_token)
            
            # Calculate statistics
            coverage_by_chain = {}
            for match in matched_tokens:
                chain = match.chain
                coverage_by_chain[chain] = coverage_by_chain.get(chain, 0) + 1
            
            match_stats = self._get_match_type_stats(matched_tokens)
            
            # Save results if output file specified
            if output_file:
                await self._save_results(matched_tokens, output_file)
            
            self.logger.info(f"Successfully matched {len(matched_tokens)}/{len(exchange_tokens)} tokens")
            
            return ProcessorResult(
                success=True,
                data=matched_tokens,
                processed_count=len(matched_tokens),
                metadata={
                    "total_exchange_tokens": len(exchange_tokens),
                    "matched_tokens": len(matched_tokens),
                    "unmatched_tokens": len(unmatched_tokens),
                    "coverage_by_chain": coverage_by_chain,
                    "match_type_stats": match_stats,
                    "min_confidence": min_confidence,
                    "target_chains": target_chains,
                    "unmatched_sample": [
                        {"symbol": t.base, "exchange": t.exchange, "market_id": t.market_id}
                        for t in unmatched_tokens[:20]  # Sample for debugging
                    ]
                }
            )
            
        except Exception as e:
            error_msg = f"Token matching processing failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _load_coingecko_metadata(self, target_chains: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Load token metadata from CoinGecko database tables."""
        platform_tokens = {}
        
        # Convert chain names to CoinGecko platform IDs
        target_platforms = []
        for chain in target_chains:
            platform = self.chain_platform_mapping.get(chain)
            if platform:
                target_platforms.append(platform)
        
        if not target_platforms:
            self.logger.warning("No valid platforms found for target chains")
            return {}
        
        try:
            async with self.storage.pool.acquire() as conn:
                # Query joined data from CoinGecko tables
                query = """
                    SELECT 
                        t.id as coingecko_id,
                        t.symbol,
                        t.name,
                        t.market_cap_rank,
                        tp.platform,
                        tp.address,
                        tp.decimals,
                        tp.total_supply
                    FROM coingecko_tokens t
                    JOIN coingecko_token_platforms tp ON t.id = tp.token_id
                    WHERE tp.platform = ANY($1)
                        AND tp.address IS NOT NULL
                        AND tp.address != ''
                        AND LENGTH(tp.address) > 10
                    ORDER BY t.market_cap_rank ASC, t.symbol ASC
                """
                
                results = await conn.fetch(query, target_platforms)
                
                # Group by platform
                for row in results:
                    platform = row['platform']
                    if platform not in platform_tokens:
                        platform_tokens[platform] = []
                    
                    platform_tokens[platform].append({
                        'coingecko_id': row['coingecko_id'],
                        'symbol': row['symbol'].upper(),
                        'name': row['name'],
                        'market_cap_rank': row['market_cap_rank'],
                        'platform': row['platform'],
                        'address': row['address'],
                        'decimals': row['decimals'],
                        'total_supply': row['total_supply']
                    })
                
                # Log summary
                total_tokens = sum(len(tokens) for tokens in platform_tokens.values())
                self.logger.info(f"Loaded {total_tokens} tokens from CoinGecko database:")
                for platform, tokens in platform_tokens.items():
                    self.logger.info(f"  {platform}: {len(tokens)} tokens")
                
        except Exception as e:
            self.logger.error(f"Failed to load CoinGecko metadata: {e}")
            return {}
        
        return platform_tokens
    
    async def _find_token_matches(
        self, 
        exchange_token: ExchangeToken, 
        token_metadata: Dict[str, List[Dict[str, Any]]]
    ) -> List[TokenMatch]:
        """Find all possible matches for an exchange token across chains."""
        matches = []
        exchange_symbol = exchange_token.base.upper()
        
        # Check manual symbol mappings
        mapped_symbol = self.symbol_mappings.get(exchange_symbol, exchange_symbol)
        
        # Search across all platforms
        for platform, tokens in token_metadata.items():
            # Convert platform back to our chain name
            chain = self._platform_to_chain(platform)
            if not chain:
                continue
            
            for token_data in tokens:
                confidence, match_type = self._calculate_match_confidence(
                    exchange_symbol,
                    mapped_symbol, 
                    token_data
                )
                
                if confidence > 0.0:  # Include all potential matches
                    match = TokenMatch(
                        symbol=exchange_symbol,
                        exchange_token=exchange_token,
                        chain_address=token_data['address'],
                        chain=chain,
                        confidence=confidence,
                        match_type=match_type,
                        coingecko_id=token_data['coingecko_id'],
                        token_name=token_data['name'],
                        decimals=token_data['decimals'],
                        additional_data={
                            'platform': platform,
                            'market_cap_rank': token_data['market_cap_rank'],
                            'total_supply': str(token_data['total_supply'])
                        }
                    )
                    matches.append(match)
        
        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x.confidence, reverse=True)
        return matches
    
    def _calculate_match_confidence(
        self,
        exchange_symbol: str,
        mapped_symbol: str,
        token_data: Dict[str, Any]
    ) -> Tuple[float, str]:
        """Calculate matching confidence between exchange token and CoinGecko token."""
        cg_symbol = token_data['symbol'].upper()
        
        # Exact symbol match (highest confidence)
        if exchange_symbol == cg_symbol:
            return 0.95, 'exact_symbol'
        
        # Mapped symbol match 
        if mapped_symbol == cg_symbol:
            return 0.90, 'mapped_symbol'
        
        # Check for common variations
        if self._symbol_variations_match(exchange_symbol, cg_symbol):
            return 0.80, 'symbol_variation'
        
        # Market cap rank boost for popular tokens with fuzzy match
        if token_data.get('market_cap_rank') and token_data['market_cap_rank'] <= 100:
            if self._fuzzy_symbol_match(exchange_symbol, cg_symbol):
                return 0.75, 'top_token_fuzzy'
        
        # Regular fuzzy matching
        if self._fuzzy_symbol_match(exchange_symbol, cg_symbol):
            return 0.50, 'fuzzy_match'
        
        return 0.0, 'no_match'
    
    def _symbol_variations_match(self, symbol1: str, symbol2: str) -> bool:
        """Check for common symbol variations."""
        variations1 = {
            symbol1,
            symbol1.replace('W', ''),     # WETH -> ETH
            symbol1.replace('.E', ''),    # USDC.E -> USDC
            symbol1.replace('1000', ''),  # 1000PEPE -> PEPE
        }
        
        variations2 = {
            symbol2,
            symbol2.replace('W', ''),
            symbol2.replace('.E', ''),
            symbol2.replace('1000', ''),
        }
        
        return bool(variations1 & variations2)
    
    def _fuzzy_symbol_match(self, symbol1: str, symbol2: str) -> bool:
        """Simple fuzzy matching for symbols."""
        if len(symbol1) >= 3 and len(symbol2) >= 3:
            # Check if shorter symbol is contained in longer
            if symbol1 in symbol2 or symbol2 in symbol1:
                return True
        return False
    
    def _platform_to_chain(self, platform: str) -> Optional[str]:
        """Convert CoinGecko platform ID back to our chain name."""
        for chain, cg_platform in self.chain_platform_mapping.items():
            if cg_platform == platform:
                return chain
        return None
    
    def _get_match_type_stats(self, matches: List[TokenMatch]) -> Dict[str, int]:
        """Get statistics on match types."""
        stats = {}
        for match in matches:
            match_type = match.match_type
            stats[match_type] = stats.get(match_type, 0) + 1
        return stats
    
    async def _save_results(self, matched_tokens: List[TokenMatch], output_file: str) -> None:
        """Save matched tokens to file."""
        import ujson
        from datetime import datetime
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare export data
        export_data = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "processor": "TokenMatchingProcessor",
                "total_matches": len(matched_tokens)
            },
            "matched_tokens": [
                {
                    "symbol": match.symbol,
                    "chain": match.chain,
                    "chain_address": match.chain_address,
                    "exchange": match.exchange_token.exchange,
                    "market_type": match.exchange_token.market_type,
                    "confidence": round(match.confidence, 3),
                    "match_type": match.match_type,
                    "coingecko_id": match.coingecko_id,
                    "token_name": match.token_name,
                    "decimals": match.decimals
                }
                for match in matched_tokens
            ]
        }
        
        with open(output_path, 'w') as f:
            ujson.dump(export_data, f, indent=2)
        
        self.logger.info(f"Saved {len(matched_tokens)} matched tokens to {output_path}")
    
    async def process_with_all_chains(
        self,
        exchange_tokens: List[ExchangeToken],
        target_chains: Optional[List[str]] = None,
        min_confidence: float = 0.7,
        output_file: Optional[str] = None,
        **kwargs
    ) -> ProcessorResult:
        """
        Process exchange tokens and collect ALL chain matches for each token.
        
        Unlike the regular process() method which returns the best match per token,
        this method collects all valid matches across chains for whitelist generation.
        
        Args:
            exchange_tokens: List of tokens from exchange fetchers
            target_chains: Chains to match against (default: ethereum, base, arbitrum)
            min_confidence: Minimum confidence threshold for matches
            output_file: Optional output file for results
            
        Returns:
            ProcessorResult with tokens that have chain_addresses dict for each chain
        """
        try:
            if not self.validate_config():
                return ProcessorResult(success=False, error="Invalid config")
            
            # Use default chains if not specified
            if target_chains is None:
                target_chains = ['ethereum', 'base', 'arbitrum']
            
            self.logger.info(f"Processing {len(exchange_tokens)} exchange tokens for all chains: {target_chains}")
            
            # Load token metadata from CoinGecko database
            token_metadata = await self._load_coingecko_metadata(target_chains)
            
            if not token_metadata:
                return ProcessorResult(
                    success=False,
                    error="Failed to load CoinGecko token metadata"
                )
            
            # Process tokens and collect all chain matches
            processed_tokens = []
            unmatched_tokens = []
            
            for exchange_token in exchange_tokens:
                matches = await self._find_token_matches(exchange_token, token_metadata)
                
                # Collect all matches above confidence threshold per chain
                chain_addresses = {}
                best_match_data = None
                highest_confidence = 0.0
                
                for match in matches:
                    if match.confidence >= min_confidence:
                        chain = match.chain
                        
                        # Keep the best match for this chain
                        if chain not in chain_addresses or match.confidence > chain_addresses[chain]['confidence']:
                            chain_addresses[chain] = {
                                'address': match.chain_address,
                                'confidence': match.confidence,
                                'match_type': match.match_type,
                                'coingecko_id': match.coingecko_id,
                                'token_name': match.token_name,
                                'decimals': match.decimals,
                                'additional_data': match.additional_data
                            }
                        
                        # Track overall best match for primary metadata
                        if match.confidence > highest_confidence:
                            highest_confidence = match.confidence
                            best_match_data = match
                
                # Create token entry if we found matches
                if chain_addresses and best_match_data:
                    token_entry = {
                        'symbol': exchange_token.base,
                        'exchange_symbol': exchange_token.symbol,
                        'exchange': exchange_token.exchange,
                        'market_id': exchange_token.market_id,
                        'market_type': exchange_token.market_type,
                        
                        # Primary token metadata from best match
                        'coingecko_id': best_match_data.coingecko_id,
                        'token_name': best_match_data.token_name,
                        'primary_chain': best_match_data.chain,
                        'primary_confidence': best_match_data.confidence,
                        'primary_match_type': best_match_data.match_type,
                        
                        # All chain addresses
                        'chain_addresses': chain_addresses,
                        'supported_chains': list(chain_addresses.keys()),
                        'chain_count': len(chain_addresses)
                    }
                    
                    processed_tokens.append(token_entry)
                else:
                    unmatched_tokens.append(exchange_token)
            
            # Calculate statistics
            total_addresses = sum(len(token['chain_addresses']) for token in processed_tokens)
            coverage_by_chain = {}
            for token in processed_tokens:
                for chain in token['supported_chains']:
                    coverage_by_chain[chain] = coverage_by_chain.get(chain, 0) + 1
            
            # Multi-chain distribution stats
            multi_chain_tokens = [t for t in processed_tokens if t['chain_count'] > 1]
            single_chain_tokens = [t for t in processed_tokens if t['chain_count'] == 1]
            
            # Save results if output file specified
            if output_file:
                await self._save_all_chains_results(processed_tokens, output_file)
            
            self.logger.info(f"Successfully processed {len(processed_tokens)}/{len(exchange_tokens)} tokens")
            self.logger.info(f"Total chain addresses collected: {total_addresses}")
            self.logger.info(f"Multi-chain tokens: {len(multi_chain_tokens)}, Single-chain: {len(single_chain_tokens)}")
            
            return ProcessorResult(
                success=True,
                data=processed_tokens,
                processed_count=len(processed_tokens),
                metadata={
                    "total_exchange_tokens": len(exchange_tokens),
                    "processed_tokens": len(processed_tokens),
                    "unmatched_tokens": len(unmatched_tokens),
                    "total_addresses": total_addresses,
                    "coverage_by_chain": coverage_by_chain,
                    "multi_chain_tokens": len(multi_chain_tokens),
                    "single_chain_tokens": len(single_chain_tokens),
                    "multi_chain_distribution": {
                        str(count): len([t for t in processed_tokens if t['chain_count'] == count])
                        for count in range(1, max([t['chain_count'] for t in processed_tokens], default=1) + 1)
                    },
                    "min_confidence": min_confidence,
                    "target_chains": target_chains,
                    "unmatched_sample": [
                        {"symbol": t.base, "exchange": t.exchange, "market_id": t.market_id}
                        for t in unmatched_tokens[:10]  # Sample for debugging
                    ]
                }
            )
            
        except Exception as e:
            error_msg = f"All-chains token processing failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return ProcessorResult(success=False, error=error_msg)
    
    async def _save_all_chains_results(self, processed_tokens: List[Dict[str, Any]], output_file: str) -> None:
        """Save processed tokens with all chain addresses to file."""
        import ujson
        from datetime import datetime
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare export data
        export_data = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "processor": "TokenMatchingProcessor",
                "method": "process_with_all_chains",
                "total_tokens": len(processed_tokens),
                "total_addresses": sum(len(t['chain_addresses']) for t in processed_tokens)
            },
            "tokens": []
        }
        
        for token in processed_tokens:
            # Convert chain addresses to a cleaner format for export
            chain_data = {}
            for chain, data in token['chain_addresses'].items():
                chain_data[chain] = {
                    "address": data['address'],
                    "confidence": round(data['confidence'], 3),
                    "match_type": data['match_type'],
                    "decimals": data['decimals']
                }
            
            export_data["tokens"].append({
                "symbol": token['symbol'],
                "exchange_symbol": token['exchange_symbol'],
                "coingecko_id": token['coingecko_id'],
                "token_name": token['token_name'],
                "supported_chains": token['supported_chains'],
                "chain_count": token['chain_count'],
                "chain_addresses": chain_data,
                "exchange_data": {
                    "exchange": token['exchange'],
                    "market_id": token['market_id'],
                    "market_type": token['market_type']
                }
            })
        
        with open(output_path, 'w') as f:
            ujson.dump(export_data, f, indent=2)
        
        self.logger.info(f"Saved {len(processed_tokens)} tokens with all chain addresses to {output_path}")