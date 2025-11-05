"""
Exchange market fetchers using CCXT for centralized exchange data.

Follows the same patterns as blockchain fetchers but targets CEX APIs instead of RPC.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import ccxt

from ..config.manager import ConfigManager
from .base import FetchError, FetchResult

logger = logging.getLogger(__name__)


@dataclass
class ExchangeToken:
    """Standardized token representation from exchanges."""

    symbol: str
    base: str
    quote: str
    market_id: str
    exchange: str
    is_active: bool
    market_type: str  # 'spot', 'swap', 'futures'
    additional_data: Optional[Dict[str, Any]] = None


class BaseExchangeFetcher:
    """
    Base class for exchange market fetchers using CCXT.

    Follows same patterns as BaseFetcher but for centralized exchanges.
    """

    def __init__(self, exchange_id: str, config: Optional[ConfigManager] = None):
        """
        Initialize exchange fetcher.

        Args:
            exchange_id: CCXT exchange identifier (e.g., 'hyperliquid')
            config: Configuration manager instance
        """
        self.exchange_id = exchange_id
        self.config = config or ConfigManager()
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

        # Initialize CCXT exchange
        self.ccxt_exchange = self._create_exchange()

    def _create_exchange(self) -> ccxt.Exchange:
        """Create CCXT exchange instance with configuration."""
        try:
            exchange_class = getattr(ccxt, self.exchange_id)

            # Basic configuration
            exchange_config = {
                "enableRateLimit": True,
                "timeout": 30000,  # 30 seconds
            }

            return exchange_class(exchange_config)

        except AttributeError:
            raise FetchError(f"Unsupported exchange: {self.exchange_id}")
        except Exception as e:
            self.logger.error(f"Failed to create {self.exchange_id} exchange: {e}")
            raise FetchError(f"Exchange creation failed: {e}")

    async def fetch_markets(self, market_type: str = "swap") -> FetchResult:
        """
        Fetch markets from the exchange.

        Args:
            market_type: Type of markets to fetch ('spot', 'swap', 'futures')

        Returns:
            FetchResult with market data
        """
        try:
            self.logger.info(
                f"Fetching {market_type} markets from {self.exchange_id}..."
            )

            # Fetch markets using CCXT in executor to avoid blocking
            loop = asyncio.get_event_loop()

            if market_type == "swap":
                markets = await loop.run_in_executor(
                    None, self.ccxt_exchange.fetchSwapMarkets
                )
            elif market_type == "spot":
                all_markets = await loop.run_in_executor(
                    None, self.ccxt_exchange.fetchMarkets
                )
                markets = [m for m in all_markets if m.get("spot", False)]
            else:
                raise FetchError(f"Unsupported market type: {market_type}")

            if not markets:
                return FetchResult(
                    success=False,
                    error=f"No {market_type} markets returned from {self.exchange_id}",
                )

            # Filter and convert markets to tokens
            tokens = []
            for market in markets:
                if self._should_include_market(market):
                    token = self._create_exchange_token(market, market_type)
                    if token:
                        tokens.append(token)

            self.logger.info(
                f"Fetched {len(tokens)} tokens from {len(markets)} {self.exchange_id} markets"
            )

            return FetchResult(
                success=True,
                data_path=None,  # No file path for exchange data
                fetched_blocks=len(tokens),  # Use token count instead of blocks
                metadata={
                    "exchange": self.exchange_id,
                    "market_type": market_type,
                    "total_markets": len(markets),
                    "filtered_tokens": len(tokens),
                    "tokens": tokens,
                },
            )

        except Exception as e:
            error_msg = f"Failed to fetch {self.exchange_id} markets: {str(e)}"
            self.logger.error(error_msg)
            return FetchResult(success=False, error=error_msg)

    def _should_include_market(self, market: Dict[str, Any]) -> bool:
        """
        Determine if a market should be included in results.

        Override in subclasses for exchange-specific filtering.

        Args:
            market: Raw market data from CCXT

        Returns:
            True if market should be included
        """
        # Default: include active markets with valid base assets
        try:
            if not market.get("active", True):
                return False

            base = market.get("base", "")
            if not base or len(base) < 2:
                return False

            return True

        except Exception:
            return False

    def _create_exchange_token(
        self, market: Dict[str, Any], market_type: str = "spot"
    ) -> Optional[ExchangeToken]:
        """Create ExchangeToken from CCXT market data."""
        try:
            return ExchangeToken(
                symbol=market.get("symbol", ""),
                base=market.get("base", ""),
                quote=market.get("quote", ""),
                market_id=market.get("id", ""),
                exchange=self.exchange_id,
                is_active=market.get("active", True),
                market_type=market_type,
                additional_data=market,
            )
        except Exception as e:
            self.logger.warning(f"Failed to create token from market {market}: {e}")
            return None


class HyperliquidFetcher(BaseExchangeFetcher):
    """
    Hyperliquid exchange market fetcher.

    Focuses on perpetual swap markets for token discovery.
    """

    def __init__(self, config: Optional[ConfigManager] = None):
        """Initialize Hyperliquid fetcher."""
        super().__init__("hyperliquid", config)

    def _should_include_market(self, market: Dict[str, Any]) -> bool:
        """
        Include active swap markets with valid base assets.

        Args:
            market: CCXT market data

        Returns:
            True if market should be included
        """
        try:
            # Must be active
            if not market.get("active", True):
                return False

            # Must have valid base asset
            base = market.get("base", "")
            if not base or len(base) < 2:
                return False

            # Accept USD quotes (Hyperliquid standard)
            quote = market.get("quote", "")
            if quote in ["USD", "USDC"]:
                return True

            return True

        except Exception:
            return False


class BinanceFetcher(BaseExchangeFetcher):
    """
    Binance exchange market fetcher.

    Example implementation for extensibility.
    """

    def __init__(self, config: Optional[ConfigManager] = None):
        """Initialize Binance fetcher."""
        super().__init__("binance", config)

    def _should_include_market(self, market: Dict[str, Any]) -> bool:
        """
        Include active markets with major quote assets.

        Args:
            market: CCXT market data

        Returns:
            True if market should be included
        """
        try:
            # Must be active
            if not market.get("active", True):
                return False

            # Must have valid base asset
            base = market.get("base", "")
            if not base or len(base) < 2:
                return False

            # Focus on major quote assets
            quote = market.get("quote", "")
            if quote in ["USDT", "USDC", "BTC", "ETH"]:
                return True

            return False

        except Exception:
            return False
