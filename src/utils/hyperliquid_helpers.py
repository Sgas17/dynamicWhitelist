import ccxt 


def get_hyperliquid_symbols_and_prices():
        """Get symbols and their current prices from Hyperliquid"""
        try:
            hyperliquid = ccxt.hyperliquid()
            markets = hyperliquid.fetchSwapMarkets() 
            tickers = hyperliquid.fetchTickers()  
            
            symbol_prices = {}
            for market in markets:
                base_symbol = market["base"].lower()
                market_symbol = market["symbol"]  
                
                # Get price from tickers
                if market_symbol in tickers and tickers[market_symbol]['last']:
                    price = float(tickers[market_symbol]['last'])
                    symbol_prices[base_symbol] = price
                    
            #logger.info(f"Retrieved {len(symbol_prices)} symbols with prices from Hyperliquid")
            return symbol_prices
            
        except Exception:
            #logger.error(f"Failed to get Hyperliquid prices: {e}")
            # Fallback to just symbols without prices
            hyperliquid = ccxt.hyperliquid()
            markets = hyperliquid.fetchSwapMarkets()  
            symbols = [x["base"].lower() for x in markets]
            return {symbol: None for symbol in symbols}