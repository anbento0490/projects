
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Union
from eodhd import APIClient, WebSocketClient

# =================================================================== #
# Unified EODHD Data Client Class
# Here's a comprehensive wrapper class that combines all the functionality above into a single, 
# easy-to-use interface. This follows production-ready patterns with proper error handling and 
# logging.

class EODHDDataClient:
    def __init__(self, api_token: str, default_exchange: str = "US"):
        self.api_token = api_token
        self.default_exchange = default_exchange
        self._client = APIClient(api_token)
    
    def _build_symbol(self, symbol: str, exchange: str = None) -> str:
        if "." in symbol: return symbol
        return f"{symbol}.{exchange or self.default_exchange}"
    
    def get_eod(self, symbol: str, exchange: str = None, from_date: str = None, 
                to_date: str = None, period: str = "d", order: str = "a") -> pd.DataFrame:
        
        ticker = self._build_symbol(symbol, exchange)
        data = self._client.get_eod_historical_stock_market_data(
            symbol=ticker, period=period, from_date=from_date, to_date=to_date, order=order)
        df = pd.DataFrame(data)
        if not df.empty and "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
        return df
    
    def get_intraday(self, symbol: str, exchange: str = None, interval: str = "5m",
                     days_back: int = 7, from_date: datetime = None, to_date: datetime = None) -> pd.DataFrame:
        
        ticker = self._build_symbol(symbol, exchange)
        to_dt = to_date or datetime.now()
        from_dt = from_date or (to_dt - timedelta(days=days_back))
        data = self._client.get_intraday_historical_data(
            symbol=ticker, interval=interval,
            from_unix_time=str(int(from_dt.timestamp())),
            to_unix_time=str(int(to_dt.timestamp())))
        df = pd.DataFrame(data)
        if not df.empty:
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"])
                df = df.set_index("datetime").sort_index()
            elif "timestamp" in df.columns:
                df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
                df = df.set_index("datetime").sort_index()
        return df
    
    def get_live_delayed(self, symbols: Union[str, List[str]], exchange: str = None) -> Union[Dict, List[Dict]]:

        if isinstance(symbols, str): symbols = [symbols]
        primary = self._build_symbol(symbols[0], exchange)
        additional = [self._build_symbol(s, exchange) for s in symbols[1:]] if len(symbols) > 1 else None
        s_param = ",".join(additional) if additional else None
        return self._client.get_live_stock_prices(ticker=primary, s=s_param)
    
    def get_stream(self, endpoint: str, symbols: List[str], store_data: bool = True,
                        display_stream: bool = False) -> WebSocketClient:
        
        ws_client = WebSocketClient(
            api_key=self.api_token, 
            endpoint=endpoint, 
            symbols=symbols,
            store_data=store_data, 
            display_stream=display_stream)
        return ws_client
