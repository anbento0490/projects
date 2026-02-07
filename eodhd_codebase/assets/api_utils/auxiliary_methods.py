import pandas as pd
from typing import Dict, List, Union

_RENAME_MAPS = {
    "crypto": {"s": "symbol", "p": "price", "q": "quantity", "dc": "daily_change_pct", "dd": "daily_diff", "t": "timestamp_ms"},
    "forex": {"s": "symbol", "a": "ask", "b": "bid", "dc": "daily_change_pct", "dd": "daily_diff", "t": "timestamp_ms"},
    "us": {"s": "symbol", "p": "price", "v": "volume", "c": "condition", "dp": "dark_pool", "ms": "market_status", "t": "timestamp_ms"},
    "us-quote": {"s": "symbol", "ap": "ask_price", "as": "ask_size", "bp": "bid_price", "bs": "bid_size", "t": "timestamp_ms"},
}

# ===================================================================================================== #
def to_live_df(data: Union[Dict, List[Dict]], tz: str = "America/New_York") -> pd.DataFrame:
    
    """Convert live price JSON to DataFrame with readable timestamps."""
    
    if isinstance(data, dict): data = [data]
    df = pd.DataFrame(data)
    if not df.empty and "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s").dt.tz_localize("UTC").dt.tz_convert(tz)
        df["datetime_str"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        cols = [c for c in ["code", "datetime_str", "datetime", "timestamp", "open", "high", "low", "close", "volume", "previousClose", "change", "change_p"] if c in df.columns]
        df = df[cols]
    return df

# ===================================================================================================== #
def to_ws_df(data: Union[Dict, List[Dict]], endpoint: str = "crypto", tz: str = "UTC") -> pd.DataFrame:

    """Convert WebSocket streaming data to DataFrame with readable timestamps."""

    if isinstance(data, dict): data = [data]
    if not data: return pd.DataFrame()
    df = pd.DataFrame(data)
    if not df.empty and "t" in df.columns:
        df["datetime"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        if tz != "UTC": df["datetime"] = df["datetime"].dt.tz_convert(tz)
        df["datetime_str"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        rename_map = {k: v for k, v in _RENAME_MAPS.get(endpoint, {"t": "timestamp_ms"}).items() if k in df.columns}
        df = df.rename(columns=rename_map)
        priority_cols = ["datetime_str", "datetime", "symbol"]
        cols = [c for c in priority_cols if c in df.columns] + [c for c in df.columns if c not in priority_cols]
        df = df[cols]
    return df
