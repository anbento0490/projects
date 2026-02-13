
# historical_data.py
import requests
import pandas as pd
from datetime import datetime, timedelta

# Import EODHD SDK and API token
from eodhd import APIClient
from assets.credentials.api import EODHD_API_TOKEN

# Initialize EODHD API Client (SDK)
api = APIClient(EODHD_API_TOKEN)

# =============================================================================
# 1A. End-of-Day (EOD) Historical Data - Using API Requests
# =============================================================================
def fetch_eod_data_requests(
    symbol: str,
    exchange: str = "US",
    period: str = "d",
    order: str = "a",
    from_date: str = None,
    to_date: str = None,
    api_token: str = EODHD_API_TOKEN
) -> pd.DataFrame:
    """Fetch daily OHLCV data via raw HTTP requests."""
    base_url = "https://eodhd.com/api/eod"
    ticker = f"{symbol}.{exchange}"
    
    params = {
        "api_token": api_token,
        "fmt": "json",
        "period": period,
        "order": order,
    }
    
    if from_date is not None:
        params["from"] = from_date
    if to_date is not None:
        params["to"] = to_date
    
    response = requests.get(f"{base_url}/{ticker}", params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data)
    
    if not df.empty:
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
        elif "timestamp" in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"], unit="s")
            df = df.set_index("date").sort_index()
    
    return df


import requests
import pandas as pd

# =============================================================================
# 1B. End-of-Day (EOD) Historical Data - Using EODHD Python SDK
# =============================================================================
def fetch_eod_data_sdk(
    symbol: str,
    exchange: str = "US",
    period: str = "d",
    order: str = "a",
    from_date: str = None,
    to_date: str = None,
    api_client: APIClient = api
) -> pd.DataFrame:
    """Fetch daily OHLCV data via the official SDK."""
    ticker = f"{symbol}.{exchange}"
    
    kwargs = {"symbol": ticker, "period": period, "order": order}

    if from_date is not None:
        kwargs["from_date"] = from_date
    if to_date is not None:
        kwargs["to_date"] = to_date
    
    data = api_client.get_eod_historical_stock_market_data(**kwargs)
    
    df = pd.DataFrame(data)
    
    if not df.empty:
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
        elif "timestamp" in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"], unit="s")
            df = df.set_index("date").sort_index()
    
    return df
