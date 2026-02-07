# intraday_historical_data.py
import requests
import pandas as pd
from datetime import datetime, timedelta

# Import EODHD SDK and API token
from eodhd import APIClient
from assets.credentials.api import EODHD_API_TOKEN
 
# Initialize EODHD API Client (SDK)
api = APIClient(EODHD_API_TOKEN)

# =============================================================================
# 2A. Intraday Historical Data - Using API Requests
# =============================================================================

_FROM = datetime.now() - timedelta(days=5) # equivalent to 1 week of trading activity
_TO = datetime.now()

def fetch_intraday_data_requests(
    symbol: str,
    exchange: str = "US",
    interval: str = "5m",
    from_date: datetime = _FROM,
    to_date: datetime = _TO,
    api_token: str = EODHD_API_TOKEN
) -> pd.DataFrame:
    """
    Fetch Intraday historical data using raw HTTP requests.
    """
    base_url = "https://eodhd.com/api/intraday"
    ticker = f"{symbol}.{exchange}"
    
    # Convert to Unix timestamps
    from_ts = int(from_date.timestamp())
    to_ts = int(to_date.timestamp())
    
    params = {
        "api_token": api_token,
        "fmt": "json",
        "interval": interval,
        "from": from_ts,
        "to": to_ts,
    }
    
    url = f"{base_url}/{ticker}"
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data)
    
    if not df.empty:
        # Handle datetime field (could be 'datetime' or 'timestamp')
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.set_index("datetime").sort_index()
        elif "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
            df = df.set_index("datetime").sort_index()
    
    return df

# =============================================================================
# 2B. Intraday Historical Data - Using EODHD Python SDK
# =============================================================================
def fetch_intraday_data_sdk(
    symbol: str,
    exchange: str = "US",
    interval: str = "5m",
    from_date: datetime = _FROM,
    to_date: datetime = _TO,
    api_client: APIClient = api
) -> pd.DataFrame:
    """
    Fetch Intraday historical data using the EODHD Python SDK.
    The SDK handles timestamp conversion internally.
    """
    ticker = f"{symbol}.{exchange}"
    
    # Convert to Unix timestamps (SDK expects strings)
    from_ts = str(int(from_date.timestamp()))
    to_ts = str(int(to_date.timestamp()))
    
    data = api_client.get_intraday_historical_data(
        symbol=ticker,
        interval=interval,
        from_unix_time=from_ts,
        to_unix_time=to_ts
    )
    
    df = pd.DataFrame(data)
    
    if not df.empty:
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.set_index("datetime").sort_index()
        elif "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
            df = df.set_index("datetime").sort_index()
    
    return df
