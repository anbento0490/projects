# intraday_live_delayed_data.py
import requests
from typing import Dict, List, Union

# Import EODHD SDK and API token
from eodhd import APIClient
from assets.credentials.api import EODHD_API_TOKEN

# Initialize EODHD API Client (SDK)
api = APIClient(EODHD_API_TOKEN)

# =============================================================================
# 3A. Live (Delayed) Stock Prices - Using API Requests
# =============================================================================
def fetch_live_price_requests(
    symbol: str,
    exchange: str = "US",
    additional_symbols: List[str] = None,
    api_token: str = EODHD_API_TOKEN
) -> Union[Dict, List[Dict]]:
    
    """Fetch live (delayed) stock prices via raw HTTP requests."""

    base_url = "https://eodhd.com/api/real-time"
    ticker = f"{symbol}.{exchange}"
    params = {"api_token": api_token, "fmt": "json"}

    if additional_symbols:
        params["s"] = ",".join(additional_symbols)
        
    response = requests.get(f"{base_url}/{ticker}", params=params)
    response.raise_for_status()

    return response.json()

# =============================================================================
# 3B. Live (Delayed) Stock Prices - Using EODHD Python SDK
# =============================================================================
def fetch_live_price_sdk(
    symbol: str,
    exchange: str = "US",
    additional_symbols: List[str] = None,
    api_client: APIClient = api
) -> Union[Dict, List[Dict]]:
    
    """Fetch live (delayed) stock prices via the official SDK."""

    ticker = f"{symbol}.{exchange}"
    s_param = ",".join(additional_symbols) if additional_symbols else None

    return api_client.get_live_stock_prices(ticker=ticker, s=s_param)