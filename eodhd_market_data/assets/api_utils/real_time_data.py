# real_time_data.py
import json, time, websockets, asyncio, nest_asyncio
import pandas as pd
from datetime import datetime, timedelta
from IPython.display import display, clear_output
from typing import List

from assets.credentials.api import EODHD_API_TOKEN
from eodhd import WebSocketClient

nest_asyncio.apply()

# Allowed Websocket Endpoints (URIs):
# US Stocks (Trades) -> "wss://ws.eodhistoricaldata.com/ws/us"
# US Stocks (Quotes) -> "wss://ws.eodhistoricaldata.com/ws/us-quote"
# Forex -> "wss://ws.eodhistoricaldata.com/ws/forex"
# Crypto -> "wss://ws.eodhistoricaldata.com/ws/crypto"

# =============================================================================
# Helper Functions
# =============================================================================
def _build_rows(latest: dict) -> List[dict]:
    """Convert latest price dict to list of row dicts."""
    return [{"symbol": d["s"], "price": round(float(d["p"]), 2),
             "datetime": pd.to_datetime(d["t"], unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
             "timestamp_ms": d["t"]} for d in latest.values() if d and d.get("s")]

# =============================================================================
# 4. Real-Time Data Using WebSockets - Using Pure websockets Library (Async)
# =============================================================================

async def stream_live_data(symbols: List[str], 
                           duration: int = 10, 
                           sample_interval: int = 1) -> pd.DataFrame:
    
    """Stream data, sampling one price per symbol every `sample_interval` seconds."""
    
    #####
    endpoint = f"wss://ws.eodhistoricaldata.com/ws/crypto?api_token={EODHD_API_TOKEN}"
    df = pd.DataFrame(columns=["symbol", "price", "timestamp_ms", "datetime"])
    #####

    async with websockets.connect(endpoint) as ws:
        await ws.send(json.dumps({"action": "subscribe", "symbols": ",".join(symbols)}))
        n_samples = duration // sample_interval
        
        for i in range(n_samples):
            latest = {sym: None for sym in symbols}
            interval_start = time.time()

            while time.time() - interval_start < sample_interval:
                try:
                    data = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.5))
                    if data.get("s") in latest: latest[data["s"]] = data
                except asyncio.TimeoutError: continue
            
            new_rows = _build_rows(latest)
            if new_rows: df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
            
            clear_output(wait=True)
            print(f"Streaming (Pure websockets) in progress... Sample {i+1}/{n_samples} | {len(df)} rows\n")
            display(df)
    
    clear_output(wait=True)
    print(f"Streaming Completed: {len(df)} prices over {duration}s\n")
    return df

# =============================================================================
# 4. Real-Time Data Using WebSockets - Using EODHD SDK WebSocketClient
# =============================================================================
def stream_live_data_sdk(symbols: List[str], duration: int = 10, sample_interval: int = 1) -> pd.DataFrame:
    
    """Stream data using SDK, sampling one price per symbol every `sample_interval` seconds."""
    
    client = WebSocketClient(api_key=EODHD_API_TOKEN, 
                             endpoint="crypto", 
                             symbols=symbols, 
                             store_data=True, 
                             display_stream=False)
    client.start()
    df = pd.DataFrame(columns=["symbol", "price", "timestamp_ms", "datetime"])
    n_samples = duration // sample_interval
    
    for i in range(n_samples):
        time.sleep(sample_interval)
        messages = client.get_data()
        latest = {sym: None for sym in symbols}
        for msg in messages:
            data = json.loads(msg) if isinstance(msg, str) else msg
            if data.get("s") in latest: latest[data["s"]] = data
        
        new_rows = _build_rows(latest)
        if new_rows: df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        
        clear_output(wait=True)
        print(f"Streaming (SDK) in progress... Sample {i+1}/{n_samples} | {len(df)} rows\n")
        display(df)
    
    client.stop()
    clear_output(wait=True)
    print(f"Streaming Completed: {len(df)} prices over {duration}s\n")
    return df