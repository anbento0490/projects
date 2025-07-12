import requests, logging, pandas as pd
from datetime import date
from typing import List
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
######
class IntradayStockData:
    def __init__(self, api_key, engine, schema_name, table_name):
        self.api_key = api_key
        self.engine = engine
        self.schema_name = schema_name
        self.table_name = table_name
        print(f'Class IntradayStockData has been initialised!\n\n')
        
    def get_intraday_historical_data(self, symbol: str) -> pd.DataFrame:
        """
        Download full 15-minute intraday history for a ticker
        and return **only** the rows not yet in the database.

        The method:
        1. Calls AlphaVantage.
        2. Builds a DataFrame limited to US market hours (09:30-16:00).
        3. Drops dates already stored in the target table.

        """
        stock_data = []
    
        logging.info(f'Fetching [historical] intraday [15 mins] data for {symbol} from API...')
        intraday_url = (f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
                        f'&symbol={symbol}&interval=15min&outputsize=full&apikey={self.api_key}')
        
        response = requests.get(intraday_url)
        data = response.json()
        
        time_series = data.get("Time Series (15min)")
    
        if time_series is None:
            print(f"Warning: No data found for symbol {symbol}. This may be due to API rate limits or an API error.")
        
        for timestamp, values in time_series.items():
            record = {
                "ticker": symbol,                  
                "datetime": timestamp,           
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }
            stock_data.append(record)
    
        logging.info(f'Fetched {len(stock_data)} records for {symbol} symbol.\n')
        logging.info(f'Creating DataFrame with fetched data...')
    
        df = pd.DataFrame(stock_data)
    
        logging.info(f'DataFrame SUCCESSFULLY created!\n')
        logging.info(f'Filtering records in between 9:30 AM and 4 PM...')
    
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['time'] = df['datetime'].dt.time
    
        start_time = pd.to_datetime('09:30:00').time()
        end_time = pd.to_datetime('16:00:00').time()
    
        df = df[(df['time'] >= start_time) & (df['time'] <= end_time)]
        df.drop(columns=['time'], inplace=True)
    
        logging.info(f'Records filtered - STEP #1!\n')
    
        df['date'] = df['datetime'].dt.date
    
        dates_available = self.get_available_dates(symbol, )
        dates_new = df['date'].unique().tolist()
        missing_dates = [date for date in dates_new if date not in dates_available]
    
        logging.info(f"There is (are) {len(missing_dates)} date(s) fetched by the API, but not yet processed for {symbol} ")
        logging.info(f"Missing dates are:\n{missing_dates}")
        logging.info(f"Original dataframe will be filtered to only include these dates.")
        
        df = df[df['date'].isin(missing_dates)]
        logging.info(f'Records filtered - STEP #2!')
    
        cols = df.columns.tolist()
        cols.remove('date') 
        cols.insert(1, 'date') 
        df = df[cols]
        df.sort_values(by=["ticker", "datetime"], ascending=[True, False], inplace=True)
        df.reset_index(drop=True, inplace=True)
    
        logging.info(f'Final dataframe [to be inserted in DB] has shape {df.shape}\n\n')
        return df
    
    def write_df_to_database(self, insert_strategy: str, index_strategy: bool, df: pd.DataFrame) -> None:
        """
        Bulk-insert a DataFrame into target table.
        """
        df['inserted_at'] = pd.Timestamp.now()
    
        df.to_sql(
            name=self.table_name,
            con=self.engine,                    
            schema=self.schema_name,
            if_exists=insert_strategy,
            index=index_strategy,
            method='multi'
        )          
        logging.info(f"Inserted into {self.schema_name}.{self.table_name}")
        logging.info(f'###############################\n\n')
    
    def get_available_dates(self, symbol: str) -> List[date]:
        """
        Query the database to get available dates for a specific ticker.
        """
        query = f"""
            SELECT DISTINCT DATE(datetime) AS date
            FROM {self.schema_name}.{self.table_name}
            WHERE ticker = :ticker
            ORDER BY date;
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"ticker": symbol})
            available_dates = [row[0] for row in result.fetchall()]
        return available_dates