import os, sys, time

current_dir = os.getcwd()
target_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(target_dir) 

from assets.credentials import API_KEY, USER, PASSWORD
from assets.model_utils import IntradayStockData
from assets.connection import DBConnector

DB_NAME = 'securities_master'
SCHEMA_NAME = 'nasdaq_100'
TABLE_NAME = 'intraday_historical'

def main():
    ENGINE = DBConnector(USER, PASSWORD, DB_NAME).connect_to_postgres_db()
    stock_data = IntradayStockData(API_KEY, ENGINE, SCHEMA_NAME, TABLE_NAME)

    symbols = ['MSFT', 'AAPL', 'NVDA']

    for symbol in symbols:
        df = stock_data.get_intraday_historical_data(symbol)
        
        if df is not None:
            stock_data.write_df_to_database(insert_strategy="append", index_strategy=False, df=df)
        time.sleep(5)