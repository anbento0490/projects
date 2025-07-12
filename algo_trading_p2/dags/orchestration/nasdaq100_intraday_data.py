##### GLOBAL IMPORT
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

script = importlib.import_module('computation.nasdaq100_main')

default_args = {
    'owner': 'Quant Engineering Team',
    'retries': 0,
    'catchup': False,
    'retry_delay': timedelta(minutes=0),
    'start_date': datetime(2025, 5, 1)
}

with DAG('nasdaq100_intraday_historical',
         description='Fetches NASDAQ 100 stocks intraday days [15 mins candles] via AlphaVantage API',
         schedule_interval='0 5 * * *',  # once a day at 5AM
         catchup=False,
         max_active_runs=1,
         default_args=default_args,
         tags=['intraday', 'nasdaq100', 'postgresDB']) as dag:

    fetch_historical_data = PythonOperator(dag=dag,
                                           task_id='fetch_historical_data',
                                           provide_context=True,
                                           python_callable=script.main)