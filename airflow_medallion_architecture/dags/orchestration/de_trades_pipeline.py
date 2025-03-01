
import importlib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import logging
logging.basicConfig(level=logging.INFO)

main_service = importlib.import_module('computation.process_data_module')
dp = main_service.DataProcessing(uri = "postgresql://postgres:postgres@postgres:5432/analytics_db") #initialising Class

default_args = {
    'owner': 'Data Engineering Team @Mercuria',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
#####################
with DAG(
    dag_id='de_trades_pipeline',
    description='Trades data processing pipeline using medallion pattern to store datasets.',
    default_args=default_args,
    schedule_interval= None, 
    catchup=False,
    tags=['trades', 'medallion', 'mercuria'],
) as dag:
    
    porcess_bronze_layer_task = PythonOperator(
        task_id='porcess_bronze_layer_task',
        python_callable=dp.process_bronze_layer
    )

    porcess_silver_layer_task = PythonOperator(
        task_id='porcess_silver_layer_task',
        python_callable=dp.process_silver_layer
    )

    porcess_gold_layer_task = PythonOperator(
        task_id='porcess_gold_layer_task',
        python_callable=dp.process_gold_layer
    )

porcess_bronze_layer_task >> porcess_silver_layer_task >> porcess_gold_layer_task