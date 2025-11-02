##### GLOBAL IMPORT
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the computation scripts
extract_script = importlib.import_module('computation.extract_data_main')
mask_script = importlib.import_module('computation.mask_pii_main')
audit_script = importlib.import_module('computation.capture_audit_log_main')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'catchup': False,
}

with DAG(
    'gdpr_sox_compliance_ppl',
    default_args=default_args,
    description='Pipeline for data extraction, GDPR PII masking, and SOX audit logging',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['compliance', 'gdpr', 'sox'],
) as dag:

    # Task 1: Extract data and store in DuckDB
    extract_data_main = PythonOperator(
        dag=dag,
        task_id='extract_data_main',
        provide_context=True,
        python_callable=extract_script.extract_data,
    )

    # Task 2: Mask PII data for GDPR compliance
    mask_pii_main = PythonOperator(
        dag=dag,
        task_id='mask_pii_main',
        provide_context=True,
        python_callable=mask_script.mask_pii_data,
    )

    # Task 3: Capture audit log for SOX compliance
    capture_audit_log_main = PythonOperator(
        dag=dag,
        task_id='capture_audit_log_main',
        provide_context=True,
        python_callable=audit_script.capture_audit_log_data,
    )

    # Define task dependencies
    extract_data_main >> mask_pii_main >> capture_audit_log_main

