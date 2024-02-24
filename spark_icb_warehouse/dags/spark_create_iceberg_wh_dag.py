import importlib
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

### GENERAL PARAMS ###
script = importlib.import_module('assets.utils')
step_name = "create_iceberg_wh_via_emr_airflow"
step_name_sensor = f'{step_name}_sensor'

af_variable_name = None
path_to_params = "/assets/dag_params.json"

emr_conn = 'aws_emr_conn'

# ---------- DAG Definition ------------------------------------------------------------------------------------- #
default_args = {
    "owner": "Data Engineering",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG('spark_create_iceberg_wh',
         schedule_interval= None, # Executed On Demand
         catchup=False,
         max_active_runs=1,
         default_args=default_args,
         render_template_as_native_obj=True,
         tags=['s3', 'emr', 'pyspark']) as dag:

    upload_scripts_to_s3 = PythonOperator(
        dag=dag,
        task_id="upload_scripts_to_s3",
        python_callable=script.local_to_s3,
        op_kwargs = {"path_to_params": path_to_params}
    )

    generate_step = PythonOperator(
        dag=dag,
        task_id='generate_step',
        python_callable= script.generate_step,
        provide_context=True,
        op_kwargs = {"aws_conn_id": emr_conn,
                     "step_name": step_name,
                     "path_to_params": path_to_params}
    )

    execute_pyspark_script = EmrAddStepsOperator(
        dag=dag,
        task_id=step_name,
        job_flow_id="{{ ti.xcom_pull(task_ids='generate_step', key='return_value')[0] }}",
        aws_conn_id=emr_conn,
        steps=["{{ ti.xcom_pull(task_ids='generate_step', key='return_value')[1] }}"]
    )

    execute_pyspark_script_sensor = EmrStepSensor(
        dag=dag,
        task_id=step_name_sensor,
        job_flow_id="{{ ti.xcom_pull(task_ids='create_iceberg_wh_via_emr_airflow', key='return_value')[0] }}",
        aws_conn_id=emr_conn,
        step_id="{{ ti.xcom_pull(task_ids='create_iceberg_wh_via_emr_airflow', key='return_value')[0] }}",
        sla=timedelta(minutes=5)
    )


    upload_scripts_to_s3 >> generate_step >> execute_pyspark_script >> execute_pyspark_script_sensor