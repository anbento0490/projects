import os
import json
import itertools
import logging
from datetime import datetime, timedelta
###
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
###
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

logging.basicConfig(level=logging.INFO)

# ---------- Variables ------------------------------------------------------------------------------------- #
parent_folder_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) 
default_json = parent_folder_path + '/assets/dag_params.json'

try:
    dag_params = Variable.get("dag_params", deserialize_json=True)
except Exception as excpt:
    dag_params = json.loads(open(default_json, 'r').read())

# ---------------------------------------------------------------------------------------------------------- #

#### Local Conf ######
local_sub_folder = dag_params['local_conf']['local_sub_folder']
spark_script = dag_params['local_conf']['spark_script']
local_script = parent_folder_path + local_sub_folder + spark_script

#### S3 Conf ######
BUCKET_NAME = dag_params['s3_conf']['bucket_name']
s3_script = dag_params['s3_conf']['s3_script']
s3_input = dag_params["s3_conf"]["s3_input"].format(bucket_name=BUCKET_NAME)
s3_output = dag_params['s3_conf']['s3_output'].format(bucket_name=BUCKET_NAME)

#### SPARK Conf ###### / #["spark-submit", "--deploy-mode", "cluster", "--master", "yarn"]
spark_submit_cmd =  json.loads(dag_params['spark_submit_cmd']['cmd'])
spark_conf_map = dag_params['spark_conf']

#### JARS Conf ######
BUCKET_PREFIX = dag_params["spark_jars_conf"]["bucket_prefix"].format(bucket_name=BUCKET_NAME)
BUCKET_SUBFOLDER = dag_params["spark_jars_conf"]["bucket_subfolder"]
_JARS = dag_params["spark_jars_conf_value"]

jars_string = ",".join([f"{BUCKET_PREFIX}{BUCKET_SUBFOLDER}{jar_name}" for jar_name in _JARS])

# ---------- Auxiliary Functions ----------------------------------------------------------------------------------------- #
def fetch_cluster_id():
    emr_hook = EmrHook(aws_conn_id="aws_default")
    cluster_id = emr_hook.get_cluster_id_by_name(emr_cluster_name="finance-emr",cluster_states=["WAITING", "RUNNING"])
    ####
    logging.info('Fetched CLUSTER_ID is: %s', cluster_id)
    logging.info(f'Fetched LOCAL CONFIG is:\n %s\n %s \n %s \n %s \n', parent_folder_path, local_sub_folder, spark_script, local_script)
    logging.info(f'Fetched S3 CONFIG is:\n %s\n %s \n %s \n %s \n', BUCKET_NAME, s3_script, s3_input, s3_output)
    logging.info(f'Fetched SPARK S3 CONFIG is:\n %s\n %s \n', spark_submit_cmd, spark_conf_map)
    logging.info(f'JAR STRING is:\n %s \n', jars_string)

    return cluster_id

def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook('aws_default')
    s3.load_file(filename=filename, bucket_name=BUCKET_NAME, replace=True, key=key)

def generate_spark_submit_command(spark_submit_cmd, spark_conf_map):
    if len(spark_conf_map) > 0:
        _conf = [f"{name}={value}" for name, value in spark_conf_map.items()]
        _confs_prefix = list("--conf" for i in range(len(_conf)))
        _confs = list(itertools.chain.from_iterable(zip(_confs_prefix, _conf)))

        spark_submit_cmd = spark_submit_cmd + _confs
    else:
        spark_submit_cmd
    return spark_submit_cmd

# ---------- Pipeline Tasks ----------------------------------------------------------------------------------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2023, 8, 1)
}

with DAG('submit_pyspark_script_to_emr',
         schedule_interval= None,  
         catchup=False, 
         max_active_runs=1, 
         default_args=default_args,
         tags=['s3', 'emr', 'spark']) as dag:

    ############
    fetch_cluster_id = PythonOperator(
        dag=dag,
        task_id="fetch_cluster_id",
        python_callable=fetch_cluster_id,
    )

    upload_script_to_s3 = PythonOperator(
        dag=dag,
        task_id="upload_script_to_s3",
        python_callable=_local_to_s3,
        op_kwargs={"filename": local_script, "key": s3_script}
    )
    execute_pyspark_script = EmrAddStepsOperator(
        dag=dag,
        task_id='execute_pyspark_script',
        job_flow_id= "{{ ti.xcom_pull(task_ids='fetch_cluster_id', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'execute_pyspark_script',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    *generate_spark_submit_command(spark_submit_cmd, spark_conf_map),
                    "--jars",
                    '{{ params.jars_string }}',
                    's3://{{ params.bucket_name }}/{{ params.s3_script }}',
                    '--s3_output',
                    '{{ params.s3_output }}',
                    '--s3_input',
                    '{{ params.s3_input }}'
                ]
            }
        }],
        params={
            "bucket_name": BUCKET_NAME,
            "s3_script": s3_script,
            "s3_input": s3_input,
            "s3_output": s3_output,
            "jars_string": jars_string
        }
    )

    execute_pyspark_script_sensor = EmrStepSensor(
        dag=dag,
        task_id="execute_pyspark_script_sensor",
        job_flow_id="{{ ti.xcom_pull(task_ids='fetch_cluster_id', key='return_value') }}",
        aws_conn_id="aws_default",
        step_id="{{ ti.xcom_pull(task_ids='execute_pyspark_script', key='return_value')[0] }}",
        sla=timedelta(minutes=5)
    )

    fetch_cluster_id >> upload_script_to_s3 >> execute_pyspark_script >> execute_pyspark_script_sensor