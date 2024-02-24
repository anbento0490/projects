import os
import sys
import json
import boto3
import itertools
import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.emr import EmrHook

current_directory = os.path.dirname(os.path.realpath(__file__))  # spark_icb_warehouse/assets
target_directory = os.path.abspath(os.path.join(current_directory, os.pardir))  # spark_icb_warehouse/
sys.path.append(target_directory)  # appending target directory to be importable as a module

logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------------------------------------------ #

def local_to_s3(path_to_params=None):

    logging.info('Running function to UPLOAD required scripts to S3 Bucket...\n')
    logging.info('Fetching PARAMETERS...')

    default_json = target_directory + path_to_params
    dag_params = json.loads(open(default_json, 'r').read())
    logging.info('Required PARAMS were fetched from the JSON file saved at this location: %s\n', path_to_params)

    local_sub_folder = dag_params['local_conf']['local_sub_folder']
    files_to_upload_lst = dag_params['local_conf']['files_to_upload']
    BUCKET_NAME = dag_params['s3_conf']['bucket_name']
    s3_scripts_path = dag_params['s3_conf']['s3_scripts_path']

    logging.info('The passed files to upload to S3 bucket are: %s', files_to_upload_lst)
    logging.info('The passed BUCKET_NAME is: %s', BUCKET_NAME)
    logging.info('The passed s3_scripts_path (destination in S3) is: %s\n\n', s3_scripts_path)

    # Upload each file to the S3 bucket
    for file_name in files_to_upload_lst:
        local_path = target_directory + local_sub_folder + file_name

        s3_key = f'{s3_scripts_path}{file_name}'  # You can modify this key as needed in the S3 bucket

        logging.info('Uploading file with LOCAL PATH: %s to S3 PATH: %s\n', local_path, s3_key)

        try:
            boto3.setup_default_session(aws_access_key_id='XXXXX',
                                        aws_secret_access_key='XXXXX'
                                        )
            client = boto3.client('s3')
            client.upload_file(local_path, BUCKET_NAME, s3_key)
            logging.info(f'File SUCCESSFULLY uploaded to S3 bucket!\n\n')
        except Exception as e:
            logging.info(f'Error uploading {file_name}: {e}')
            break

    logging.info('All required files were SUCCESSFULLY uploaded to the specified S3 location.')
# ------------------------------------------------------------------------------------------ #
    
def generate_spark_submit_command(spark_submit_cmd, spark_conf_map) -> list:

    logging.info('Generating SPARK SUBMIT COMMAND as part of the STEP...')

    if len(spark_conf_map) > 0:
        if 'spark_pyfile' in spark_conf_map.keys():
            _conf = [f"{name}={value}" for name, value in spark_conf_map.items() if name != 'spark_pyfile']
            _confs_prefix = list("--conf" for i in range(len(_conf)))
            _confs = list(itertools.chain.from_iterable(zip(_confs_prefix, _conf)))

            _pyfiles = [f"{value}" for name, value in spark_conf_map.items() if name == 'spark_pyfile']
            _pyfiles_prefix = list("--py-files" for i in range(len(_pyfiles)))
            _pyfiles = list(itertools.chain.from_iterable(zip(_pyfiles_prefix, _pyfiles)))

            spark_submit_cmd = spark_submit_cmd + _confs + _pyfiles
        else:
            _conf = [f"{name}={value}" for name, value in spark_conf_map.items()]
            _confs_prefix = list("--conf" for i in range(len(_conf)))
            _confs = list(itertools.chain.from_iterable(zip(_confs_prefix, _conf)))

            spark_submit_cmd = spark_submit_cmd + _confs
    else:
        spark_submit_cmd
    logging.info('SPARK_SUBMIT_COMMNAD was generated!')

    return spark_submit_cmd
# ------------------------------------------------------------------------------------------ #

def generate_step(aws_conn_id, step_name, path_to_params) -> tuple:

    logging.info(' Generating EMR STEP to execute the specified PYSPARK script...')

    logging.info('The optional path_to_params is set to %s', path_to_params)

    #### CLUSTER ID ######
    emr_hook = EmrHook(aws_conn_id=aws_conn_id)
    cluster_id = emr_hook.get_cluster_id_by_name(emr_cluster_name="iceberg-wh-emr", cluster_states=["WAITING", "RUNNING"])

    #### FETCHING PARAMS ######
    logging.info('Fetching PARAMS...')

    path_to_dag_params = path_to_params
    default_json = target_directory + path_to_dag_params
    dag_params = json.loads(open(default_json, 'r').read())
    logging.info('Required PARAMS were fetched from the JSON file saved at this location: %s\n', path_to_params)

    #### S3 Conf ######
    BUCKET_NAME = dag_params['s3_conf']['bucket_name']

    #### SPARK Conf ######
    spark_submit_cmd = json.loads(dag_params['spark_submit_cmd']['cmd'])
    exec_script = dag_params['spark_submit_cmd']['pyspark_exec']
    spark_conf_map = dag_params['spark_conf']

    #### JARS Conf ######
    BUCKET_PREFIX = dag_params["spark_jars_conf"]["bucket_prefix"].format(bucket_name=BUCKET_NAME)
    BUCKET_SUBFOLDER = dag_params["spark_jars_conf"]["bucket_subfolder"]
    _JARS = dag_params["spark_jars_conf_value"]

    jars_string = ",".join([f"{BUCKET_PREFIX}{BUCKET_SUBFOLDER}{jar_name}" for jar_name in _JARS])

    #### OTHER PARAMS ######
    try:
        other_paramns = json.loads(dag_params['other_paramns'])

        args_list = [
            *generate_spark_submit_command(spark_submit_cmd, spark_conf_map),
            '--jars',
            jars_string,
            f's3://{BUCKET_NAME}/{exec_script}',
        ]

        for param in other_paramns:
            args_list.append(param)

        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': args_list }}

        ##########################
        ####  GENERATING STEP ######
        logging.info('Fetched CLUSTER_ID is: %s', cluster_id)
        logging.info(f'Fetched S3 CONFIG is:\n %s\n %s\n', BUCKET_NAME)
        logging.info(f'Fetched SPARK S3 CONFIG is:\n %s\n %s \n', spark_submit_cmd, spark_conf_map, exec_script)
        logging.info(f'JAR STRING is:\n %s \n', jars_string)
        logging.info(f'OTHER PARAMS are:\n %s \n', other_paramns)

        logging.info('STEP was successfully generated! \n DETAIL: %s', step)

    except:
        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    *generate_spark_submit_command(spark_submit_cmd, spark_conf_map),
                    '--jars',
                    jars_string,
                    f's3://{BUCKET_NAME}/{exec_script}'
                ]}}

        logging.info('Fetched CLUSTER_ID is: %s', cluster_id)
        logging.info(f'Fetched S3 CONFIG is:\n %s\n %s\n', BUCKET_NAME)
        logging.info(f'Fetched SPARK S3 CONFIG is:\n %s\n %s \n', spark_submit_cmd, spark_conf_map, exec_script)
        logging.info(f'JAR STRING is:\n %s \n', jars_string)

        logging.info('STEP was successfully generated!')

    return cluster_id, step