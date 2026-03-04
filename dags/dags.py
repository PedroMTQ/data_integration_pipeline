import os

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain
from dotenv import dotenv_values

# plugin is reachable like this, a bit strange but it is what it is
from email_callback import send_email_callback

IMAGE_NAME = 'data-integration-service'
APP_FOLDER = '/opt/airflow/app/'
ENV_FILE = os.path.join(APP_FOLDER, '.env')
ENV_CONFIG = dotenv_values(ENV_FILE)
# this makes setup easier, but you should use Variables as defined in the readme
S3_CONNECTION = 'http://data-integration-minio:9000'
BUCKET_NAME = ENV_CONFIG['DATA_BUCKET']
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '60'))

DOCKER_BASE_CONFIG = {
    'image': IMAGE_NAME,
    'private_environment': ENV_CONFIG,
    'api_version': '1.51',
    'network_mode': 'data-integration-network',
    'auto_remove': 'force',
    'mount_tmp_dir': False,
    # for docker in docker (tecnativa/docker-socket-proxy:v0.4.1) -> https://github.com/benjcabalona1029/DockerOperator-Airflow-Container/tree/master
    'docker_url': 'tcp://airflow-docker-socket:2375',
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=1),
}


def create_s3_sensor(task_suffix, file_pattern):
    """
    Factory function to generate S3KeySensors based on a template.
    """
    return S3KeySensor(
        task_id=f'sensor_{task_suffix}',
        # aws_conn_id=Variable.get('S3_CONNECTION', default='s3_connection'),
        aws_conn_id=S3_CONNECTION,
        bucket_name=BUCKET_NAME,
        bucket_key=file_pattern,
        timeout=SENSOR_TIMEOUT,
        soft_fail=True,
        use_regex=True,
        poke_interval=10,
        exponential_backoff=True,
    )


@dag(
    schedule='0 * * * *',
    default_args={'on_failure_callback': send_email_callback},
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    tags=['upload_bronze'],
    max_active_tasks=ENV_CONFIG.get('max_active_tasks', 5),
    doc_md="""
    Processes bronze data to silver and audits the data:
    1. Processes bronze data and uploads to silver delta table (can be parallel)
    2. Audits silver delta talbe (can be parallel)
    """,
)
def process_bronze_dag():
    sensor = create_s3_sensor(task_suffix='check_bronze', file_pattern=r'^bronze\/[a-zA-Z0-0_]+\/.*?\.(parquet|csv|json)$')

    @task.docker(**DOCKER_BASE_CONFIG, task_id='get_list_bronze_files')
    def get_list_bronze_files():
        from data_integration_pipeline.jobs.process_bronze_and_load_to_delta_silver import get_tasks

        return get_tasks()

    @task.docker(**DOCKER_BASE_CONFIG, task_id='process_bronze')
    def process_bronze(task_data: dict):
        from data_integration_pipeline.jobs.process_bronze_and_load_to_delta_silver import process_task

        print(f'Task data: {task_data}')
        process_task(task_data)

    @task.docker(**DOCKER_BASE_CONFIG, task_id='audit_silver')
    def audit_silver(s3_path: str):
        from data_integration_pipeline.jobs.audit_silver import process_task

        print(f'Task data: {s3_path}')
        process_task(s3_path)

    task_data_list = get_list_bronze_files()
    silver_s3_paths = process_bronze.expand(task_data=task_data_list)
    audit_tasks = audit_silver.expand(s3_path=silver_s3_paths)
    chain(sensor, task_data_list, audit_tasks)


process_bronze_dag()
