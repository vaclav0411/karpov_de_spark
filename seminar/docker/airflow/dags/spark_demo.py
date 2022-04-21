from datetime import datetime
from datetime import timedelta

from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreateSparkJobOperator, \
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocCreatePysparkJobOperator

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

CONNECTION_ID = 'yandexcloud_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
SERVICE_ACCOUNT_ID = 'ajefvdh2c1gfmab33ioc'
DEFAULT_S3_ENDPOINT = 'storage.yandexcloud.net'
S3_ACCESS_KEY = 'YCAJEMKM9i1VAXfDXAg9W22RN'
S3_SECRET_ACCESEE_KEY = 'YCPYMiLTHmNIqY3NK6f7qOoklNPv_AUXj3ARD7nz'

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
}

S3_BUCKET_NAME_FOR_JOB_LOGS = 'seminar-data'

S3_CONNECTION_ID = 'yandexcloud_default'
DAG_ID = 'spark_demo'

JOB_NAME = 'Test job'
JOB_SCRIPT_LOCATION = 's3a://seminar-data/main.py'

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='Demo spark DAG',
        schedule_interval='5 * * * *',
        start_date=datetime(2021, 4, 1, ),
        tags=['demo', 'spark'],
        catchup=False,
        max_active_runs=1,
) as loader_dag:
    # create_cluster = DataprocCreateClusterOperator(
    #     task_id='create',
    #     zone=AVAILABILITY_ZONE_ID,
    #     connection_id=CONNECTION_ID,
    #     service_account_id=SERVICE_ACCOUNT_ID,
    #     datanode_count=1,
    #     computenode_count=1,
    #     cluster_image_version='2.0',
    #     masternode_disk_size=30,
    #     datanode_disk_size=32,
    #     computenode_disk_size=33,
    #     datanode_resource_preset='s2.small',
    #     computenode_resource_preset='s2.small',
    #     cluster_name='spark_cluster',
    #     s3_bucket=S3_BUCKET_NAME_FOR_JOB_LOGS,
    #     subnet_id='e2l9lreuc0tfivimeabt',
    #     folder_id='b1gjpjgtcptvb1bsdbli',
    #
    # )

    job_properties = {
        'spark.submit.deployMode': 'cluster',
        'spark.yarn.maxAppAttempts': '1',
        'spark.executor.heartbeatInterval': '600000',
        'spark.network.timeout': '600000',
        'spark.hadoop.fs.s3a.access.key': S3_ACCESS_KEY,
        'spark.hadoop.fs.s3a.secret.key': S3_SECRET_ACCESEE_KEY,
        'spark.hadoop.fs.s3a.endpoint': DEFAULT_S3_ENDPOINT,
        'spark.dynamicAllocation.enabled': 'false',
        'spark.executor.memory': '2g',
        'spark.executor.instances': '2',
        'spark.driver.memory': '2g',
    }

    spark_job = DataprocCreatePysparkJobOperator(
        cluster_id='c9q7ia47l5epogb1m8dn',
        task_id='run_pyspark_job',
        main_python_file_uri=JOB_SCRIPT_LOCATION,
        properties=job_properties,
        args=['s3a://karpov-data/2020/yellow_tripdata_2020-01.csv', 's3a://karpov-data/output'],
        name='test spark job',
    )


    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id='delete',
    #     trigger_rule=TriggerRule.NONE_SKIPPED,
    #     retries=3,
    #     retry_delay=timedelta(seconds=30)
    # )
    #
    # create_cluster >> spark_job >> delete_cluster
