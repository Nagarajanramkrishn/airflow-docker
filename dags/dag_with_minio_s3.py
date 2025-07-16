from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'nagarajan',
    'start_date': datetime(2025, 7, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(

    dag_id = 'dag_with_minio_s3_v2',
    default_args=default_args,
    description='A DAG that interacts with MinIO and S3',
    schedule='@daily',
    tags=['minio', 'practice'],
) as dag:
    task1 = S3KeySensor(
        task_id='sesnsor_minio_s3',
        bucket_name="airflow",
        bucket_key="data.csv",
        aws_conn_id="minio_s3_conn",
        mode='poke',
        poke_interval=5,
        timeout=30,
        soft_fail=False,
       
    )