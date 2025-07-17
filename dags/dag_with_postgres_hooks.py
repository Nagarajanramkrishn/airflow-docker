import csv
import logging
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def postgres_to_s3(**kwargs):
    logging.info(f"Running postgres_to_s3 with kwargs: {kwargs}")
    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']

    ds = data_interval_start.strftime('%Y-%m-%d')
    next_ds = data_interval_end.strftime('%Y-%m-%d')

    logging.info(f"Data Interval Start (ds): {ds}")
    logging.info(f"Data Interval End (next_ds): {next_ds}")

    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Use >= and < for the full day range
    query = f"SELECT * FROM orders WHERE date >= '{ds}' AND date < '{next_ds}'"
    logging.info(f"Query: {query}")
    cursor.execute(query)

    rows = cursor.fetchall()
    logging.info(f"Fetched {len(rows)} rows.")
    logging.info("cursor description: %s", cursor.description)
    with open(f"dags/get_orders_{ds}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(rows)
    
    cursor.close()
    conn.close()

    logging.info(f"Data fetched from Postgres and saved to file as get_orders_{ds}.txt.")
    # Uncomment and adjust S3 upload if needed
    # s3_hook = S3Hook(aws_conn_id="minio_s3_conn")
    # s3_hook.load_file(
    #     filename=f"dags/get_orders_{ds}.txt",
    #     bucket_name="airflow",
    #     key=f"orders/{ds}.txt",
    #     replace=True
    # )

with DAG(
    dag_id="dag_with_postgres_hooks_v06",
    default_args=default_args,
    description="A DAG that uses PostgresOperator",
    start_date=pendulum.datetime(2025, 7, 13, tz="UTC"),
    schedule='@daily',
    catchup=True,
    tags=["practice", "postgres_operator"]
) as dag:
    
    task1 = PythonOperator(
        task_id="postgres_to_s3_task",
        python_callable=postgres_to_s3,
    )