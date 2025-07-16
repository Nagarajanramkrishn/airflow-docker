import csv

import logging

from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook



defualts_args ={
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

def postgres_to_s3():

    #step1: query data from postgres save it into text file
    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    conn =pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date < '2024-06-10' ")
    with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    
    cursor.close()
    conn.close()

    logging.info("Data fetched from Postgres and saved to file as get_orders.txt.")



    # sql = "SELECT * FROM your_table_name"
    # records = pg_hook.get_records(sql)


with DAG(

    dag_id="dag_with_postgres_hooks",
    default_args=defualts_args,
    description="A DAG that uses PostgresOperator",
    start_date=datetime(2025, 7, 13, 10),
    schedule='@daily',
    tags=["practice", "postgres_operator"]
) as dag:
    
    task1 = PythonOperator(
        task_id = "postgres_to_s3_task",
        python_callable=postgres_to_s3,
    )

    task1