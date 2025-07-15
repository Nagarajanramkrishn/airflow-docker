from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="dag_with_postgres_operator_v2",
    default_args=default_args,
    description="A DAG that uses PostgresOperator",
    start_date=datetime(2025, 7, 13, 10),
    schedule='@daily',
    tags=["practice", "postgres_operator"]
) as dag:
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_db",
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs(
        dt date,
        dag_id varchar(100),
        primary key (dt, dag_id)
        );

        """
    ) 

    insert_table = SQLExecuteQueryOperator(
        task_id="insert_table",
        conn_id="postgres_db",
        sql="""

        INSERT INTO dag_runs(dt, dag_id)
        VALUES ( '{{ ds }}', '{{ dag.dag_id }}');

        """
    )
    

    delete_data_table = SQLExecuteQueryOperator(
        task_id="delete_data_table",
        conn_id="postgres_db",
        sql="""

        delete from dag_runs where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}';
        """
    )

    create_table >> delete_data_table >> insert_table