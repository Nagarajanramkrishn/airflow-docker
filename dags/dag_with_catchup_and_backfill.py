from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator



default_args = {
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 7, 8, 10),
}

with DAG(

    dag_id = "dag_with_catchup_and_backfill_v02",
    default_args=default_args,
    description="A DAG with catchup and backfill enabled",
    schedule="@daily",
    catchup = False,  # Enable catchup 
    tags=["practice"],

) as dag:
    
    task1 = BashOperator(
        task_id = "dag_with_catchup_and_backfill_task1",
        bash_command = "echo This is the simple Bash command task",
    )