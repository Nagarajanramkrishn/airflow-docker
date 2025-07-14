from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator

default_args ={
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 7, 13, 10),
}


def greet(name):
    print(f"Hello world! your name is {name}")



with DAG(

    dag_id ="our_dag_with_python_operator_v2",
    default_args=default_args,
    description="A simple DAG with Python Operator",
    schedule="@daily",
    tags=["practice"],

) as dag:
    
    task1 = PythonOperator(
        task_id = "greet_task",
        python_callable = greet,
        op_kwargs = {"name": "Nagarajan"},  # passing arguments to the function
    )

    task1