from airflow.sdk import DAG

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "nagarajan",
    "retries": 3,
    "retry_delay" : timedelta(minutes =2)

}


with DAG(
    dag_id="first_dag_pipeline_v4",
    default_args = default_args,
    description="A simple first DAG pipeline",
    start_date = datetime(2025, 7, 13, 10),
    schedule = '@daily',
    tags = ["practice"]

    ) as dag:
    
    task1 = BashOperator(
        task_id = "first_task",
        bash_command = "echo 'this is the first task of the DAG'",
            
   )
    

    task2 = BashOperator(
        task_id = "second_task",
        bash_command = "echo 'this is the second task of the DAG after the first task'",
    )
    

    task3 = BashOperator(
        task_id = "third_task",
        bash_command = "echo 'this is the third task of the DAG after the first task and same time as the second task'",
    )

    # task depeencies method 1
    task1.set_downstream(task2)
    task1.set_downstream(task3)


    # task dependencies method 2
    # task1 >> task2
    # task1 >> task3


    # task dependencies method 3
    task1 >> [task2, task3]


