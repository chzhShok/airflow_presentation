from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="multiple_dependence",
    tags=['dependence']
) as dag:

    task1 = BashOperator(task_id="task1", bash_command="sleep 10 && echo 'task1'")
    task2 = BashOperator(task_id="task2", bash_command="sleep 10 && echo 'task2'")
    task3 = BashOperator(task_id="task3", bash_command="sleep 10 && echo 'task3'")

    task1 >> [task2, task3]
