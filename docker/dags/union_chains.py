from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="union_chains",
    tags=['dependence']
) as dag:

    task1 = BashOperator(task_id="task1", bash_command="sleep 10 && echo 'task1'")
    task2 = BashOperator(task_id="task2", bash_command="sleep 10 && echo 'task2'")
    task3 = BashOperator(task_id="task3", bash_command="sleep 10 && echo 'task3'")
    task4 = BashOperator(task_id="task4", bash_command="sleep 10 && echo 'task4'")

    (task1 >> task2) >> (task3 >> task4)
