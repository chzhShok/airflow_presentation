from airflow.decorators import dag, task
from datetime import datetime

@dag(
    tags=["versioning"],
)
def dag_versioning_example():
    
    @task
    def task_a():
        print("task_a completed")

    @task
    def task_b():
        print("task_b completed")
    
    task_a = task_a()
    task_b = task_b()
    
    task_a >> task_b

dag_versioning_example()
