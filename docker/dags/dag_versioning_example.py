from airflow.decorators import dag, task
from datetime import datetime

@dag(
    tags=["versioning"],
)
def dag_versioning_example():
    
    @task
    def task1():
        print("task1 completed")
    
    @task
    def task2():
        print("task2 completed")

    @task
    def task3():
        print("task3 completed")

    @task
    def task4():
        print("task4 completed")
    
    task1 = task1()
    task2 = task2()
    task3 = task3()
    task4 = task4()
    
    task1 >> task2
    task2 >> [task3, task4]

dag_versioning_example()
