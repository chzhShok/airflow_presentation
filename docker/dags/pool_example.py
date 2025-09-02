from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime
from time import sleep

with DAG(
    dag_id="ml_dag",
    schedule="@daily",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["pool"],
) as dag:
    
    @task()
    def extract_api():
        print("extract_api")
        return "api_data"
    
    @task()
    def extract_file():
        print("extract_file")
        return "file_data"
    
    @task_group(group_id="ml_models")
    def ml_models(api_data, file_data):
        
        @task(pool="ml_pool", pool_slots=1)
        def ml_a():
            print("Running ml_a")
            sleep(10)
            return "ml_a_complete"
        
        @task(pool="ml_pool", pool_slots=10)
        def ml_b():
            print("Running ml_b")
            sleep(10)
            return "ml_b_complete"
        
        @task(pool="ml_pool", pool_slots=1)
        def ml_c():
            print("Running ml_c")
            sleep(10)
            return "ml_c_complete"
        
        task_a = ml_a()
        task_b = ml_b()
        task_c = ml_c()
        
        api_data >> task_a
        api_data >> task_b
        api_data >> task_c
        
        file_data >> task_a
        file_data >> task_b
        file_data >> task_c
        
        return [task_a, task_b, task_c]
    
    api_task = extract_api()
    file_task = extract_file()
    
    ml_tasks = ml_models(api_task, file_task)
