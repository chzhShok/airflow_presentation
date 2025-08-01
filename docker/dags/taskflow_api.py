from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2025, 7, 1), tags=["presentation"])
def etl_pipeline():
    
    @task
    def extract():
        return {"data": [1, 2, 3]}
    
    @task
    def transform(data: dict):
        return [x * 2 for x in data["data"]]
    
    @task
    def load(transformed_data: list):
        print(f"Загружено: {transformed_data}")
    
    load(transform(extract()))

etl_pipeline()
