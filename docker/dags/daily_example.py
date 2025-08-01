from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="daily_example",
    schedule="@daily",
    start_date=datetime(2025, 7, 1),
    catchup=True,
    tags=["presentation"],
)
def daily_example_dag():
    @task
    def process_daily_data(ds=None):
        print(f"Обрабатываю данные за {ds}")
        return f"Данные за {ds} обработаны"
    
    @task
    def send_report(ds=None):
        print(f"Отчет за {ds} отправлен!")
        return f"Отчет за {ds} отправлен"
    
    processed_data = process_daily_data()
    send_report() << processed_data

daily_example_dag()
