from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime
import random

def choose_branch(**context):
    data_type = random.choice(["csv", "json"])
    if data_type == "csv":
        return "process_csv"
    return "process_json"

def process_csv():
    print("Обработка CSV данных")

def process_json():
    print("Обработка JSON данных")

def final_report():
    print("Формирование итогового отчета")

with DAG(
    dag_id="easy_branching",
    tags=['presentation']
) as dag:
    
    branch_task = BranchPythonOperator(
        task_id="choose_format",
        python_callable=choose_branch
    )
    
    process_csv_task = PythonOperator(
        task_id="process_csv",
        python_callable=process_csv
    )
    
    process_json_task = PythonOperator(
        task_id="process_json",
        python_callable=process_json
    )
    
    final_task = PythonOperator(
        task_id="final_report",
        python_callable=final_report,
        trigger_rule="none_failed"
    )

    branch_task >> [process_csv_task, process_json_task] >> final_task
