from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

TABLES = ["sales", "customers", "products"]

def process_table(table_name: str):
    print(f"Обрабатываю таблицу: {table_name}")
    return f"Данные из {table_name} обработаны"

def final_processing():
    print("Все таблицы успешно обработаны")

with DAG(
    dag_id="dynamic_dag",
    tags=["dynamic_dag"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = PythonOperator(
        task_id="final_processing",
        python_callable=final_processing,
    )

    for table in TABLES:
        task = PythonOperator(
            task_id=f"process_{table}",
            python_callable=process_table,
            op_kwargs={"table_name": table},
        )
        
        start >> task >> end