from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_to_xcoms(**context):
    context['ti'].xcom_push(key='my_key', value='manual_value')

with DAG(
    dag_id='push_to_xcoms_example',
    catchup=False,
    tags=['xcoms']
) as dag:

    push_task = PythonOperator(
        task_id='push_to_xcoms',
        python_callable=push_to_xcoms
    )
