from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    'insert_data_to_test_table',
    tags=["sql", "extra"],
) as dag:

    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='postgres_default',
        sql="""
            INSERT INTO test_table (data_text)
            VALUES ('start');
        """,
    )

    insert_data
