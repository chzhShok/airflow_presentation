from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    'delete_data_from_test_table',
    tags=["sql", "extra"],
) as dag:

    delete_data = SQLExecuteQueryOperator(
        task_id='delete_data',
        conn_id='postgres_default',
        sql="""
            DELETE FROM test_table
            WHERE data_text = 'start';
        """,
    )

    delete_data
