from datetime import datetime
from airflow import DAG
from sql_deferrable import SQLCheckOperator


# Classes SQLCheckOperator and SQLCheckTrigger are defined in `plugins/sql_deferrable.py`.
# To insert the required record into `test_table`, use the DAG `insert_data_to_test_table`.
# To delete this record, use the DAG `delete_data_from_test_table`.

with DAG(
    dag_id="deferrable_sql_check_dag",
    tags=["deferrable"],
) as dag:

    wait_for_sql = SQLCheckOperator(
        task_id="wait_for_start_record",
        conn_id="postgres_default",
        sql="SELECT COUNT(*) FROM test_table WHERE data_text = 'start'",
        poll_interval=15,
    )
