from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

def succeed():
    print("Task succeeded!")

def fail():
    raise Exception("Task failed intentionally!")


with DAG(
    dag_id='trigger_rules',
    tags=['trigger_rules'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    # Tasks that deliberately succeed or fail
    task_success = PythonOperator(
        task_id='task_success',
        python_callable=succeed
    )

    task_fail = PythonOperator(
        task_id='task_fail',
        python_callable=fail
    )

    # Tasks that runs if all upstream tasks succeed
    run_all_success_start = PythonOperator(
        task_id='run_all_success_start',
        python_callable=succeed,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    run_all_success_fail = PythonOperator(
        task_id='run_all_success_fail',
        python_callable=succeed,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Task that runs if at least one upstream task succeeds
    run_one_success = PythonOperator(
        task_id='run_one_success',
        python_callable=succeed,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Tasks that runs if all upstream tasks fail
    run_all_failed_start = PythonOperator(
        task_id='run_all_failed_start',
        python_callable=succeed,
        trigger_rule=TriggerRule.ALL_FAILED
    )

    run_all_failed_fail = PythonOperator(
        task_id='run_all_failed_fail',
        python_callable=succeed,
        trigger_rule=TriggerRule.ALL_FAILED
    )

    # Task that runs regardless of upstream task state
    run_always = PythonOperator(
        task_id='run_always',
        python_callable=succeed,
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> task_success >> run_all_success_start
    [task_success, task_fail] >> run_all_success_fail
    start >> task_success >> run_one_success
    start >> task_fail >> run_one_success
    start >> task_fail >> run_all_failed_start
    [task_success, task_fail] >> run_all_failed_fail
    [task_success, task_fail] >> run_always
