from __future__ import annotations

import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="latest_only",
    schedule="@daily",
    start_date=datetime.datetime(2025, 7, 1),
    tags=["latest_only"],
    catchup=True,
) as dag:
    latest_only = LatestOnlyOperator(task_id="latest_only", depends_on_past=True)
    task1 = EmptyOperator(task_id="task1")

    latest_only >> task1
