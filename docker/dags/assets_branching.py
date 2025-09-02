from airflow.sdk import asset, Asset
import random


@asset(schedule="@daily", tags=["assets", "branching"])
def task1(context):
    return {
        "name": "Dexter Morgan",
        "score": random.randint(0, 100),
        "update_date": "2025-05-01",
    }


@asset(schedule=Asset("task1"), tags=["assets", "branching"])
def task2(context, task1):
    ti = context["ti"]
    event = context["inlet_events"][task1][-1]

    data = ti.xcom_pull(
        dag_id=event.source_dag_id,
        task_ids=event.source_task_id,
        key="return_value",
        run_id=event.source_run_id,
    )

    score = data["score"]
    print(f"[task2] The score is: {score}")

    return "pass" if score >= 80 else "fail"


@asset(schedule=Asset("task2"), tags=["assets", "branching"])
def task3(context, task2):
    ti = context["ti"]
    event = context["inlet_events"][task2][-1]

    result = ti.xcom_pull(
        dag_id=event.source_dag_id,
        task_ids=event.source_task_id,
        key="return_value",
        run_id=event.source_run_id,
    )

    if result == "pass":
        print("✅ Passed!")
    else:
        print("Skipped (task3)")


@asset(schedule=Asset("task2"), tags=["assets", "branching"])
def task4(context, task2):
    ti = context["ti"]
    event = context["inlet_events"][task2][-1]

    result = ti.xcom_pull(
        dag_id=event.source_dag_id,
        task_ids=event.source_task_id,
        key="return_value",
        run_id=event.source_run_id,
    )

    if result == "fail":
        print("❌ Failed.")
    else:
        print("Skipped (task4)")
