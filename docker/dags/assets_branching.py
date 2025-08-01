from airflow.sdk import asset
import random

@asset(schedule="@daily")
def task1():
    return {
        "name": "Pupkin Zalupkin",
        "score": random.randint(0, 100),
        "update_date": "2025-05-01"
    }

@asset(schedule=task1)
def task2(context):
    task1_data = context["ti"].xcom_pull(
        dag_id="task1",
        task_ids="task1",
        key="return_value",
        include_prior_dates=True,
    )
    print(task1_data)
    score = task1_data["score"]
    print(f"[task2] The score is: {score}")

    return "pass" if score >= 80 else "fail"

@asset(schedule=task2)
def task3(context):
    result = context["ti"].xcom_pull(
        dag_id="task2",
        task_ids="task2",
        key="return_value",
        include_prior_dates=True,
    )
    
    if result == "pass":
        print("✅ Passed!")
    else:
        print("Skipped (task3)")

@asset(schedule=task2)
def task4(context):
    result = context["ti"].xcom_pull(
        dag_id="task2",
        task_ids="task2",
        key="return_value",
        include_prior_dates=True,
    )

    if result == "fail":
        print("❌ Failed.")
    else:
        print("Skipped (task4)")
