from airflow.sdk import asset, Asset


@asset(schedule="@daily", tags=["assets"])
def extracted_data(context):
    return {"a": 1, "b": 2}


@asset(schedule=Asset("extracted_data"), tags=["assets"])
def transformed_data(context, extracted_data):
    ti = context["ti"]
    event = context["inlet_events"][extracted_data][-1]

    data = ti.xcom_pull(
        dag_id=event.source_dag_id,
        task_ids=event.source_task_id,
        key="return_value",
        run_id=event.source_run_id,
    )

    return {k: v * 2 for k, v in data.items()}


@asset(schedule=Asset("transformed_data"), tags=["assets"])
def loaded_data(context, transformed_data):
    ti = context["ti"]
    event = context["inlet_events"][transformed_data][-1]

    data = ti.xcom_pull(
        dag_id=event.source_dag_id,
        task_ids=event.source_task_id,
        key="return_value",
        run_id=event.source_run_id,
    )

    print(f"Summed data: {sum(data.values())}")
