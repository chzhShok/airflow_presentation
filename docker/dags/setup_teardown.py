"""
DAG that uses setup/teardown to prepare a CSV file to write to and then showcases the
behavior in case faulty data is fetched

Teardown task runs after all its setup tasks are done, regardless of whether the setup 
tasks succeed, fail, or are skipped
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
import os
import csv
import time


def get_params_helper(**context):
    folder = context["params"]["folder"]
    filename = context["params"]["filename"]
    cols = context["params"]["cols"]
    return folder, filename, cols


@dag(
    params={
        "folder": "include/my_data",
        "filename": "data.csv",
        "cols": ["id", "name", "age"],
        "fetch_bad_data": Param(False, type="boolean"),
    },
    tags=["setup_teardown"],
)
def setup_teardown():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def report_filepath(**context):
        folder, filename, cols = get_params_helper(**context)
        print(f"Filename: {folder}/{filename}")

    @task
    def create_csv(**context):
        folder, filename, cols = get_params_helper(**context)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(f"{folder}/{filename}", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows([cols])

    @task
    def fetch_data(**context):
        bad_data = context["params"]["fetch_bad_data"]

        if bad_data:
            return [
                [1, "Joe", "Forty"],
                [2, "Tom", 29],
                [3, "Lea", 19],
            ]
        else:
            return [
                [1, "Joe", 40],
                [2, "Tom", 29],
                [3, "Lea", 19],
            ]

    @task
    def write_to_csv(data, **context):
        folder, filename, cols = get_params_helper(**context)

        with open(f"{folder}/{filename}", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        time.sleep(10)

    @task
    def get_average_age(**context):
        folder, filename, cols = get_params_helper(**context)

        with open(f"{folder}/{filename}", "r", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            ages = [int(row[2]) for row in reader]

        return sum(ages) / len(ages)

    @task
    def delete_csv(**context):
        folder, filename, cols = get_params_helper(**context)

        os.remove(f"{folder}/{filename}")

        if not os.listdir(f"{folder}"):
            os.rmdir(f"{folder}")

    start >> report_filepath() >> end

    create_csv_obj = create_csv()
    fetch_data_obj = fetch_data()
    write_to_csv_obj = write_to_csv(fetch_data_obj)
    get_average_age_obj = get_average_age()
    delete_csv_obj = delete_csv()

    chain(
        start,
        create_csv_obj,
        write_to_csv_obj,
        get_average_age_obj,
        delete_csv_obj.as_teardown(
            setups=[create_csv_obj, write_to_csv_obj, fetch_data_obj]
        ),
        end,
    )


setup_teardown()
