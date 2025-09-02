from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, chain, task

file_path = "/tmp/test"

trigger = FileDeleteTrigger(filepath=file_path)

asset = Asset("example_asset", watchers=[AssetWatcher(name="test_asset_watcher", trigger=trigger)])

with DAG(
    dag_id="example_asset_with_watchers",
    schedule=[asset],
    tags=["assets", "watcher"]
):
    @task
    def test_task():
        print("Hello world")


    chain(test_task())
