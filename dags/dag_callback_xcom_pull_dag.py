import datetime
import os

from airflow.decorators import dag, task

CALLBACK_DIR = "/tmp/dag_callback_xcom_pull"
CALLBACK_FILE = f"{CALLBACK_DIR}/mapped_xcom.txt"


def pull_mapped_xcom_callback(context):
    # xcom_pull of a mapped task (no map_indexes) issues XCom.get_all ->
    # GetXComSequenceSlice; the file is only written if that read succeeds.
    values = context["ti"].xcom_pull(task_ids="produce")
    os.makedirs(CALLBACK_DIR, exist_ok=True)
    with open(CALLBACK_FILE, "w") as f:
        f.write(
            f"pulled mapped xcom in dag processor: {sorted(v for v in values if v is not None)}"
        )


@dag(
    dag_id="dag_callback_xcom_pull_dag",
    schedule=None,
    start_date=datetime.datetime(2024, 12, 1),
    catchup=False,
    on_failure_callback=pull_mapped_xcom_callback,
)
def dag_callback_xcom_pull_dag():
    @task
    def produce(x):
        return x

    @task
    def fail_the_dag():
        raise RuntimeError(
            "intentional failure to trigger the DAG-level on_failure_callback"
        )

    # produce runs first and pushes an XCom per map index (0, 1, 2); fail_the_dag
    # then fails, so the mapped XComs exist when the callback pulls them.
    produce.expand(x=[0, 1, 2]) >> fail_the_dag()


dag_callback_xcom_pull_dag()
