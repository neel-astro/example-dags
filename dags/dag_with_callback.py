import os

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def dag_success_alert(context):
    os.mkdir("/tmp/callback_dag")
    with open("/tmp/callback_dag/success.txt", "w") as f:
        f.write("DAG has succeeded")


with DAG(
    "callback_dag",
    schedule=None,
    start_date=(pendulum.datetime(2024, 12, 1, tz="UTC")),
    on_success_callback=dag_success_alert,
):
    BashOperator(
        task_id="extract",
        bash_command="touch 'hello world' && date",
        cwd=".",
    )

    BashOperator(
        task_id="transform",
        bash_command="sleep 1",
        cwd=".",
    )

    BashOperator(
        task_id="load",
        bash_command="true",
        cwd=".",
    )
