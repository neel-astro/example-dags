from airflow.decorators import dag, task
from pendulum import datetime


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Neel", "retries": 3},
    tags=["example"],
)
def simple_sleep():
    @task
    def sleep() -> None:
        """Sleep for 5 seconds."""
        import time

        time.sleep(20)

    sleep()


# Instantiate the DAG
simple_sleep()
