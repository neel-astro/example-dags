from airflow.decorators import dag, task
from pendulum import datetime


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Neel", 
        "retries": 0,
        "email": ["airflow@example.com"],
        "email_on_failure": True,
    },
    tags=["example3"],
)
def simple_sleep():
    @task
    def sleep1() -> None:
        """Sleep for 20 seconds."""
        import time
        raise "Exception"
        print("Running short on time, please work!!!!")

        time.sleep(30)

    sleep1()


# Instantiate the DAG
simple_sleep()
