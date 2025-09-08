from airflow.decorators import dag
from pendulum import datetime
from airflow.sensors.date_time import DateTimeSensorAsync

def my_dag_success_callback(context):
    print("DAG succeeded!")
    # Add custom logic here, e.g., send an email

def my_dag_failure_callback(context):
    print("DAG failed!")
    # Add custom logic here, e.g., send a Slack notification

@dag(
    start_date=datetime(2024, 5, 23, 20, 0),
    schedule="0 * * * *",
    catchup=False,
    on_success_callback=my_dag_success_callback,
    on_failure_callback=my_dag_failure_callback,
)
def async_dag_1():
    DateTimeSensorAsync(
        task_id="async_task",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(seconds=10) }}""",
    )


async_dag_1()
