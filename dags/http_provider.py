from pprint import pprint

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.sdk.exceptions import AirflowRuntimeError
from pendulum import today


from airflow.hooks.base import BaseHook
from airflow.sdk import DAG
from plugins import api_utility

dag_name = "simple_http_operator"

def my_dag_success_callback(context):
    print("DAG succeeded!")
    # Add custom logic here, e.g., send an email

def my_dag_failure_callback(context):
    print("DAG failed!")
    # Add custom logic here, e.g., send a Slack notification


def add_conn():
    try:
        found = BaseHook().get_connection(f"{dag_name}_connection")
    except Exception as e:
        found = None
        print(
            "The connection is not defined, please add a connection in the dags first task"
        )
    if found:
        print("The connection has been made previously.")
    else:
        request_body = {
            "connection_id": f"{dag_name}_connection",
            "conn_type": HttpHook().conn_type,
            "host": "www.astronomer.io",
            "schema": "https",
            "port": 443
        }
        response = api_utility.create_connection(request_body)
        assert response.json()['connection_id'] == f"{dag_name}_connection"


def print_the_response(response):
    if "Master Subscription Agreement" in response.text:
        pprint(response.text)
        return True
    else:
        pprint(
            "The specified string is not in the response, please check your spelling."
        )
        return False


with DAG(
    dag_id=dag_name,
    start_date=today('UTC').add(days=-2),
    schedule=None,
    on_success_callback=my_dag_success_callback,
    on_failure_callback=my_dag_failure_callback,
    tags=["core"]
) as dag:

    py0 = PythonOperator(task_id="add_connection", python_callable=add_conn)

    h1 = HttpOperator(
        task_id="simple_http_operator",
        method="GET",
        http_conn_id=f"{dag_name}_connection",
        endpoint="legal/msa",
        response_check=print_the_response,
    )

py0 >> h1
