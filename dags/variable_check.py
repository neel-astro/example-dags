from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_healthcheck_variable():
    """
    Retrieves the Airflow variable 'healthcheck'
    and raises an exception if it is not 'ok'.
    """
    value = Variable.get("healthcheck", default_var=None)
    if value is None:
        raise ValueError("❌ Airflow variable 'healthcheck' is not set.")
    elif value.lower() != "ok":
        raise ValueError(f"⚠️ Healthcheck variable value is '{value}', expected 'ok'.")
    else:
        print("✅ Healthcheck variable is set to 'ok'.")

with DAG(
    dag_id="airflow_var_check",
    description="DAG to verify Airflow variable 'healthcheck' is correctly set",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["monitoring", "healthcheck"],
) as dag:

    check_health = PythonOperator(
        task_id="check_healthcheck_variable",
        python_callable=check_healthcheck_variable,
    )
