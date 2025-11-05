from airflow.sdk import DAG
from airflow.decorators import task
from airflow.utils import timezone

# Define DAG
with DAG(
    dag_id="airflow_var_check",
    description="Check Airflow Variable 'healthcheck' for expected value",
    schedule="@hourly",
    start_date=timezone.datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "healthcheck"],
):

    @task()
    def check_healthcheck_variable():
        from airflow.models import Variable

        value = Variable.get("healthcheck", default_var=None)
        if value is None:
            raise ValueError("❌ Variable 'healthcheck' not found.")
        elif value.lower() != "ok":
            raise ValueError(f"⚠️ Variable 'healthcheck' = '{value}', expected 'ok'.")
        else:
            print("✅ Healthcheck variable is set to 'ok'.")

    check_healthcheck_variable()
