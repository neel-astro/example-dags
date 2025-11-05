from airflow.sdk import DAG
from airflow.decorators import task

# DAG definition
with DAG(
    dag_id="airflow_var_check",
    description="Check Airflow Variable 'healthcheck' for expected value",
    schedule="@hourly",        # or None for manual
    start_date="2024-01-01",
    catchup=False,
    tags=["monitoring", "healthcheck"],
):

    @task()
    def check_healthcheck_variable():
        from airflow.models import Variable

        # Get variable from Airflow (Key Vault backend if configured)
        value = Variable.get("healthcheck", default_var=None)
        if value is None:
            raise ValueError("❌ Variable 'healthcheck' not found.")
        elif value.lower() != "ok":
            raise ValueError(f"⚠️ Variable 'healthcheck' = '{value}', expected 'ok'.")
        else:
            print("✅ Healthcheck variable is set to 'ok'.")

    check_healthcheck_variable()
