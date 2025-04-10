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
    def sleep1() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
    
    @task
    def sleep2() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
        
    @task
    def sleep3() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
        
    @task
    def sleep4() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
    
    @task
    def sleep5() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
        
    @task
    def sleep6() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
    
    @task
    def sleep7() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
        
    @task
    def sleep8() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
        
    @task
    def sleep9() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)
    
    @task
    def sleep10() -> None:
        """Sleep for 20 seconds."""
        import time

        time.sleep(20)

    sleep1()
    sleep2()
    sleep3()
    sleep4()
    sleep5()
    sleep6()
    sleep7()
    sleep8()
    sleep9()
    sleep10()


# Instantiate the DAG
simple_sleep()
