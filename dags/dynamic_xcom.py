import datetime

from airflow.decorators import dag
from airflow.decorators import task


@dag(start_date=datetime.datetime(2024, 10, 1), schedule=None, catchup=False)
def dynamic_xcom():
    @task
    def task_1():
        print("task_1")

    @task
    def task_2(x):
        print("task_2")
        print(x)
        return x

    @task
    def task_3(y):
        print("task_3")
        print(y)

    t1 = task_1()
    t2 = task_2.expand(x=[0, 1, 2])

    t1 >> t2

    task_3.expand(y=t2)


dynamic_xcom()
