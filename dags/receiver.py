from airflow.decorators import task, dag
from pendulum import datetime



@dag(
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False
)

def receiver():
    @task
    def check_process():
        print("Checked and ok")
    check_process()

receiver()