# dags/test_dag.py
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="test_simple_dag",
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def test_dag():
    
    @task
    def hello_world():
        print("Hello, Airflow!")
        return "Success"
    
    hello_world()

dag_instance = test_dag()