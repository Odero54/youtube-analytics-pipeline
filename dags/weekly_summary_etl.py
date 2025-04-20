from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract(): pass
def transform(): pass
def load(): pass

with DAG("weekly_youtube_etl", start_date=datetime(2023, 1, 1), schedule_interval="@weekly", catchup=False) as dag:
    t1 = PythonOperator(task_id="extract_data", python_callable=extract)
    t2 = PythonOperator(task_id="transform_data", python_callable=transform)
    t3 = PythonOperator(task_id="load_data", python_callable=load)

    t1 >> t2 >> t3
