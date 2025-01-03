from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ndst',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def extract_to_bucket():
    pass

with DAG(
    dag_id='extract_to_bucket',
    default_args=default_args,
    description='Extract data to bucket name "rizzler"',
    start_date=datetime(2024, 11, 28),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='extract_to_bucket',
        python_callable=extract_to_bucket
    )

    task1