from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from google.cloud import storage

default_args = {
    'owner': 'ndst',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def load_to_db():
    pass

with DAG(
    dag_id='load_to_db',
    default_args=default_args,
    description='Load preprocessed data to PsdtgreSQL database',
    start_date=datetime(2024, 11, 28),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
    )