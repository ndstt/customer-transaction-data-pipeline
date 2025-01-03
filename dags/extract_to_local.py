from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ndst',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def extract_to_local():
    import requests
    import os

    url = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.csv'
    local_path = '/opt/airflow/data/all_transactions.csv'
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes
        with open(local_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        print(f"File successfully downloaded to {local_path}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve data: {e}")


with DAG(
    dag_id='extract_to_local',
    default_args=default_args,
    description='Extract data to local device',
    start_date=datetime(2024, 11, 28),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='extract_to_local_task',
        python_callable=extract_to_local,
    )

    task1