from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import random
import uuid
from faker import Faker
from google.cloud import storage
import numpy as np
import os
import io

default_args = {
    'owner': 'ndst',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/keys/model-zoo-444708-e2-0b9701afda8d.json"

def synth_data(**kwargs):
    fake = Faker()
    transactions = []

    for _ in range(500):

        #customer_id 10% missing
        customer_id = random.randint(1000, 9999) if random.random() > 0.1 else np.nan

        #amount 5% missing
        amount = round(random.uniform(10, 500), 2) if random.random() > 0.05 else np.nan

        timestamp = datetime.now() - timedelta(days=random.randint(0, 365))  # Random date within the past year

        #product_category 7% missing
        product_category = random.choice(["Electronics", "Clothing", "Groceries", "Books", "Toys"]) if random.random() > 0.07 else np.nan

        location = fake.city()
        
        transactions.append({
            "customer_id": customer_id,
            "amount": amount,
            "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "product_category": product_category,
            "location": location
        })
    
    df = pd.DataFrame(transactions)
    execution_date = kwargs['ds']
    file_name = f"raw_{execution_date}.csv"
    df.to_csv(file_name, index=False)

    bucket_name = "rizzler"
    destination_blob_name = f"raw/{file_name}"
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_filename(file_name)
    print(f"File {file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")

def preprocess_data(**kwargs):
    bucket_name = "rizzler"
    execution_date = kwargs['ds']
    raw_file_name = f"raw_{execution_date}.csv"
    processed_file_name = f"mairaw_{execution_date}.csv"
    raw_blob_path = f"raw/{raw_file_name}"
    processed_blob_path = f"mairaw/{processed_file_name}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    blob = bucket.blob(raw_blob_path)
    content = blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))

    df['customer_id'].fillna(-1, inplace=True)
    df['amount'].fillna(df['amount'].median(), inplace=True)
    df['product_category'].fillna('Unknown', inplace=True)

    df.to_csv(processed_file_name, index=False)

    processed_blob = bucket.blob(processed_blob_path)
    processed_blob.upload_from_filename(processed_file_name)

    print(f"File {processed_file_name} uploaded to gs://{bucket_name}/{processed_blob_path}")

with DAG(
    dag_id='synth_data',
    default_args=default_args,
    description='Generate Synthetic Data with Missing Values and preprocess data',
    catchup=True,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='synth_data',
        python_callable=synth_data,
    )

    task2 = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )

    task1 >> task2