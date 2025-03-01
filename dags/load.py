from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("skibidi") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar:/opt/spark/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom") \
    .getOrCreate()

BUCKET_NAME = "rizzler"
POSTGRES_CONN_ID = "db"
JDBC_URL = "jdbc:postgresql://172.21.131.170:5432/airflow"
JDBC_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

default_args = {
    'owner': 'ndst',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def read_file_and_store(**kwargs):
    execution_date = kwargs['ds']
    BUCKET_NAME = "rizzler"
    file_name = f"mairaw_{execution_date}.csv"
    file_path = f"gs://{BUCKET_NAME}/mairaw/{file_name}"

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df.write.jdbc(url=JDBC_URL, \
                  table="transaction", \
                  mode="append", \
                  properties=JDBC_PROPERTIES)
    
    print("Data saved to DB successfully!")

with DAG(
    dag_id='load_to_db',
    default_args=default_args,
    description='Load preprocessed data to PsdtgreSQL database',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = POSTGRES_CONN_ID,
        sql = """
            CREATE TABLE IF NOT EXISTS transaction (
                transaction_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                product_category VARCHAR(255) NOT NULL,
                location VARCHAR(255) NOT NULL
            );
        """
    )
    
    task2 = PythonOperator(
        task_id='read_file_and_store',
        python_callable=read_file_and_store
    )

task1 >> task2