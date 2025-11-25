from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from scripts.extract_from_s3 import extract_sales_data_from_s3
from scripts.transform_clean_sales import clean_and_transform_sales_data
from scripts.load_to_snowflake import load_data_to_snowflake

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
dag = DAG(
    'ecommerce_sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for e-commerce sales data from S3 to Snowflake',
    schedule_interval='@daily',  # Runs daily
    catchup=False,
    tags=['etl', 'sales', 'snowflake', 'aws'],
)

# Task 1: Extract raw sales data from AWS S3
extract_task = PythonOperator(
    task_id='extract_sales_data_from_s3',
    python_callable=extract_sales_data_from_s3,
    op_kwargs={
        'bucket_name': 'ecommerce-raw-data',
        'prefix': 'sales/',
        'local_path': '/tmp/raw_sales_data.csv'
    },
    dag=dag,
)

# Task 2: Transform and clean the sales data
transform_task = PythonOperator(
    task_id='transform_clean_sales_data',
    python_callable=clean_and_transform_sales_data,
    op_kwargs={
        'input_file': '/tmp/raw_sales_data.csv',
        'output_file': '/tmp/cleaned_sales_data.csv'
    },
    dag=dag,
)

# Task 3: Load cleaned data into Snowflake
load_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    op_kwargs={
        'csv_file': '/tmp/cleaned_sales_data.csv',
        'table_name': 'sales_data',
        'database': 'SALES_DB',
        'schema': 'PUBLIC'
    },
    dag=dag,
)

# Define task dependencies (ETL flow)
extract_task >> transform_task >> load_task
