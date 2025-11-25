# E-Commerce Sales ETL Pipeline

## Overview
Automated ETL pipeline using **Apache Airflow** to extract raw sales data from **AWS S3**, clean and transform it, and load into **Snowflake** for BI dashboards.

## Architecture
- **Extract**: Download sales CSV files from S3
- **Transform**: Clean nulls, standardize columns, calculate totals
- **Load**: Bulk load into Snowflake using COPY INTO
- **Orchestration**: Airflow DAG scheduled daily

## Airflow DAG
The pipeline is orchestrated by `ecommerce_sales_etl_dag.py` with:
- Daily schedule (`@daily`)
- Task dependencies: Extract → Transform → Load
- Error handling and retries

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure Airflow: `airflow db init`
3. Copy DAG to Airflow folder: `cp dags/ecommerce_sales_etl_dag.py ~/airflow/dags/`
4. Start Airflow: `airflow webserver` and `airflow scheduler`
