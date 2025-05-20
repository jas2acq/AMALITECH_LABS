# dags/pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys


# This ensures Python can find modules in the 'src' directory relative to the DAG file.
# It makes your imports work correctly when 'src' is inside 'dags'.
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))


# Import the functions from your separate scripts
from load_csv_to_mysql import load_csv_to_mysql
from validate_data import validate_data
from transform_data import transform_data
from compute_kpis import compute_kpis
from load_to_postgres import load_to_postgres


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='Main_pipeline',
    default_args=default_args,
    schedule_interval='0 */3 * * *', # At minute 0, every 3rd hour
    catchup=False,
    tags=['flight_price', 'etl', 'pipeline'],
    description='End-to-End Flight Price Analysis Pipeline for Bangladesh'
) as dag:

    task_load_csv_to_mysql = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
        doc_md="""
        #### Load Raw CSV to MySQL
        Ingests raw flight data from CSV into a MySQL table.
        """
    )

    task_validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        doc_md="""
        #### Validate Ingested Data
        Performs quality checks on the raw data loaded into MySQL.
        """
    )

    task_transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        doc_md="""
        #### Transform and Clean Data
        Applies cleaning, feature engineering, and transformation logic to the validated data.
        """
    )

    task_compute_kpis = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
        doc_md="""
        #### Compute Key Performance Indicators (KPIs)
        Calculates important metrics from the transformed data.
        """
    )

    task_load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        doc_md="""
        #### Load Data to PostgreSQL Warehouse
        Loads the final processed data or computed KPIs into the PostgreSQL data warehouse.
        """
    )

    # Define the sequential dependencies of the pipeline
    task_load_csv_to_mysql >> task_validate_data >> task_transform_data >> task_compute_kpis >> task_load_to_postgres