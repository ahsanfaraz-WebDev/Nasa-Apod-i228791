"""
NASA APOD ETL Pipeline DAG
Author: MLOps-i228791-A 
Description: Complete ETL pipeline with DVC and Git versioning
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add include directory to Python path
sys.path.insert(0, '/usr/local/airflow/include')

# Import pipeline class
from apod_pipeline import APODPipeline

# DAG default arguments
default_args = {
    'owner': 'mlops_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Wrapper functions for tasks
def extract_data_task(**context):
    """Extract data from NASA APOD API"""
    pipeline = APODPipeline()
    data = pipeline.extract_data()
    # Push data to XCom
    context['task_instance'].xcom_push(key='raw_data', value=data)
    return data

def transform_data_task(**context):
    """Transform raw JSON data"""
    pipeline = APODPipeline()
    # Pull data from XCom
    raw_data = context['task_instance'].xcom_pull(
        task_ids='extract_apod_data',
        key='raw_data'
    )
    df = pipeline.transform_data(raw_data)
    # Convert DataFrame to dict for XCom
    df_dict = df.to_dict('records')[0]
    context['task_instance'].xcom_push(key='transformed_data', value=df_dict)
    return df_dict

def load_postgres_task(**context):
    """Load data to PostgreSQL"""
    import pandas as pd
    pipeline = APODPipeline()
    # Pull data from XCom and convert back to DataFrame
    data_dict = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    df = pd.DataFrame([data_dict])
    pipeline.load_to_postgres(df)

def load_csv_task(**context):
    """Load data to CSV file"""
    import pandas as pd
    pipeline = APODPipeline()
    # Pull data from XCom and convert back to DataFrame
    data_dict = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    df = pd.DataFrame([data_dict])
    pipeline.load_to_csv(df)

def dvc_version_task(**context):
    """Version CSV with DVC"""
    pipeline = APODPipeline()
    pipeline.version_with_dvc()

def git_commit_task(**context):
    """Commit DVC metadata to Git"""
    pipeline = APODPipeline()
    pipeline.commit_to_git()

# DAG definition
with DAG(
    dag_id='nasa_apod_etl_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with DVC and Git versioning',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 15),
    catchup=False,
    tags=['nasa', 'etl', 'mlops', 'assignment3'],
    doc_md=__doc__,
) as dag:
    
    # Start marker
    start = EmptyOperator(
        task_id='start_pipeline'
    )
    
    # Task 1: Extract
    extract = PythonOperator(
        task_id='extract_apod_data',
        python_callable=extract_data_task,
        provide_context=True
    )
    
    # Task 2: Transform
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_task,
        provide_context=True
    )
    
    # Task 3a: Load to PostgreSQL
    load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_postgres_task,
        provide_context=True
    )
    
    # Task 3b: Load to CSV
    load_csv = PythonOperator(
        task_id='load_to_csv',
        python_callable=load_csv_task,
        provide_context=True
    )
    
    # Sync point
    sync = EmptyOperator(
        task_id='sync_after_load'
    )
    
    # Task 4: DVC Version
    dvc_version = PythonOperator(
        task_id='version_with_dvc',
        python_callable=dvc_version_task,
        provide_context=True
    )
    
    # Task 5: Git Commit
    git_commit = PythonOperator(
        task_id='commit_to_git',
        python_callable=git_commit_task,
        provide_context=True
    )
    
    # End marker
    end = EmptyOperator(
        task_id='pipeline_complete'
    )
    
    # Task dependencies
    start >> extract >> transform
    transform >> [load_postgres, load_csv] >> sync
    sync >> dvc_version >> git_commit >> end