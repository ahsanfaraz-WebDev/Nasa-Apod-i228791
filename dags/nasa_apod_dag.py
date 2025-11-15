"""
NASA APOD ETL Pipeline DAG
Author: i228791-Ahsan Faraz
Description: Complete ETL pipeline with DVC and Git versioning

DAG Tasks:
1. extract_apod_data - Fetch data from NASA API
2. transform_data - Clean and structure data
3. load_to_postgres - Store in PostgreSQL database
4. load_to_csv - Store in CSV file
5. version_with_dvc - Version data with DVC
6. commit_to_git - Commit metadata to Git
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add include directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'include'))

# Import pipeline functions
from apod_pipeline import (
    extract_task,
    transform_task,
    load_postgres_task,
    load_csv_task,
    dvc_version_task,
    git_commit_task
)

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

# DAG definition
with DAG(
    dag_id='nasa_apod_etl_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with DVC and Git versioning',
    schedule_interval='@daily',  # Run once per day at midnight
    start_date=datetime(2025, 11, 15),
    catchup=False,  # Don't backfill past dates
    tags=['nasa', 'etl', 'mlops', 'assignment3'],
    doc_md=__doc__,
) as dag:
    
    # Start marker
    start = EmptyOperator(
        task_id='start_pipeline',
        doc_md="Pipeline execution starts here"
    )
    
    # Task 1: Extract data from NASA API
    extract = PythonOperator(
        task_id='extract_apod_data',
        python_callable=extract_task,
        doc_md="""
        ## Extract Phase
        - Connects to NASA APOD API
        - Fetches today's astronomy picture data
        - Returns raw JSON response
        """
    )
    
    # Task 2: Transform raw data
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
        provide_context=True,
        doc_md="""
        ## Transform Phase
        - Cleans and structures API data
        - Converts to pandas DataFrame
        - Validates required fields
        """
    )
    
    # Task 3a: Load to PostgreSQL
    load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_postgres_task,
        provide_context=True,
        doc_md="""
        ## Load to Database
        - Inserts data into PostgreSQL
        - Handles duplicate dates with upsert
        - Maintains data integrity
        """
    )
    
    # Task 3b: Load to CSV (runs in parallel with postgres)
    load_csv = PythonOperator(
        task_id='load_to_csv',
        python_callable=load_csv_task,
        provide_context=True,
        doc_md="""
        ## Load to CSV
        - Appends data to local CSV file
        - Creates file if doesn't exist
        - Prepares for DVC versioning
        """
    )
    
    # Synchronization point after parallel loads
    sync_loads = EmptyOperator(
        task_id='sync_after_load',
        doc_md="Waits for both load operations to complete before proceeding"
    )
    
    # Task 4: Version with DVC
    dvc_version = PythonOperator(
        task_id='version_with_dvc',
        python_callable=dvc_version_task,
        doc_md="""
        ## DVC Versioning
        - Adds CSV to DVC tracking
        - Creates .dvc metadata file
        - Pushes to DVC remote storage
        """
    )
    
    # Task 5: Commit to Git
    git_commit = PythonOperator(
        task_id='commit_to_git',
        python_callable=git_commit_task,
        doc_md="""
        ## Git Commit
        - Adds .dvc file to Git staging
        - Commits with timestamp
        - Pushes to GitHub remote
        - Links code version to data version
        """
    )
    
    # End marker
    end = EmptyOperator(
        task_id='pipeline_complete',
        doc_md="Pipeline completed successfully!"
    )
    
    # Define task dependencies (workflow)
    # Sequential: start -> extract -> transform
    # Parallel: transform -> [postgres, csv] -> sync
    # Sequential: sync -> dvc -> git -> end
    start >> extract >> transform
    transform >> [load_postgres, load_csv] >> sync_loads
    sync_loads >> dvc_version >> git_commit >> end