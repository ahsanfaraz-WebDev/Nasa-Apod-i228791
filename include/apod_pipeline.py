"""
NASA APOD ETL Pipeline Module
Author: i228791-Ahsan Faraz
Description: Handles extraction, transformation, and loading of NASA APOD data
"""

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os
import subprocess
import logging
from typing import Dict, Any, List
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APODPipeline:
    """
    Complete ETL Pipeline for NASA Astronomy Picture of the Day
    
    Attributes:
        api_url: NASA APOD API endpoint
        api_key: API authentication key
        csv_path: Path to store CSV data
        db_config: PostgreSQL connection configuration
    """
    
    def __init__(self):
        self.api_url = "https://api.nasa.gov/planetary/apod"
        self.api_key = "BTvi9l7qiB0OhZMBoc44YAYxjaE5p6e94YGLm2zK"
        self.csv_path = "/usr/local/airflow/include/apod_data.csv"
        self.dvc_file_path = "/usr/local/airflow/include/apod_data.csv.dvc"
        
        # Database configuration
        # Database configuration (using Astronomer's default postgres)
        self.db_config = {
        'host': 'postgres',
        'database': 'postgres',  # Changed from 'airflow'
        'user': 'postgres',      # Changed from 'airflow'
        'password': 'postgres',  # Changed from 'airflow'
        'port': 5432
        }
        
        # Ensure include directory exists
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
    
    def extract_data(self) -> Dict[str, Any]:
        """
        Step 1: Extract data from NASA APOD API
        
        Returns:
            dict: Raw JSON data from API
            
        Raises:
            requests.RequestException: If API request fails
        """
        logger.info("="*60)
        logger.info("STEP 1: EXTRACTING DATA FROM NASA APOD API")
        logger.info("="*60)
        
        try:
            # Prepare API parameters
            params = {
                'api_key': self.api_key,
                'date': datetime.now().strftime('%Y-%m-%d')
            }
            
            logger.info(f"Requesting data for date: {params['date']}")
            
            # Make API request
            response = requests.get(
                self.api_url, 
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Log success
            logger.info(f"✓ Successfully extracted data")
            logger.info(f"  - Date: {data.get('date')}")
            logger.info(f"  - Title: {data.get('title')}")
            logger.info(f"  - Media Type: {data.get('media_type')}")
            logger.info("="*60)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Failed to extract data: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse API response: {str(e)}")
            raise
    
    def transform_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Step 2: Transform raw JSON data to clean format
        
        Args:
            raw_data: Raw JSON data from API
            
        Returns:
            list: List of dictionaries (serializable for XCom)
        """
        logger.info("="*60)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*60)
        
        try:
            # Extract relevant fields with defaults
            cleaned_data = {
                'date': raw_data.get('date'),
                'title': raw_data.get('title', 'No Title'),
                'url': raw_data.get('url', ''),
                'explanation': raw_data.get('explanation', ''),
                'media_type': raw_data.get('media_type', 'image'),
                'hdurl': raw_data.get('hdurl', ''),
                'copyright': raw_data.get('copyright', 'Public Domain')
            }
            
            # Validate required fields
            if not cleaned_data['date']:
                raise ValueError("Date field is missing")
            
            # Clean text fields (remove extra whitespace)
            text_fields = ['title', 'explanation', 'copyright']
            for field in text_fields:
                if cleaned_data[field]:
                    cleaned_data[field] = cleaned_data[field].strip()
            
            # Return as list of dicts (XCom serializable)
            result = [cleaned_data]
            
            # Log transformation details
            logger.info(f"✓ Successfully transformed data")
            logger.info(f"  - Records: {len(result)}")
            logger.info(f"  - Fields: {list(cleaned_data.keys())}")
            logger.info(f"  - Date: {cleaned_data['date']}")
            logger.info("="*60)
            
            return result
            
        except Exception as e:
            logger.error(f"✗ Transformation failed: {str(e)}")
            raise
    
    def load_to_postgres(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Step 3a: Load data to PostgreSQL database
        
        Args:
            data: List of dictionaries containing cleaned data
            
        Returns:
            dict: Status information about the load
            
        Raises:
            psycopg2.Error: If database operation fails
        """
        logger.info("="*60)
        logger.info("STEP 3A: LOADING DATA TO POSTGRESQL")
        logger.info("="*60)
        
        conn = None
        cursor = None
        
        try:
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data)
            
            # Establish database connection
            logger.info("Connecting to PostgreSQL...")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Prepare insert query with conflict handling
            insert_query = """
                INSERT INTO apod_data (
                    date, title, url, explanation, media_type, hdurl, copyright
                )
                VALUES %s
                ON CONFLICT (date) 
                DO UPDATE SET
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    explanation = EXCLUDED.explanation,
                    media_type = EXCLUDED.media_type,
                    hdurl = EXCLUDED.hdurl,
                    copyright = EXCLUDED.copyright,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, date;
            """
            
            # Prepare data tuples
            data_tuples = [
                (
                    row['date'],
                    row['title'],
                    row['url'],
                    row['explanation'],
                    row['media_type'],
                    row['hdurl'],
                    row['copyright']
                )
                for _, row in df.iterrows()
            ]
            
            # Execute batch insert
            execute_values(cursor, insert_query, data_tuples)
            result = cursor.fetchone()
            
            # Commit transaction
            conn.commit()
            
            # Log success
            logger.info(f"✓ Successfully loaded data to PostgreSQL")
            logger.info(f"  - Record ID: {result[0]}")
            logger.info(f"  - Date: {result[1]}")
            logger.info(f"  - Rows affected: {cursor.rowcount}")
            logger.info("="*60)
            
            return {
                'status': 'success',
                'record_id': result[0],
                'date': str(result[1]),
                'rows_affected': cursor.rowcount
            }
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"✗ PostgreSQL load failed: {str(e)}")
            raise
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logger.info("Database connection closed")
    
    def load_to_csv(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Step 3b: Load data to CSV file
        
        Args:
            data: List of dictionaries containing cleaned data
            
        Returns:
            dict: Status information about the CSV save
        """
        logger.info("="*60)
        logger.info("STEP 3B: LOADING DATA TO CSV")
        logger.info("="*60)
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Check if CSV exists
            file_exists = os.path.exists(self.csv_path)
            
            if file_exists:
                # Append to existing CSV
                logger.info(f"Appending to existing CSV: {self.csv_path}")
                df.to_csv(self.csv_path, mode='a', header=False, index=False)
            else:
                # Create new CSV with header
                logger.info(f"Creating new CSV: {self.csv_path}")
                df.to_csv(self.csv_path, mode='w', header=True, index=False)
            
            # Verify file was created
            if os.path.exists(self.csv_path):
                file_size = os.path.getsize(self.csv_path)
                logger.info(f"✓ Successfully saved to CSV")
                logger.info(f"  - Path: {self.csv_path}")
                logger.info(f"  - Size: {file_size} bytes")
                
                # Count total rows
                with open(self.csv_path, 'r') as f:
                    total_rows = sum(1 for _ in f) - 1  # Exclude header
                logger.info(f"  - Total rows in CSV: {total_rows}")
            
            logger.info("="*60)
            
            return {
                'status': 'success',
                'path': self.csv_path,
                'size_bytes': file_size,
                'total_rows': total_rows
            }
            
        except Exception as e:
            logger.error(f"✗ CSV load failed: {str(e)}")
            raise
    
    def version_with_dvc(self) -> Dict[str, str]:
        """
        Step 4: Version CSV file with DVC
        
        Creates .dvc metadata file and stores data in DVC cache
        
        Returns:
            dict: Status information
            
        Raises:
            Exception: If DVC operation fails
        """
        logger.info("="*60)
        logger.info("STEP 4: VERSIONING DATA WITH DVC")
        logger.info("="*60)
        
        original_dir = os.getcwd()
        
        try:
            # Change to airflow directory
            os.chdir('/usr/local/airflow')
            
            # Verify CSV exists
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
            
            logger.info(f"Adding file to DVC: {self.csv_path}")
            
            # Add file to DVC
            result = subprocess.run(
                ['dvc', 'add', 'include/apod_data.csv'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                # Check if it's already tracked
                if "is already tracked" in result.stderr or "is already tracked" in result.stdout:
                    logger.info("File already tracked by DVC, updating...")
                else:
                    logger.error(f"DVC stderr: {result.stderr}")
                    logger.error(f"DVC stdout: {result.stdout}")
                    raise Exception(f"DVC add failed: {result.stderr}")
            
            # Verify .dvc file was created
            if os.path.exists(self.dvc_file_path):
                logger.info(f"✓ DVC metadata file created: {self.dvc_file_path}")
                
                # Read and log DVC file content
                with open(self.dvc_file_path, 'r') as f:
                    dvc_content = f.read()
                    logger.info(f"DVC file content:\n{dvc_content}")
            else:
                logger.warning("DVC file not found, but operation may have succeeded")
            
            # Push to DVC remote
            logger.info("Pushing to DVC remote storage...")
            push_result = subprocess.run(
                ['dvc', 'push'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if push_result.returncode == 0:
                logger.info("✓ Successfully pushed to DVC remote")
            else:
                logger.warning(f"DVC push warning: {push_result.stderr}")
            
            logger.info("="*60)
            
            return {
                'status': 'success',
                'dvc_file': self.dvc_file_path
            }
            
        except Exception as e:
            logger.error(f"✗ DVC versioning failed: {str(e)}")
            raise
        finally:
            # Always return to original directory
            os.chdir(original_dir)
    
    def commit_to_git(self) -> None:
        """
        Step 5: Commit DVC metadata to Git repository
        
        Commits .dvc file and updated .gitignore to Git
        """
        logger.info("="*60)
        logger.info("STEP 5: COMMITTING TO GIT")
        logger.info("="*60)
        
        try:
            # Change to airflow directory
            original_dir = os.getcwd()
            os.chdir('/usr/local/airflow')
            
            # Check Git status
            status_result = subprocess.run(
                ['git', 'status', '--short'],
                capture_output=True,
                text=True
            )
            logger.info(f"Git status:\n{status_result.stdout}")
            
            # Add DVC metadata files
            logger.info("Adding DVC files to Git...")
            subprocess.run(
                ['git', 'add', 'include/apod_data.csv.dvc', '.gitignore'],
                check=True
            )
            
            # Create commit message with timestamp
            commit_message = f"Update APOD data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Commit changes
            logger.info(f"Committing: {commit_message}")
            result = subprocess.run(
                ['git', 'commit', '-m', commit_message],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                logger.info(f"✓ Successfully committed to Git")
                logger.info(f"  - Message: {commit_message}")
                logger.info(f"  - Output: {result.stdout}")
                
                # Push to remote repository
                logger.info("Pushing to remote repository...")
                push_result = subprocess.run(
                    ['git', 'push', 'origin', 'main'],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                if push_result.returncode == 0:
                    logger.info(f"✓ Successfully pushed to GitHub")
                else:
                    logger.warning(f"⚠ Git push failed: {push_result.stderr}")
                    logger.warning("This might be due to authentication. Commit succeeded locally.")
                    
            elif "nothing to commit" in result.stdout:
                logger.info("ℹ No changes to commit (data unchanged)")
            else:
                logger.warning(f"Git commit returned code {result.returncode}")
                logger.warning(f"  - stdout: {result.stdout}")
                logger.warning(f"  - stderr: {result.stderr}")
            
            logger.info("="*60)
            
            # Return to original directory
            os.chdir(original_dir)
            
        except Exception as e:
            logger.error(f"✗ Git commit failed: {str(e)}")
            os.chdir(original_dir)
            raise

# Convenience functions for Airflow tasks
def extract_task():
    """Wrapper function for extract step"""
    pipeline = APODPipeline()
    return pipeline.extract_data()


def transform_task(**context):
    """Wrapper function for transform step"""
    pipeline = APODPipeline()
    raw_data = context['task_instance'].xcom_pull(task_ids='extract_apod_data')
    return pipeline.transform_data(raw_data)


def load_postgres_task(**context):
    """Wrapper function for PostgreSQL load step"""
    pipeline = APODPipeline()
    data = context['task_instance'].xcom_pull(task_ids='transform_data')
    return pipeline.load_to_postgres(data)


def load_csv_task(**context):
    """Wrapper function for CSV load step"""
    pipeline = APODPipeline()
    data = context['task_instance'].xcom_pull(task_ids='transform_data')
    return pipeline.load_to_csv(data)


def dvc_version_task():
    """Wrapper function for DVC versioning step"""
    pipeline = APODPipeline()
    return pipeline.version_with_dvc()


def git_commit_task():
    """Wrapper function for Git commit step"""
    pipeline = APODPipeline()
    return pipeline.commit_to_git()