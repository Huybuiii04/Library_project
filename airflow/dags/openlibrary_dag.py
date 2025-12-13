from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os
from pathlib import Path

# Define BASE_DIR for the project
BASE_DIR = Path('/opt')  # In Docker, BASE_DIR is /opt

# Add scripts directory to path
sys.path.insert(0, '/opt/scripts/crawl-data')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'openlibrary_crawler_v6',
    default_args=default_args,
    description='Crawl Open Library API and store in MySQL',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['crawler', 'openlibrary', 'mysql'],
)


def run_crawler():
    """Run the Open Library crawler."""
    from crawl_openlibrary import run_crawl
    run_crawl()

def check_results():
    """Check crawl results in database."""
    from db_helper import get_books_count
    count = get_books_count()
    print(f"Total books in database: {count}")
    
    if count == 0:
        raise Exception("No books found in database after crawl!")
    
    return count

crawl_task = PythonOperator(
    task_id='crawl_openlibrary',
    python_callable=run_crawler,
    dag=dag,
    execution_timeout=timedelta(hours=6),  # Max 6 hours for crawling
)

verify_task = PythonOperator(
    task_id='verify_results',
    python_callable=check_results,
    dag=dag,
)

# Set task dependencies
crawl_task >> verify_task
