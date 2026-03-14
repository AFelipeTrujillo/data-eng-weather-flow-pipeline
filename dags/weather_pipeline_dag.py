from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adding the scripts folder to the system path to import your class
sys.path.append('/opt/airflow/scripts')
from extract_weather_data import WeatherExtractor

# Default settings for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction():
    """Wrapper function to interface with your Python class"""
    # Location: Barcelona, Spain
    extractor = WeatherExtractor(latitude=41.3851, longitude=2.1734)
    data = extractor.fetch_data()
    extractor.save_to_json(data)

with DAG(
    'weather_ingestion_pipeline_v1',
    default_args=default_args,
    description='A professional pipeline to extract Barcelona weather data',
    schedule_interval='@hourly',  # Runs every hour
    catchup=False,               # Don't run for past dates
    tags=['weather', 'barcelona'],
) as dag:

    extraction_task = PythonOperator(
        task_id='extract_weather_from_api',
        python_callable=run_extraction,
    )

    # In the future, we will add transformation_task here
    extraction_task