import os
from airflow.decorators import dag
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.insert(0, './include/')

# Import the functions from your scripts
from include.extract_breweries_data import extract_breweries_data
from include.transform import transform
from include.partition_by_country import save_breweries_to_silver_by_country
from include.gold_view import create_gold_view

# Configuration Constants
API_URL = os.getenv('BREWERY_API_URL', "https://api.openbrewerydb.org/breweries")
BRONZE_PATH = os.getenv('BRONZE_PATH', "/usr/local/airflow/data/bronze/")
STAGE_PATH = os.getenv('STAGE_PATH', "/usr/local/airflow/data/stage/")
SILVER_PATH = os.getenv('SILVER_PATH', "/usr/local/airflow/data/silver/")
GOLD_PATH = os.getenv('GOLD_PATH', "/usr/local/airflow/data/gold/")

default_args = {
    'owner': 'Henrique',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

@dag(
    default_args=default_args,
    description='DAG to consume data from Breweries API and generate data for gold layer',
    schedule_interval='@daily',
    catchup=False,
)
def dag_breweries():
    # Task to check if the API is alive
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='brewery_api',
        endpoint='', 
        method='GET',
        response_check=lambda response: response.status_code == 200,
        timeout=10,
        retries=3,
        poke_interval=5,
    )

    # Task to run the Pandas extraction script
    run_extraction = PythonOperator(
        task_id='run_extraction',
        python_callable=extract_breweries_data,
        op_kwargs={'api_url': API_URL, 'stage_path': STAGE_PATH},
    )

    # Task to run the Pandas transformation script
    run_transformation = PythonOperator(
        task_id='run_transformation',
        python_callable=transform,
        op_kwargs={'input_path': STAGE_PATH, 'output_path': BRONZE_PATH},
    )

    # Task to partition data by country
    run_partitioning = PythonOperator(
        task_id='run_partitioning',
        python_callable=save_breweries_to_silver_by_country,
        op_kwargs={'input_path': BRONZE_PATH, 'output_path': SILVER_PATH},
    )
    
    # Generate gold layer report
    run_report = PythonOperator(
        task_id='run_report',
        python_callable=create_gold_view,
        op_kwargs={'input_path': SILVER_PATH, 'output_path': GOLD_PATH},
    )

    # Set task dependencies
    check_api >> run_extraction >> run_transformation >> run_partitioning >> run_report

# Instantiate the DAG
dag_instance = dag_breweries()