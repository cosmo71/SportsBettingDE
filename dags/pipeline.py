import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
#from airflow.providers.google.cloud.sensors.gcs import GCSBucketSensor
from airflow.providers.google.cloud.sensors.bigquery import (
    #BigQueryDatasetExistenceSensor,
    BigQueryTableExistenceSensor
)
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from scripts import scrape_props_data, transform_props_data


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
from io import StringIO
import time
import re
from math import ceil

DATASET_NAME = os.getenv('BQ_DATASET_NAME')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
DATA_OBJECT_NAME =  f"props_{str(datetime.now().date())}.csv"
CSV_FILE_LOCAL_PATH = f"/opt/airflow/data/transformed_props_{str(datetime.now().date())}.csv"
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='props_ingestion_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 4, 25),
    schedule='@daily',   
    catchup=False,
) as dag: 
    
    extract_props_task = PythonOperator(
        task_id='extract_props',
        python_callable=scrape_props_data,
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
    )

    transform_props_task = PythonOperator(
        task_id='transform_props',
        python_callable=transform_props_data,
        dag=dag,
    )

    # Upload file
    upload_file = LocalFilesystemToGCSOperator(
        task_id = "upload_file_to_bucket",
        src = CSV_FILE_LOCAL_PATH,
        dst = DATA_OBJECT_NAME,
        bucket = GCS_BUCKET_NAME,
    )
    

    extract_props_task >> transform_props_task >> upload_file




   
