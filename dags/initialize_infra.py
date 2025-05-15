import os 
import sys
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime


from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator
)

from airflow import DAG

default_args = {
    'owner': 'airflow',
}

DATASET_NAME = os.getenv('BQ_DATASET_NAME')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
DATA_OBJECT_NAME =  f"props_{str(datetime.now().date())}.csv"
CSV_FILE_LOCAL_PATH = f"transformed_props_{str(datetime.now().date())}.csv"
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')

with DAG(
    dag_id='props_infra_setup',
    default_args=default_args,
    start_date=datetime(2024, 4, 25),
    schedule=None,   
    catchup=False,
) as dag: 

    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=GCS_BUCKET_NAME, gcp_conn_id="google_cloud_default")

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id = "create_dataset", dataset_id = DATASET_NAME, gcp_conn_id="google_cloud_default")

    create_external_table = BigQueryCreateTableOperator(
    task_id="create_external_table",
    table_id=f"props",
    dataset_id=DATASET_NAME,
    gcp_conn_id="google_cloud_default",
    table_resource={
        "tableReference": {
            "projectId": GCS_PROJECT_ID,
            "datasetId": DATASET_NAME,
            "tableId": "props",
        },
        "externalDataConfiguration": {
        "sourceFormat": "CSV",
        "sourceUris": [f"gs://{GCS_BUCKET_NAME}/props_*.csv"],
        "autodetect": True,
        "skipLeadingRows": 1
        },

    }
)
    
    delete_table = BigQueryDeleteTableOperator(
    task_id="delete_existing_table",
    deletion_dataset_table=f"{GCS_PROJECT_ID}.{DATASET_NAME}.props",
    ignore_if_missing=True,
    gcp_conn_id="google_cloud_default"
)


    
    create_bucket >> create_dataset >> delete_table >> create_external_table

