import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)

# from airflow.utils.dates import days_ago

# Rename project ID
PROJECT_ID = "distributed-system-347306"

# Rename Cluster
CLUSTER_NAME = "violence-cluster"


REGION = "us-central1"
ZONE = "us-central1-c"

# Get api key from GCP
API_KEY = "AIzaSyBvYG4uWhEas64G2MifzZnwpQHA4KP5Tyw"
# GCS location of your PySpark Code
PYSPARK_URI = "gs://staging.distributed-system-347306.appspot.com/violence_etl.py"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False
}

with models.DAG(
        "dataproc_etl_pyspark", 
        schedule_interval='@once',
        default_args=default_dag_args) as dag:
    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
    }

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    pyspark_task
