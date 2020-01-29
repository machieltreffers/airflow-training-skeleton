from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    'owner': 'mtreffers',
    'start_date': datetime(2019, 1, 1),
}

dag = DAG(
    dag_id='final_use_case',
    default_args=args,
    schedule_interval='@daily',
)
