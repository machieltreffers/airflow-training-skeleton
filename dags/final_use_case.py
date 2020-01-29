from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
#from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

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

http_to_gcs = HttpToGcsOperator(
    task_id='http_to_gcs',
    http_conn_id='http_final_use_case',
    endpoint='history?start_at=2018-01-01&end_at=2018-01-04&symbols=EUR&base=GBP',
    gcs_bucket='final_use_case',
    gcs_path='foobar',
    dag=dag,
)

http_to_gcs
