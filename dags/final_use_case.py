from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator
#from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    'owner': 'mtreffers',
    'start_date': datetime(2020, 1, 1),
}

dag = DAG(
    dag_id='final_use_case',
    default_args=args,
    schedule_interval='@daily',
)

get_exchange_rate = HttpToGcsOperator(
    task_id='http_to_gcs',
    http_conn_id='http_final_use_case',
    endpoint='history?start_at=2018-01-01&end_at=2018-01-04&symbols=EUR&base=GBP',
    gcs_bucket='final_use_case',
    gcs_path='exchange_rates/{{ ds_nodash }}_exchange_rates_{}.json',
    dag=dag,
)

dataproc_cluster_create = DataprocClusterCreateOperator(
    task_id='dataproc_cluster_create',
    project_id='airflowbolcom-jan2829-e6b55095',
    num_workers='5',
    cluster_name='compute_stats',
    dag=dag,
)

calculate_rates = DataProcPySparkOperator(
    task_id='calculate_rates',

    dag=dag,

)

get_exchange_rate >> dataproc_cluster_create
