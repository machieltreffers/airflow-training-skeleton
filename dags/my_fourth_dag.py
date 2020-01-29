from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
#from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    'owner': 'mtreffers',
    'start_date': datetime(2019, 11, 1),
}

dag = DAG(
    dag_id='my_fourth_dag',
    default_args=args,
    schedule_interval='@daily',
)

postgres_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id='postgres_to_gcs',
    postgres_conn_id='postgres_gdd',
    sql="""select *
           from public.land_registry_price_paid_uk
           where transfer_date = {{ execution_date.strftime('%Y-%m-%d') }}
           ;""",
    bucket="airflow_exercise_4",
    filename="foobar",
    provide_context=True,
    dag=dag,
)

postgres_to_gcs

# select *
# from public.land_registry_price_paid_uk
# where transfer_date = '2003-12-17'
# ;
