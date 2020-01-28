import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'mtreffers',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='None',
    dagrun_timeout=timedelta(minutes=60),
)

run_first = DummyOperator(
    task_id='run_first',
    dag=dag,
)

run_second = DummyOperator(
    task_id='run_second',
    dag=dag,
)

run_third = DummyOperator(
    task_id='run_third',
    dag=dag,
)

run_fourth = DummyOperator(
    task_id='run_fourth',
    dag=dag,
)

run_fifth = DummyOperator(
    task_id='run_fifth',
    dag=dag,
)

run_first >> run_second >> run_third >> run_fifth
run_second >> run_fourth >> run_fifth
