from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'mtreffers',
    'start_date': datetime(2020, 1, 27),
}

dag = DAG(
    dag_id='my_second_dag',
    default_args=args,
    schedule_interval=timedelta(minutes=150),
)


def _print_exec_date(**context):
    print("This is my execution date: " + str(context["execution_date"]))


print_execution_date = PythonOperator(
    task_id="print_execution_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag,
)

for i in (1, 5, 10):
    wait = BashOperator(
        task_id=f"wait_{i}",
        bash_command=f"sleep {i}"
    )


the_end = DummyOperator(
    task_id='the_end',
    dag=dag,
)

print_execution_date >> wait >> the_end
