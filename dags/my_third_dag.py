from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'mtreffers',
    'start_date': datetime(2020, 1, 1),
}

dag = DAG(
    dag_id='my_third_dag',
    default_args=args,
    schedule_interval='@daily',
)


def _get_weekday(execution_date, **context):

    weekday=execution_date.strftime("%a")
    print(weekday)

    if weekday == "Mon":
        person="email_bob"
    elif weekday == "Wed":
        person="email_alice"
    elif weekday == "Fri":
        person="email_joe"
    else:
        person="unkown"

    return person

print_week_day = PythonOperator(
    task_id="print_week_day",
    python_callable=_get_weekday,
    provide_context=True,
    dag=dag,
)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=_get_weekday,
    provide_context=True,
    dag=dag
)

join = DummyOperator(
    task_id="join",
    trigger_rule="none_failed",
    dag=dag
)


print_week_day >> branching

persons = ["email_bob","email_alice","email_joe"]
for person in persons:
    branching >> DummyOperator(task_id=person, dag=dag) >> join
