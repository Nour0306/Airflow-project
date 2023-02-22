from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'schedule_interval': timedelta(days=1)
}

dag = DAG(
    'First_DAG',
    default_args=args,
    description='First DAG test',
    schedule_interval=timedelta(days=1),
)


t1 = BashOperator(
    task_id='print_instance',
    bash_command='echo "{{ task_instance_key_str }}"',
    dag=dag,
)

t2 = BashOperator(
    task_id='print_ciao',
    bash_command='echo ciao',
    dag=dag,
)

t1 >> t2
