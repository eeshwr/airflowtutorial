# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
from datetime import timedelta
import app_productivity_report as rep
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'activity_record',
    default_args=default_args,
    description='generate activity report',
    schedule_interval=timedelta(days=1),
)


def read_events(ds, **kwargs):
    app = rep.AppProductivity()
.   app.read_events()


def process_data(ds, **kwargs):
    record = rep.AppProductivity()
    record.process_data()


def write_to_db(ds, **kwargs):
    record = rep.AppProductivity()
    record.write_to_db()


t1 = PythonOperator(
        task_id='t1',
        provide_context=True,
        python_callable=read_events,
        dag=dag)

t2 = PythonOperator(
        task_id='t2',
        provide_context=True,
        python_callable=process_data,
        dag=dag)

t3 = PythonOperator(
        task_id='t3',
        provide_context=True,
        python_callable=write_to_db,
        dag=dag)

t1 >> t2 >> t3
