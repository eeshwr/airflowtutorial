# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
from datetime import timedelta
import daily_report as rep
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_report',
    default_args=default_args,
    description='generate_daily_report',
    schedule_interval=timedelta(days=1),
)

def create_daily_report(ds, **kwargs):
    record = rep.DailyReport()
    record.create_daily_report()


def write_to_db(ds, **kwargs):
    record = rep.DailyReport()
    record.write_to_db()


t1 = PythonOperator(
        task_id='create_daily_report',
        provide_context=True,
        python_callable=create_daily_report,
        dag=dag)

t2 = PythonOperator(
        task_id='write_to_db',
        provide_context=True,
        python_callable=write_to_db,
        dag=dag)

t1 >> t2
