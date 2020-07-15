import reporting as rep
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
    'productivity_report',
    default_args=default_args,
    description='generate productivity report',
    schedule_interval=timedelta(days=1),
)

def generate_report(ds,**kwargs):
    from_date ='2020-06-16 08:17:17.862045+00'
    duration_in_hours=1.5
    member='32d3e157-fecc-4066-9b78-b92683c2f78b'
    rep.connect()
    start,end=rep.get_start_end_date(from_date,duration_in_hours)
    actions = rep.actions(member,start,end)
    events = rep.events(member,start, end)
    private_time, break_time = rep.process_actions(actions)
    idle_time=rep.process_events(events,1)
    break_hours = sum(break_time) / 3600  # convert to hours
    private_hours = sum(private_time) / 3600
    office_hours = (end- start).total_seconds() / 3600
    idle_hours = sum(idle_time) / 3600
    active_hours = office_hours - (idle_hours + private_hours + break_hours)
    report_date = start
    values = {
                "member_id":member,
                "break_hours":break_hours,
                "private_hours":private_hours,
                "office_hours":office_hours,
                "idle_hours":idle_hours,
                "active_hours":active_hours,
                "report_date":report_date
            }
    rep.update_productivity_report(values)
    rep.dispose()

task1= PythonOperator(
        task_id ='get_data',
        provide_context=True,
        python_callable=generate_report,
        dag=dag)
