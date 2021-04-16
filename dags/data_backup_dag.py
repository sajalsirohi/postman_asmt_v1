
from datetime import timedelta

from airflow import DAG

from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago

fd = open('/c/users/sanjeev/airflowhome/dags/scripts/backup.sql', 'r')
sqlFile = fd.read()
fd.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sirohisajal@gmail.com'],
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
    'Database-Backup',
    default_args=default_args,
    description='DAG to do Backup of the [DATA] table',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['SQL Operator'],
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
task1 = MsSqlOperator(
    task_id='do_backup',
    sql=sqlFile,
    dag=dag,
)

dag.doc_md = __doc__

task1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

task1