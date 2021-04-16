from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  }

dag = DAG('azure-ingestion-dag', default_args=default_args, schedule_interval=timedelta(minutes=1))

t1 = BashOperator(
    task_id='python_script',
    bash_command='python3 /c/users/sanjeev/PycharmProjects/postman_asmt/azure_data_ingestion.py',
    dag=dag)

t1