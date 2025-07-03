from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='hello_spark_job',
    default_args=default_args,
    description='A simple Spark job executed by Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'example'],
) as dag:
    run_spark_job = BashOperator(
        task_id='run_hello_spark',
        bash_command='docker exec listen_brainz_spark_master spark-submit /opt/bitnami/spark/app/hello.py',
    )
