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
    dag_id='upload_job',
    default_args=default_args,
    description='',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'example'],
) as dag:
    run_spark_job = BashOperator(
        task_id='upload_job',
        bash_command='docker exec listen_brainz_spark_master bash -c "PYTHONPATH=/opt/bitnami/spark spark-submit /opt/bitnami/spark/app/upload.py"',
    )
