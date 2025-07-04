from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='ingestion_job',
    default_args=default_args,
    description='',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'data'],
) as dag:
    run_spark_job = BashOperator(
        task_id='ingestion_job',
        bash_command='docker exec listen_brainz_spark_master bash -c "PYTHONPATH=/opt/bitnami/spark spark-submit /opt/bitnami/spark/app/upload.py"',
    )
