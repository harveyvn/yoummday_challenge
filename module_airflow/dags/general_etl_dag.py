from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain

import ingestion_dag
import etl_user_dag
import etl_track_dag
import etl_listen_dag
import dbt_analytic_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='general_etl_job',
    default_args=default_args,
    description='',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'etl', 'data', 'users', 'tracks', 'listens', 'analytics'],
) as dag:
    ingestion_job = ingestion_dag.create_bash_operator(task_id='ingestion_job')
    etl_user_job = etl_user_dag.create_bash_operator(task_id='etl_user_job')
    etl_track_job = etl_track_dag.create_bash_operator(task_id='etl_track_job')
    etl_listen_job = etl_listen_dag.create_bash_operator(task_id='etl_listen_job')
    dbt_analytic_job = dbt_analytic_dag.create_bash_operator(task_id='dbt_analytic_job')
    chain(
        ingestion_job,
        etl_user_job,
        etl_track_job,
        etl_listen_job,
        dbt_analytic_job
    )
