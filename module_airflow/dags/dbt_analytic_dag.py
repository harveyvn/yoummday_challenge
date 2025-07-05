from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def create_bash_operator(task_id) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command='docker exec -it listen_brainz_dbt_container dbt run --profiles-dir profiles',
    )


with DAG(
    dag_id='dbt_analytic_job',
    default_args=default_args,
    description='',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'listen'],
) as dag:
    run_spark_job = create_bash_operator(task_id='dbt_analytic_job')
