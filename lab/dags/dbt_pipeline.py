from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='Run DBT models, snapshots, and tests',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=['dbt', 'ETL'],
) as dag:

    # Task to run DBT models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
    )

    # Define task dependencies
    dbt_run >> dbt_snapshot >> dbt_test
