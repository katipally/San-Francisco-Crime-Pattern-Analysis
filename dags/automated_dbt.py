from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow import DAG

with DAG(
    dag_id='DAG_for_incidents',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='@yearly',
    tags=['ETL']
) as dag:
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/police_reports && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/police_reports && dbt test'
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/police_reports && dbt snapshot'
    )
    dbt_run >> dbt_test >> dbt_snapshot