from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from snowflake.connector import connect
import os

def load_to_snowflake():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    
    cursor = conn.cursor()
    cursor.execute("""
        PUT file:///opt/airflow/filtered_data.csv @POLICE_STAGE
    """)
    cursor.execute("""
        COPY INTO incidents
        FROM @POLICE_STAGE/filtered_data.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS')
    """)
    conn.close()

with DAG(
    dag_id='docker_etl_snowflake',
    start_date=datetime(2024, 12, 2),
    catchup=False,
    schedule_interval='@yearly'
) as dag:
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )