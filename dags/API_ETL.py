from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
from sodapy import Socrata
import os

# File path to store the last_known_date as a fallback
LAST_KNOWN_DATE_FILE = "/opt/airflow/last_known_date.txt"
@task
def get_last_known_date_from_snowflake():
    """
    Retrieve the most recent 'incident_datetime' from the Snowflake table.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    SNOWFLAKE_TABLE = "incidents"
    
    # Ensure the query is correct
    query = f'SELECT MAX("incident_datetime") FROM {SNOWFLAKE_TABLE};'
    print("Executing query:", query)
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result[0]:
        # result[0] is already a datetime object
        next_last_known_date = result[0] + timedelta(minutes=1)
        return next_last_known_date.strftime('%Y-%m-%dT%H:%M:%S.000')
    else:
        # Return default date in the correct format
        return "2024-12-01T00:00:00.000"


@task
def fetch_and_preprocess_data(last_known_date: str):
    """
    Fetch data from Socrata API, filter required columns, and preprocess.
    """

    client = Socrata("data.sfgov.org", None)
    query = f"incident_datetime > '{last_known_date}'"
    results = client.get("wg3w-h783", where=query, limit=10000)

    if not results:
            # Log a message and raise a Skip exception if no data is fetched
            print(f"No new records found for last_known_date: {last_known_date}. DAG will skip further tasks.")
            return None  # Return None to signal no new data

    df = pd.DataFrame.from_records(results)
    
    # Log returned columns for debugging
    print("Columns in DataFrame:", df.columns.tolist())

    # Define required columns
    required_columns = [
        "row_id", "incident_id", "incident_datetime", "incident_day_of_week", "report_datetime",
        "incident_category", "incident_subcategory",
        "resolution", "intersection", "police_district", "analysis_neighborhood",
        "latitude", "longitude"
    ]

    # Select only the columns that exist in the DataFrame
    existing_columns = [col for col in required_columns if col in df.columns]
    df = df[existing_columns]
    print("Filtered Columns in DataFrame:", df.columns.tolist())

    # Preprocess the data
    if "incident_datetime" in df.columns:
        df["incident_datetime"] = pd.to_datetime(df["incident_datetime"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    if "report_datetime" in df.columns:
        df["report_datetime"] = pd.to_datetime(df["report_datetime"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Remove rows with empty or invalid values
    drop_columns = ["analysis_neighborhood", "incident_category", "incident_subcategory", "incident_datetime", "incident_id"]
    for col in drop_columns:
        if col in df.columns:
            df.dropna(subset=[col], inplace=True)

    # Convert latitude and longitude to strings
    if "latitude" in df.columns:
        df["latitude"] = df["latitude"].astype(str)
    if "longitude" in df.columns:
        df["longitude"] = df["longitude"].astype(str)

    return df.to_dict(orient='records')


@task
def insert_to_snowflake(data: list):

    if not data:
        # Skip insertion if there's no data
        print("No new data to insert into Snowflake.")
        return
    """
    Insert the preprocessed data into the Snowflake table using SnowflakeHook.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    SNOWFLAKE_TABLE = "incidents"

    for row in data:
        hook.run(f"""
            INSERT INTO {SNOWFLAKE_TABLE} (
                "row_id", "incident_id", "incident_datetime", "incident_day_of_week",
                "report_datetime", "incident_category", "incident_subcategory",
                "resolution", "intersection", "police_district", "analysis_neighborhood",
                "latitude", "longitude"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, parameters=[
            row["row_id"], row["incident_id"], row["incident_datetime"], row["incident_day_of_week"],
            row["report_datetime"], row["incident_category"], row["incident_subcategory"],
            row["resolution"], row["intersection"], row["police_district"], row["analysis_neighborhood"],
            row["latitude"], row["longitude"]
        ])


@task
def update_last_known_date(data: list):
    """
    Update the last known date dynamically in the file.
    """
    if not data:
        print("No data processed, skipping update of last_known_date.")
        return
    
    df = pd.DataFrame(data)
    latest_processed_date = pd.to_datetime(df['incident_datetime']).max()
    next_last_known_date = (latest_processed_date + timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%S.000')
    with open(LAST_KNOWN_DATE_FILE, 'w') as file:
        file.write(next_last_known_date)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'socrata_to_snowflake_decorator',
    default_args=default_args,
    description='Fetch data from Socrata API and insert into Snowflake using SnowflakeHook and Task Decorators',
    schedule_interval='@weekly',
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Task dependencies
    last_known_date = get_last_known_date_from_snowflake()
    raw_data = fetch_and_preprocess_data(last_known_date)
    inserted_data = insert_to_snowflake(raw_data)
    update_last_known_date(raw_data)
