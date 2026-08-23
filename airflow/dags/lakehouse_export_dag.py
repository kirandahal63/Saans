"""
Saans - lakehouse export DAG.

This is the DAG that ties Supabase, Azure, and Snowflake together into
one lakehouse pattern:

  1. Extract yesterday's readings from Supabase (the fast operational store)
  2. Land them as a JSON file in Azure Blob Storage (the data lake --
     cheap, durable, replaces the AWS S3 bucket from the original design)
  3. Trigger a Snowflake COPY INTO that loads the new file from the Azure
     Blob external stage into the warehouse (the analytics layer for
     full history, at scale)

Hot path (Supabase) stays fast and small. Cold path (Blob -> Snowflake)
accumulates full history for analytics and for the Databricks forecasting
job to train on.
"""

import json
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient

DATABASE_URL = os.environ["SUPABASE_DB_URL"]
AZURE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER_NAME", "saans-lake")

SNOWFLAKE_CONFIG = dict(
    account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    user=os.environ.get("SNOWFLAKE_USER", ""),
    password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
    warehouse="SAANS_WH",
    database="SAANS",
    schema="CORE",
)


def export_to_blob(**context):
    """Pull yesterday's readings out of Supabase and land them as one
    JSON file in Azure Blob Storage."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT location, lat, lon, pm2_5, pm10, us_aqi, aqi_category,
                   temperature, wind_speed, recorded_at
            FROM readings
            WHERE recorded_at >= CURRENT_DATE - INTERVAL '1 day'
              AND recorded_at < CURRENT_DATE
            """
        )
        rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No rows to export for yesterday -- skipping blob upload")
        context["ti"].xcom_push(key="blob_name", value=None)
        return

    payload = json.dumps(rows, default=str)
    day_str = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    blob_name = f"readings/{day_str}.json"

    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(AZURE_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass  # already exists

    container_client.upload_blob(name=blob_name, data=payload, overwrite=True)
    print(f"Uploaded {len(rows)} rows to azure://{AZURE_CONTAINER}/{blob_name}")
    context["ti"].xcom_push(key="blob_name", value=blob_name)


def load_into_snowflake(**context):
    """Trigger the COPY INTO that loads the file just landed in Blob."""
    blob_name = context["ti"].xcom_pull(key="blob_name", task_ids="export_to_blob")
    if blob_name is None:
        print("Nothing was exported -- skipping Snowflake load")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            COPY INTO readings_archive
            FROM @saans_azure_stage
            FILE_FORMAT = saans_json_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = %s
            ON_ERROR = 'CONTINUE'
            """,
            (f".*{blob_name.split('/')[-1]}$",),
        )
        result = cur.fetchall()
        print(f"Snowflake COPY INTO result: {result}")
    finally:
        cur.close()
        conn.close()


default_args = {"owner": "saans", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="saans_lakehouse_export",
    default_args=default_args,
    description="Export Supabase readings to Azure Blob, load into Snowflake",
    schedule="30 1 * * *",  # after the daily_aggregation DAG
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["saans", "lakehouse", "azure", "snowflake"],
) as dag:

    export_task = PythonOperator(task_id="export_to_blob", python_callable=export_to_blob)
    load_task = PythonOperator(task_id="load_into_snowflake", python_callable=load_into_snowflake)

    export_task >> load_task
