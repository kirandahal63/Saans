"""
Saans - daily aggregation DAG.

Runs once a day and:
  1. checks that every tracked location reported data recently
     (a basic data-quality gate -- if the producer dies, you find out here)
  2. rolls up yesterday's readings into daily_summary, one row per location

This is deliberately written with plain psycopg2 in a PythonOperator
rather than the Postgres provider hooks, so it has no extra Airflow
provider packages to install -- copy this file into the Airflow image's
dags/ folder (docker-compose already mounts it) and it just runs.
"""

import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

DATABASE_URL = os.environ["SUPABASE_DB_URL"]


def check_data_freshness(**context):
    """Fail loudly if any location has gone quiet -- this is your
    pipeline's smoke alarm."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT location, MAX(recorded_at) AS last_seen
        FROM readings
        GROUP BY location
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    stale = []
    cutoff = datetime.utcnow() - timedelta(hours=2)
    for location, last_seen in rows:
        if last_seen is None or last_seen.replace(tzinfo=None) < cutoff:
            stale.append(location)

    if stale:
        raise RuntimeError(f"No recent data for: {', '.join(stale)} -- check the producer/processor")
    print(f"Freshness check passed for {len(rows)} locations")


def build_daily_summary(**context):
    """Aggregate the previous day's readings into daily_summary."""
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO daily_summary (location, day, avg_aqi, max_aqi, min_aqi, reading_count)
        SELECT
            location,
            (recorded_at AT TIME ZONE 'UTC')::date AS day,
            AVG(us_aqi) AS avg_aqi,
            MAX(us_aqi) AS max_aqi,
            MIN(us_aqi) AS min_aqi,
            COUNT(*) AS reading_count
        FROM readings
        WHERE recorded_at >= (CURRENT_DATE - INTERVAL '1 day')
          AND recorded_at < CURRENT_DATE
        GROUP BY location, (recorded_at AT TIME ZONE 'UTC')::date
        ON CONFLICT (location, day) DO UPDATE SET
            avg_aqi = EXCLUDED.avg_aqi,
            max_aqi = EXCLUDED.max_aqi,
            min_aqi = EXCLUDED.min_aqi,
            reading_count = EXCLUDED.reading_count
        """
    )
    print(f"Upserted {cur.rowcount} daily_summary rows")
    cur.close()
    conn.close()


default_args = {
    "owner": "saans",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="saans_daily_aggregation",
    default_args=default_args,
    description="Daily data-quality check and AQI rollup for the Saans pipeline",
    schedule="0 1 * * *",  # 1 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["saans", "air-quality"],
) as dag:

    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    summarize = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    freshness_check >> summarize
