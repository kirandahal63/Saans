"""
Saans - Databricks trigger DAG.

Calls the Databricks Jobs API to run the PySpark forecasting job
(databricks/forecast_job.py) on a schedule, after the lakehouse export
has landed fresh data in Snowflake for it to train on.

Requires a Databricks Job already created in your workspace (Workflows
-> Create Job -> Notebook task -> point it at the imported
forecast_job.py notebook). Put that job's numeric ID in DATABRICKS_JOB_ID.
"""

import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]  # e.g. https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_JOB_ID = os.environ["DATABRICKS_JOB_ID"]

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def trigger_and_wait(**context):
    run_resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=HEADERS,
        json={"job_id": int(DATABRICKS_JOB_ID)},
        timeout=30,
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]
    print(f"Triggered Databricks run {run_id}")

    # Poll until the run finishes -- forecasting job is small, so this
    # is simpler than wiring up a separate sensor operator.
    for _ in range(60):  # up to ~30 minutes
        status_resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
            timeout=30,
        )
        status_resp.raise_for_status()
        state = status_resp.json()["state"]
        life_cycle_state = state.get("life_cycle_state")
        print(f"Run {run_id} state: {life_cycle_state}")

        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result_state = state.get("result_state")
            if result_state != "SUCCESS":
                raise RuntimeError(f"Databricks run {run_id} finished with {result_state}: {state}")
            print(f"Databricks run {run_id} succeeded")
            return
        time.sleep(30)

    raise TimeoutError(f"Databricks run {run_id} did not finish within the wait window")


default_args = {"owner": "saans", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="saans_databricks_forecast",
    default_args=default_args,
    description="Trigger the Databricks PySpark AQI forecasting job",
    schedule="0 2 * * *",  # after the lakehouse export
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["saans", "databricks", "ml"],
) as dag:

    run_forecast_job = PythonOperator(
        task_id="trigger_and_wait_for_databricks",
        python_callable=trigger_and_wait,
    )
