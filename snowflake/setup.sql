-- Run this in a Snowflake worksheet (using your trial account).
-- Sets up the warehouse layer that the Airflow lakehouse export DAG
-- and the Databricks forecasting job both write into.

-- 1. Compute + storage containers ----------------------------------------
create warehouse if not exists saans_wh
    warehouse_size = 'x-small'
    auto_suspend = 60          -- suspend after 60s idle: keeps trial credits alive
    auto_resume = true
    initially_suspended = true;

create database if not exists saans;
create schema if not exists saans.core;

use warehouse saans_wh;
use database saans;
use schema core;

-- 2. Tables mirroring the lakehouse layers --------------------------------
-- Raw archive: every reading ever collected, loaded from the Azure Blob
-- landing zone (this is the "cold storage / full history" layer -- the
-- Supabase `readings` table only needs to keep recent data fast).
create table if not exists readings_archive (
    location      string,
    lat           float,
    lon           float,
    pm2_5         float,
    pm10          float,
    us_aqi        integer,
    aqi_category  string,
    temperature   float,
    wind_speed    float,
    recorded_at   timestamp_tz,
    loaded_at     timestamp_tz default current_timestamp()
);

create table if not exists daily_summary_archive (
    location      string,
    day           date,
    avg_aqi       float,
    max_aqi       float,
    min_aqi       float,
    reading_count integer,
    loaded_at     timestamp_tz default current_timestamp()
);

create table if not exists aqi_predictions_archive (
    location       string,
    predicted_for  timestamp_tz,
    predicted_aqi  float,
    model_version  string,
    loaded_at      timestamp_tz default current_timestamp()
);

-- 3. External stage on Azure Blob -----------------------------------------
-- This is what makes Snowflake read directly from your Azure data lake.
-- Replace the placeholders with your own storage account/container, and
-- generate a SAS token in the Azure Portal (Storage Account -> Shared
-- access signature -> check "Read" + "List" permissions -> Generate).
--
-- azure://<storage-account>.blob.core.windows.net/<container>
create or replace file format saans_json_format
    type = 'JSON'
    strip_outer_array = true;

create or replace stage saans_azure_stage
    url = 'azure://YOUR_STORAGE_ACCOUNT.blob.core.windows.net/YOUR_CONTAINER/readings'
    credentials = (azure_sas_token = 'YOUR_SAS_TOKEN')
    file_format = saans_json_format;

-- Quick check that Snowflake can see your landing files:
-- list @saans_azure_stage;

-- 4. Load command the Airflow DAG triggers after each export -------------
-- (kept here as a reference -- the DAG runs the equivalent via
-- snowflake-connector-python, see airflow/dags/lakehouse_export_dag.py)
--
-- copy into readings_archive
--   from @saans_azure_stage
--   file_format = saans_json_format
--   match_by_column_name = case_insensitive
--   on_error = 'continue';

-- 5. Optional: Snowpipe for continuous auto-ingest instead of batch COPY --
-- This is the "true" streaming-into-warehouse pattern from the course.
-- Requires Azure Event Grid configured to notify Snowpipe when new blobs
-- land -- see Snowflake's "Azure Blob Storage + Snowpipe" docs for the
-- Event Grid subscription setup, which is Azure-console work outside SQL.
--
-- create pipe saans_pipe auto_ingest = true as
--   copy into readings_archive
--   from @saans_azure_stage
--   file_format = saans_json_format
--   match_by_column_name = case_insensitive;
