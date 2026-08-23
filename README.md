# Saans — real-time air quality & health advisory platform (v2)

**सांस** (breath) — a multi-cloud, production-shaped data platform that
tracks air quality across Kathmandu Valley in real time, forecasts it an
hour ahead with a trained ML model, and answers health questions through
a real Claude-powered chatbot grounded in live data.

This version replaces every "prototype stand-in" from the first build
with the real service: **Supabase** (managed Postgres + pgvector +
Realtime) instead of self-hosted Postgres, **Azure Blob Storage**
instead of AWS S3, **Databricks** (real Spark cluster) instead of a
Python script pretending to be Spark, **Snowflake** as a real cloud
warehouse, and **Claude** as a real LLM instead of a template.

## Architecture

```
                     Open-Meteo APIs (live, no key)
                              |  polled every 10 min
                              v
                    Kafka topic (Redpanda, local
                    -- or Azure Event Hub for the
                    fully-cloud demo)
                              |
                              v
                     Stream processor (Python)
                     rolling window + spike detect
                              |
                              v
              +---------- Supabase (Postgres) ----------+
              |  readings . alerts . daily_summary .     |
              |  aqi_predictions . guidance_embeddings   |
              |  (pgvector) . Realtime -> live dashboard |
              +--------------------+---------------------+
                                   |  nightly, via Airflow
                                   v
                     Azure Blob Storage (data lake)
                     readings/YYYY-MM-DD.json
                                   |
                                   v  Snowflake external stage + COPY INTO
                     Snowflake (warehouse)
                     readings_archive . daily_summary_archive .
                     aqi_predictions_archive
                                   |
                                   v  triggered by Airflow
                     Databricks (real Spark cluster)
                     PySpark feature engineering + GBTRegressor
                     forecast model -> writes back to Snowflake
                                   |
                                   v
                     FastAPI (/api/*) -- Claude API (RAG chat,
                              |          grounded on pgvector + live data)
                              v
                     Live dashboard (Supabase Realtime, no polling)
```

Airflow is the conductor across all three DAGs: daily rollup,
lakehouse export (Supabase -> Blob -> Snowflake), and the Databricks
trigger.

## What maps to every chapter you studied

| Chapter | Where it is now |
|---|---|
| Local analytics engine | Supabase Postgres (`sql/supabase_setup.sql`) |
| ETL/ELT | `producer/producer.py` (extract+transform), `airflow/dags/lakehouse_export_dag.py` (load) |
| Data Lakes & Lakehouse | Azure Blob (`saans-lake` container) as the lake, Supabase as the hot operational layer, Snowflake as the warehouse -- a real three-tier lakehouse |
| Distributed processing (Spark/Databricks) | `databricks/forecast_job.py`, running on an actual Databricks cluster |
| Snowflake / virtual warehouses / Snowpipe | `snowflake/setup.sql` -- real warehouse, real external stage on Azure Blob, Snowpipe defined (commented, needs Event Grid -- see file) |
| Orchestration (Airflow / DAGs) | Three real DAGs in `airflow/dags/`, one triggering a different cloud service each |
| Streaming (Kafka) | Redpanda (Kafka-compatible) locally; swap to Azure Event Hub for the cloud demo (see below) |
| AI-ready data / vector infrastructure | `guidance_embeddings` table in Supabase using **pgvector** -- a real vector database, not a stand-in |
| Docker | Every service containerized; custom Airflow image with Azure/Snowflake SDKs baked in |
| FastAPI | `api/main.py`, now also fronting a real LLM |

## Setup guide -- do this once, in this order

### 1. Supabase (10 min)
1. Go to `supabase.com` -> New project. Pick a strong database password and save it.
2. Once provisioned: **SQL Editor** -> New query -> paste the contents of `sql/supabase_setup.sql` -> Run.
3. **Settings -> Database -> Connection string -> URI** -> copy it -> this is `SUPABASE_DB_URL` (replace `[YOUR-PASSWORD]` with the password from step 1).
4. **Settings -> API** -> copy the **Project URL** (`SUPABASE_URL`) and the **anon public** key (`SUPABASE_ANON_KEY`).

### 2. A free LLM key for the chatbot (2 min, no credit card)
Pick one -- both are genuinely free with no card required:
- **Google AI Studio (recommended):** `aistudio.google.com` -> sign in with any Google account -> **Get API key** -> **Create API key in new project** -> this is `GOOGLE_API_KEY`. Leave `LLM_PROVIDER=gemini` (the default).
- **Groq (fast alternative):** `console.groq.com/keys` -> sign up with just an email -> Create API Key -> this is `GROQ_API_KEY`. Set `LLM_PROVIDER=groq`.

If you later get access to a funded Anthropic key, set `LLM_PROVIDER=anthropic` and `ANTHROPIC_API_KEY` -- the code already supports it, no changes needed.

### 3. Azure Storage -- your education plan (10 min)
1. Azure Portal (`portal.azure.com`, sign in with your education account) -> **Storage accounts** -> Create.
2. Any region close to you, Standard performance, LRS redundancy (cheapest) is fine.
3. Once created: **Access keys** -> copy a **Connection string** -> this is `AZURE_STORAGE_CONNECTION_STRING`.
4. Set `AZURE_CONTAINER_NAME=saans-lake` -- the Airflow DAG creates the container automatically on first run.

### 4. Snowflake (15 min)
1. Sign up for the free trial at `signup.snowflake.com` (choose Azure as the underlying cloud -- it'll matter for cross-cloud latency but not for functionality).
2. Open a Snowflake worksheet -> paste `snowflake/setup.sql` -> **before running**, fill in:
   - `YOUR_STORAGE_ACCOUNT` and `YOUR_CONTAINER` in the `create stage` block, matching your Azure Storage account and `saans-lake` container.
   - `YOUR_SAS_TOKEN`: Azure Portal -> your storage account -> **Shared access signature** -> check **Read** + **List** permissions -> Generate SAS -> copy the token (the part after `?`).
3. Run the whole script.
4. Your account identifier (for `SNOWFLAKE_ACCOUNT`) is the first part of your Snowflake URL, e.g. `abc12345.east-us-2.azure` from `https://abc12345.east-us-2.azure.snowflakecomputing.com`.

### 5. Databricks (15 min)
1. If your Azure education plan includes Azure Databricks: Azure Portal -> Create resource -> Databricks -> standard tier. Otherwise use the free Databricks Community Edition at `community.cloud.databricks.com`.
2. Create a small cluster (Personal Compute / single-node is enough).
3. **Workspace -> Import** -> upload `databricks/forecast_job.py` (Databricks auto-detects the notebook format from the `# Databricks notebook source` header).
4. Set your Snowflake credentials as cluster environment variables (**Compute -> your cluster -> Edit -> Advanced options -> Environment variables**): `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.
5. Attach the notebook to the cluster, **Run All** once manually to confirm it works end to end.
6. **Workflows -> Create Job** -> Task type "Notebook" -> point at this notebook -> note the numeric **Job ID** shown in the job's URL -- this is `DATABRICKS_JOB_ID`.
7. **Settings -> Developer -> Access tokens -> Generate new token** -- this is `DATABRICKS_TOKEN`. `DATABRICKS_HOST` is your workspace URL (e.g. `https://adb-xxxx.azuredatabricks.net`).

### 6. Put it all together
```bash
cp .env.example .env
# fill in every value from steps 1-5 above
```

### 7. Seed the chatbot's knowledge base (one-time, local machine)
```bash
pip install -r api/requirements.txt
export SUPABASE_DB_URL="<paste from .env>"
python scripts/seed_embeddings.py
```
This downloads a small embedding model (~130MB, one-time) and loads 14 guidance chunks into Supabase's `guidance_embeddings` table.

### 8. Run the stack
```bash
docker compose --env-file .env up --build
```
- **Dashboard:** http://localhost:8000
- **API docs:** http://localhost:8000/docs
- **Airflow UI:** http://localhost:8081 (admin password printed in `docker compose logs airflow`)

Trigger each Airflow DAG manually the first time (the run button in the UI) rather than waiting for its schedule, so you can confirm the whole chain works: `saans_lakehouse_export` then `saans_databricks_forecast`.

## Going fully cloud for the live demo (optional but impressive)

Swap Redpanda for **Azure Event Hubs** (Kafka-compatible -- same
`kafka-python` code, just different connection settings):
1. Azure Portal -> Event Hubs namespace -> Standard tier -> create an event hub named `air-quality-readings`.
2. **Shared access policies** -> get the connection string.
3. Set these in `.env` and point `producer`/`processor` at Event Hubs instead of `redpanda:9092`:
   ```
   KAFKA_BROKER=<namespace>.servicebus.windows.net:9092
   KAFKA_SASL_USERNAME=$ConnectionString
   KAFKA_SASL_PASSWORD=<your full Event Hubs connection string>
   ```
4. Update `KafkaProducer`/`KafkaConsumer` calls in `producer.py`/`consumer.py` to add `security_protocol="SASL_SSL", sasl_mechanism="PLAIN", sasl_plain_username="$ConnectionString", sasl_plain_password=<connection string>`.

This is genuinely "no infrastructure I manage" for the streaming layer -- a strong point to make to judges about production-readiness.

## Verifying each stage independently

- **Producer:** `docker compose logs -f producer` -- a `sent:` line per location per poll.
- **Processor -> Supabase:** Supabase dashboard -> Table Editor -> `readings` -- new rows appearing.
- **Realtime working:** open the dashboard, watch a card update *without* refreshing the page when a new reading lands.
- **Lakehouse export:** Airflow UI -> trigger `saans_lakehouse_export` -> check Azure Portal -> Storage browser -> `saans-lake` container for the new JSON file -> Snowflake worksheet -> `select count(*) from readings_archive;`
- **Databricks forecast:** Airflow UI -> trigger `saans_databricks_forecast` -> Databricks -> Workflows -> check the run succeeded -> Snowflake -> `select * from aqi_predictions_archive order by loaded_at desc limit 5;`
- **Chatbot:** dashboard chat panel -> ask "is it safe to jog in Patan" -> response should reference the live AQI number, meaning it's actually grounded, not generic.

## Project structure

```
saans-air-quality/
|-- docker-compose.yml
|-- .env.example
|-- sql/supabase_setup.sql          # run once in Supabase SQL Editor
|-- snowflake/setup.sql             # run once in a Snowflake worksheet
|-- databricks/forecast_job.py      # import into Databricks, schedule as a Job
|-- scripts/seed_embeddings.py      # run once, locally, to seed the chatbot's KB
|-- producer/                       # Kafka producer (Open-Meteo -> Kafka)
|-- processor/                      # stream processor (Kafka -> Supabase)
|-- api/                            # FastAPI + RAG + Claude chatbot
|   |-- main.py
|   |-- rag.py                      # pgvector search
|   |-- embeddings.py               # fastembed model
|   `-- knowledge_base.md
|-- airflow/
|   |-- Dockerfile                  # custom image w/ Azure+Snowflake SDKs
|   `-- dags/
|       |-- daily_aggregation_dag.py
|       |-- lakehouse_export_dag.py     # Supabase -> Azure Blob -> Snowflake
|       `-- databricks_forecast_dag.py  # triggers the Databricks job
`-- dashboard/index.html            # live front end, Supabase Realtime
```

## For the showcase

- Have the dashboard open, visibly updating **via Realtime, not a refresh button** -- that's the moment to pause and let it land.
- Show the Airflow UI mid-DAG-run: three services (Azure, Snowflake, Databricks) being orchestrated from one place is the single hardest thing to fake, and the most convincing "this is real engineering" evidence you have.
- Pull up Snowflake and run `select * from aqi_predictions_archive` live -- a trained model's actual output, not a mocked number.
- Ask the chatbot a real question live and let it answer using the current reading.
- Close on the framing: three independent cloud platforms, a trained forecasting model, and a real LLM, all orchestrated into one pipeline solving a genuine public-health gap for a place with almost 3 million people.
