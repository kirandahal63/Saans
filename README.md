# Saans 
### Real-Time Air Quality & Health Advisory Platform

> A multi-cloud, production-shaped data
> platform that monitors air quality across the Kathmandu Valley in
> real time, forecasts it an hour ahead with a trained ML model, and
> answers residents' health questions through a Claude-powered chatbot
> grounded in live sensor data.

---

## Overview

Kathmandu Valley is home to nearly **3 million people** who currently
have no easy way to get real-time, personalized, forward-looking
guidance about air quality and its health effects. **Saans** solves
this by building a genuine end-to-end lakehouse pipeline — not a
simulated prototype — that ingests live air quality data, streams it
into an operational database, archives it into a cloud data lake and
warehouse, trains a forecasting model on a real Spark cluster, and
serves everything through a live dashboard and an AI health assistant.

## Key Features

- **Live monitoring** — polls real air quality data every 10
  minutes from the Open-Meteo API across multiple Kathmandu Valley
  locations
-  **Real-time dashboard** — updates instantly via Supabase Realtime,
  no page refresh or polling needed
- **1-hour AQI forecasting** — a Spark GBTRegressor model trained on
  Databricks predicts short-term air quality trends
- **AI health chatbot** — a Retrieval-Augmented Generation (RAG)
  assistant powered by Claude, grounded in both a curated health
  knowledge base *and* the current live reading (e.g. "Is it safe to
  jog in Patan right now?")
-  **Real three-tier lakehouse** — Supabase (hot layer) → Azure Blob
  Storage (lake) → Snowflake (warehouse), fully orchestrated
- **End-to-end orchestration** — three Apache Airflow DAGs
  coordinate the daily rollup, lakehouse export, and Databricks
  forecast trigger across three independent cloud platforms

## Architecture

```
                     Open-Meteo APIs (live)
                              |  polled every 10 min
                              v
                    Redpanda locally
                              |
                              v
                     Stream processor (Python)
                     rolling window + spike detection
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
                                   |
                                   v  external stage + COPY INTO
                     Snowflake (data warehouse)
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

Apache Airflow conducts all three DAGs: the daily rollup, the lakehouse export (Supabase → Blob → Snowflake), and the Databricks forecast trigger.

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | Open-Meteo API |
| Streaming | Apache Kafka (Redpanda) |
| Stream processing | Python |
| Operational database | Supabase (Postgres + pgvector + Realtime) |
| Data lake | Azure Blob Storage |
| Data warehouse | Snowflake |
| Distributed processing | Databricks (Apache Spark, PySpark) |
| Orchestration | Apache Airflow |
| Backend API | FastAPI |
| AI / LLM | Claude API (RAG chatbot) |
| Frontend | HTML/JS + Supabase Realtime |
| Containerization | Docker & Docker Compose |

## 📁Project Structure

```
saans-air-quality/
├── docker-compose.yml
├── .env.example
├── sql/supabase_setup.sql          # run once in Supabase SQL Editor
├── snowflake/setup.sql             # run once in a Snowflake worksheet
├── databricks/forecast_job.py      # import into Databricks, schedule as a Job
├── scripts/seed_embeddings.py      # run once, locally, to seed the chatbot's KB
├── producer/                       # Kafka producer (Open-Meteo -> Kafka)
├── processor/                      # stream processor (Kafka -> Supabase)
├── api/                            # FastAPI + RAG + Claude chatbot
│   ├── main.py
│   ├── rag.py                      # pgvector search
│   ├── embeddings.py               # fastembed model
│   └── knowledge_base.md
├── airflow/
│   ├── Dockerfile                  # custom image w/ Azure+Snowflake SDKs
│   └── dags/
│       ├── daily_aggregation_dag.py
│       ├── lakehouse_export_dag.py     # Supabase -> Azure Blob -> Snowflake
│       └── databricks_forecast_dag.py  # triggers the Databricks job
└── dashboard/index.html            # live front end, Supabase Realtime
```

##  Getting Started

```bash
# 1. Configure environment
cp .env.example .env
# fill in credentials for Supabase, Azure, Snowflake, Databricks, and your LLM key

# 2. Seed the chatbot's knowledge base (one-time)
pip install -r api/requirements.txt
export SUPABASE_DB_URL="<paste from .env>"
python scripts/seed_embeddings.py

# 3. Run the full stack
docker compose --env-file .env up --build
```

Once running:
- **Dashboard:** http://localhost:8000
- **API docs:** http://localhost:8000/docs
- **Airflow UI:** http://localhost:8081

