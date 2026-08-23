-- Run this once in your Supabase project's SQL Editor
-- (Dashboard -> SQL Editor -> New query -> paste -> Run).
-- This replaces the self-hosted Postgres from the prototype: Supabase
-- IS Postgres, plus it gives us pgvector (for the RAG chatbot) and
-- Realtime (so the dashboard gets pushed updates instead of polling)
-- for free.

-- 1. Enable the pgvector extension for embedding storage/search --------
create extension if not exists vector;

-- 2. Core operational tables (same shape as the prototype) -------------
create table if not exists readings (
    id           bigserial primary key,
    location     text not null,
    lat          double precision not null,
    lon          double precision not null,
    pm2_5        double precision,
    pm10         double precision,
    us_aqi       integer,
    aqi_category text,
    temperature  double precision,
    wind_speed   double precision,
    recorded_at  timestamptz not null,
    ingested_at  timestamptz not null default now()
);

create index if not exists idx_readings_location_time
    on readings (location, recorded_at desc);

create table if not exists alerts (
    id          bigserial primary key,
    location    text not null,
    us_aqi      integer not null,
    level       text not null,
    message     text not null,
    created_at  timestamptz not null default now()
);

create index if not exists idx_alerts_location_time
    on alerts (location, created_at desc);

create table if not exists daily_summary (
    id            bigserial primary key,
    location      text not null,
    day           date not null,
    avg_aqi       double precision,
    max_aqi       double precision,
    min_aqi       double precision,
    reading_count integer,
    created_at    timestamptz not null default now(),
    unique (location, day)
);

-- 3. Predictions table, populated by the Databricks forecasting job -----
create table if not exists aqi_predictions (
    id             bigserial primary key,
    location       text not null,
    predicted_for  timestamptz not null,
    predicted_aqi  double precision not null,
    model_version  text,
    created_at     timestamptz not null default now()
);

-- 4. Vector store for the RAG chatbot's health-guidance knowledge base --
-- 384 dimensions matches the BAAI/bge-small-en-v1.5 model used in
-- api/embeddings.py. If you swap embedding models, update this.
create table if not exists guidance_embeddings (
    id        bigserial primary key,
    heading   text not null,
    content   text not null,
    embedding vector(384)
);

-- ivfflat index for fast approximate nearest-neighbour search once you
-- have more than a few hundred rows. Harmless at small scale too.
create index if not exists idx_guidance_embeddings_vector
    on guidance_embeddings using ivfflat (embedding vector_cosine_ops)
    with (lists = 10);

-- 5. Turn on Realtime for the tables the dashboard subscribes to --------
-- This lets the browser get pushed updates the instant the processor
-- inserts a row, instead of polling the API every 30 seconds.
alter publication supabase_realtime add table readings;
alter publication supabase_realtime add table alerts;

-- 6. Row Level Security: allow public read on the tables the dashboard
-- and chatbot need to read directly via the anon key. Writes still go
-- through the processor/Airflow using the service-role key, which
-- bypasses RLS -- never expose the service-role key to the browser.
alter table readings enable row level security;
alter table alerts enable row level security;
alter table daily_summary enable row level security;
alter table aqi_predictions enable row level security;

create policy "public read readings" on readings for select using (true);
create policy "public read alerts" on alerts for select using (true);
create policy "public read daily_summary" on daily_summary for select using (true);
create policy "public read aqi_predictions" on aqi_predictions for select using (true);
