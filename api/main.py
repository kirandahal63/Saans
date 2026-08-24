"""
Saans - API layer (v2: Supabase + real LLM).

Serves live/historical data and forecasts out of Supabase, and a
retrieval-grounded chatbot that now calls Claude for the actual answer
generation, using pgvector-retrieved guidance + the live reading as
context. This is the "front door" of the pipeline -- everything
upstream (Kafka, the processor, Airflow, Databricks, Snowflake) exists
to keep the tables this API reads from up to date.
"""

import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import llm
import rag

DATABASE_URL = os.environ["SUPABASE_DB_URL"]
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

app = FastAPI(title="Saans Air Quality API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/api/config")
def config():
    """Public config for the dashboard's Supabase Realtime subscription.
    Only the anon key is exposed here -- it's designed to be public and
    is restricted by the row-level-security policies in supabase_setup.sql."""
    return {"supabase_url": SUPABASE_URL, "supabase_anon_key": SUPABASE_ANON_KEY}


@app.get("/api/locations")
def list_locations():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT location, lat, lon FROM readings ORDER BY location")
        return cur.fetchall()


@app.get("/api/latest")
def latest_readings():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (location)
                location, lat, lon, pm2_5, pm10, us_aqi, aqi_category,
                temperature, wind_speed, recorded_at
            FROM readings
            ORDER BY location, recorded_at DESC
            """
        )
        return cur.fetchall()


@app.get("/api/history/{location}")
def location_history(location: str, hours: int = 24):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT recorded_at, us_aqi, pm2_5, pm10
            FROM readings
            WHERE location = %s AND recorded_at >= now() - (%s || ' hours')::interval
            ORDER BY recorded_at ASC
            """,
            (location, hours),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for '{location}' in the last {hours}h")
    return rows


@app.get("/api/alerts")
def recent_alerts(limit: int = 20):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT location, us_aqi, level, message, created_at FROM alerts ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


@app.get("/api/daily-summary/{location}")
def daily_summary(location: str, days: int = 14):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT day, avg_aqi, max_aqi, min_aqi
            FROM daily_summary WHERE location = %s ORDER BY day DESC LIMIT %s
            """,
            (location, days),
        )
        return cur.fetchall()


@app.get("/api/forecast/{location}")
def forecast(location: str):
    """Latest AQI forecast produced by the Databricks job, if any."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT predicted_for, predicted_aqi, model_version, created_at
            FROM aqi_predictions
            WHERE location = %s
            ORDER BY predicted_for DESC LIMIT 1
            """,
            (location,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"No forecast yet for '{location}'")
    return row


class ChatRequest(BaseModel):
    question: str
    location: str | None = None


SYSTEM_PROMPT = """You are Saans Health Advisor, a helpful and friendly AI assistant for Kathmandu Valley air quality.
You should use the provided CONTEXT (which includes real-time readings and health guidelines) to answer specific questions about current air quality and health guidance.
Feel free to engage in general friendly conversation (greetings, identity) or answer general AQI knowledge questions (such as what the maximum AQI is) using your general knowledge, but keep responses focused, helpful, and concise (2-4 sentences).
When answering questions about whether it is safe for someone with a health condition (like asthma or respiratory issues) to go outside:
- Remind them that asthmatics and sensitive groups should limit/avoid outdoor exertion if the AQI is above 100.
- Compare the current AQI of the locations in the context, and suggest a cleaner/better location to go to if one is available and significantly safer.
- Always include a friendly, conversational touch while reminding them you are not a doctor and they should speak with a healthcare provider for medical concerns."""


@app.post("/api/chat")
def chat(req: ChatRequest):
    """Retrieval-grounded chatbot: pgvector search over the health-guidance
    knowledge base + the live readings, sent to the LLM as context. Falls back
    to a plain templated answer if no LLM provider is configured."""
    matches = rag.search(req.question, top_k=3)

    all_readings = []
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (location)
                    location, us_aqi, aqi_category, recorded_at
                FROM readings
                ORDER BY location, recorded_at DESC
                """
            )
            all_readings = cur.fetchall()
    except Exception as e:
        pass

    reading = None
    if req.location:
        for r in all_readings:
            if r['location'].lower() == req.location.lower():
                reading = r
                break

    context_lines = []
    if all_readings:
        context_lines.append("Current AQI readings across Kathmandu Valley:")
        for r in all_readings:
            context_lines.append(
                f"- {r['location']}: AQI is {r['us_aqi']} ({r['aqi_category'].replace('_', ' ')}), recorded at {r['recorded_at']}."
            )
        context_lines.append("")

    for m in matches:
        context_lines.append(f"{m['heading']}: {m['text']}")
    context = "\n".join(context_lines) if context_lines else "No relevant data found."

    if llm.is_configured():
        answer = llm.generate_answer(
            SYSTEM_PROMPT, f"CONTEXT:\n{context}\n\nQUESTION: {req.question}"
        )
    else:
        # No LLM key configured yet -- still functional, just less fluent.
        answer = " ".join(context_lines) or "I don't have enough data to answer that yet."

    return {
        "answer": answer,
        "sources": [m["heading"] for m in matches],
        "grounded_reading": reading,
        "llm_used": llm.is_configured(),
        "llm_provider": llm.LLM_PROVIDER,
    }


dashboard_dir = "/dashboard" if os.path.isdir("/dashboard") else os.path.join(
    os.path.dirname(__file__), "..", "dashboard"
)
if os.path.isdir(dashboard_dir):
    app.mount("/dashboard", StaticFiles(directory=dashboard_dir), name="dashboard")

    @app.get("/")
    def root():
        return FileResponse(os.path.join(dashboard_dir, "index.html"))
