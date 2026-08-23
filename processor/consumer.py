"""
Saans - stream processor.

Consumes raw readings from Kafka, computes the AQI category, keeps a
rolling per-location window in memory to detect sudden pollution spikes,
writes every reading to Postgres, and raises an alert row when a spike
or hazardous reading is detected.

This plays the role Flink would play in the full architecture: real-time
windowed computation over a live stream. It's implemented here in plain
Python so the whole project runs without a JVM/Flink cluster. Swapping
this file for a PyFlink job later is a natural "grow beyond the booklet"
step once this version is working end to end.
"""

import json
import os
import time
from collections import defaultdict, deque

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "air-quality-readings")

DATABASE_URL = os.environ["SUPABASE_DB_URL"]  # e.g. postgresql://postgres:[pwd]@[host]:5432/postgres

# Rolling window size (readings) per location, used to detect spikes.
WINDOW_SIZE = 6
# A spike is flagged when the latest AQI jumps this many points above
# the rolling average of the window.
SPIKE_THRESHOLD = 40

rolling_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


def aqi_category(aqi: int | None) -> str:
    """US AQI breakpoints -> category label."""
    if aqi is None:
        return "unknown"
    if aqi <= 50:
        return "good"
    if aqi <= 100:
        return "moderate"
    if aqi <= 150:
        return "unhealthy_sensitive"
    if aqi <= 200:
        return "unhealthy"
    if aqi <= 300:
        return "very_unhealthy"
    return "hazardous"


def connect_db(retries: int = 10, delay: int = 5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as exc:
            print(f"[processor] supabase not reachable (attempt {attempt}/{retries}): {exc}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Supabase Postgres after retries")


def connect_kafka(retries: int = 10, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="saans-processor",
            )
        except NoBrokersAvailable:
            print(f"[processor] broker not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka broker after retries")


def insert_reading(conn, reading: dict, category: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO readings
                (location, lat, lon, pm2_5, pm10, us_aqi, aqi_category,
                 temperature, wind_speed, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                reading["location"], reading["lat"], reading["lon"],
                reading.get("pm2_5"), reading.get("pm10"), reading.get("us_aqi"),
                category, reading.get("temperature"), reading.get("wind_speed"),
                reading["recorded_at"],
            ),
        )


def insert_alert(conn, location: str, aqi: int, level: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alerts (location, us_aqi, level, message)
            VALUES (%s, %s, %s, %s)
            """,
            (location, aqi, level, message),
        )
    print(f"[processor] ALERT [{level}] {location}: {message}")


def process_reading(conn, reading: dict) -> None:
    location = reading["location"]
    aqi = reading.get("us_aqi")
    category = aqi_category(aqi)

    insert_reading(conn, reading, category)

    if aqi is None:
        return

    window = rolling_windows[location]
    if len(window) >= 3:
        rolling_avg = sum(window) / len(window)
        if aqi - rolling_avg >= SPIKE_THRESHOLD:
            insert_alert(
                conn, location, aqi, "SPIKE",
                f"AQI jumped to {aqi} (rolling avg was {rolling_avg:.0f}) in {location}",
            )

    if category in ("very_unhealthy", "hazardous"):
        insert_alert(
            conn, location, aqi, category.upper(),
            f"AQI is {aqi} ({category.replace('_', ' ')}) in {location} -- avoid outdoor activity",
        )

    window.append(aqi)


def main() -> None:
    conn = connect_db()
    consumer = connect_kafka()
    print(f"[processor] listening on '{TOPIC}', writing to Supabase Postgres")

    for message in consumer:
        try:
            process_reading(conn, message.value)
        except Exception as exc:  # keep the loop alive on a single bad message
            print(f"[processor] failed to process message: {exc}")


if __name__ == "__main__":
    main()
