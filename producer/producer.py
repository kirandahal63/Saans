"""
Saans - live air quality producer.

Polls the free Open-Meteo Air Quality + Weather APIs for a set of
Kathmandu Valley locations on a fixed interval, and publishes each
reading as a JSON message to a Kafka (Redpanda) topic.

No API key required for Open-Meteo. This is the "source" stage of the
pipeline: live data in, one message per location per poll.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "air-quality-readings")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "600"))  # 10 minutes by default

# Kathmandu Valley locations to track. Add more freely -- this is the
# only place you need to touch to expand coverage.
LOCATIONS = {
    "patan":      (27.6588, 85.3247),
    "budhanilkantha": (27.7770, 85.3624),
    "hattiban": (27.65071, 85.33173),
    "thamel":     (27.7154, 85.3123),
    "gongabu": (27.7356, 85.3096),
    "bhaktapur":  (27.6710, 85.4298),
    "kirtipur":   (27.6784, 85.2775),
    "boudha":     (27.7215, 85.3620),
}

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_reading(name: str, lat: float, lon: float) -> dict | None:
    """Fetch current AQI + weather for one location. Returns None on failure
    so a single bad request never kills the whole polling loop."""
    try:
        aqi_resp = requests.get(
            AIR_QUALITY_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "pm2_5,pm10,us_aqi",
            },
            timeout=15,
        )
        aqi_resp.raise_for_status()
        aqi_current = aqi_resp.json().get("current", {})

        weather_resp = requests.get(
            WEATHER_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,wind_speed_10m",
            },
            timeout=15,
        )
        weather_resp.raise_for_status()
        weather_current = weather_resp.json().get("current", {})

        return {
            "location": name,
            "lat": lat,
            "lon": lon,
            "pm2_5": aqi_current.get("pm2_5"),
            "pm10": aqi_current.get("pm10"),
            "us_aqi": aqi_current.get("us_aqi"),
            "temperature": weather_current.get("temperature_2m"),
            "wind_speed": weather_current.get("wind_speed_10m"),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
    except requests.RequestException as exc:
        print(f"[producer] fetch failed for {name}: {exc}")
        return None


def connect_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Retry connecting to Kafka -- the broker container is often still
    starting up when this container starts, in docker-compose."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[producer] broker not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka broker after retries")


def main() -> None:
    producer = connect_producer()
    print(f"[producer] connected to {KAFKA_BROKER}, publishing to '{TOPIC}'")
    print(f"[producer] tracking {len(LOCATIONS)} locations, polling every {POLL_SECONDS}s")

    while True:
        for name, (lat, lon) in LOCATIONS.items():
            reading = fetch_reading(name, lat, lon)
            if reading is not None:
                producer.send(TOPIC, key=name, value=reading)
                print(f"[producer] sent {name}: AQI={reading['us_aqi']} PM2.5={reading['pm2_5']}")
        producer.flush()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
