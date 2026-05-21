"""
smart_city/simulator/config.py
Central configuration — reads from .env file.
Copy .env.example → .env and fill in your API keys.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API keys ──────────────────────────────────────────────────
OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
TOMTOM_API_KEY: str      = os.getenv("TOMTOM_API_KEY", "")

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "telemetry":    os.getenv("KAFKA_TOPIC_TELEMETRY",    "vehicle-telemetry"),
    "weather":      os.getenv("KAFKA_TOPIC_WEATHER",      "weather-data"),
    "traffic":      os.getenv("KAFKA_TOPIC_TRAFFIC",      "traffic-events"),
    "road_events":  os.getenv("KAFKA_TOPIC_ROAD_EVENTS",  "road-events"),
    "alerts":       os.getenv("KAFKA_TOPIC_ALERTS",       "alerts"),
}

# ── Simulation ────────────────────────────────────────────────
VEHICLE_COUNT: int       = int(os.getenv("SIM_VEHICLE_COUNT", "5"))
EMIT_INTERVAL: float     = float(os.getenv("SIM_EMIT_INTERVAL_SECONDS", "1"))
DEFAULT_ROUTE: str       = os.getenv("SIM_ROUTE", "tahrir_to_new_capital")

# ── Weather cache ─────────────────────────────────────────────
WEATHER_CACHE_TTL: int   = 300   # seconds (5 min, respects free-tier limit)

# ── Alert thresholds ──────────────────────────────────────────
ALERT_SPEED_KMH: float        = 120.0
ALERT_ENGINE_TEMP_C: float    = 105.0
ALERT_FUEL_PCT: float         = 10.0
ALERT_RPM: int                = 5000
