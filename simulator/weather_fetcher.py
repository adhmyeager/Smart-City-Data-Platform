"""
smart_city/simulator/weather_fetcher.py

Fetches real weather data for Cairo and the New Administrative Capital
from the OpenWeatherMap API (free tier: 1,000 calls/day).

Features:
  - 5-minute in-memory cache (avoids burning API quota)
  - Graceful fallback to Cairo seasonal defaults when API key is missing
  - Returns typed WeatherReading dataclass
  - Thread-safe for multi-vehicle simulation

API docs: https://openweathermap.org/current
Free tier: https://openweathermap.org/price
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, asdict
from typing import Optional

import requests

from config import OPENWEATHER_API_KEY, WEATHER_CACHE_TTL

logger = logging.getLogger(__name__)

# ─── Cairo seasonal defaults (fallback when no API key) ──────
# Averages based on Cairo climate data
_CAIRO_SEASONAL_DEFAULTS = {
    1:  {"temp_c": 14.0, "humidity": 55, "wind_kmh": 10, "condition": "Clear",       "visibility_km": 10},
    2:  {"temp_c": 15.5, "humidity": 50, "wind_kmh": 12, "condition": "Clear",       "visibility_km": 10},
    3:  {"temp_c": 19.0, "humidity": 45, "wind_kmh": 15, "condition": "Haze",        "visibility_km": 7},
    4:  {"temp_c": 24.5, "humidity": 35, "wind_kmh": 14, "condition": "Dust",        "visibility_km": 5},
    5:  {"temp_c": 29.0, "humidity": 28, "wind_kmh": 13, "condition": "Clear",       "visibility_km": 9},
    6:  {"temp_c": 31.5, "humidity": 30, "wind_kmh": 12, "condition": "Clear",       "visibility_km": 10},
    7:  {"temp_c": 32.0, "humidity": 42, "wind_kmh": 11, "condition": "Clear",       "visibility_km": 10},
    8:  {"temp_c": 32.0, "humidity": 44, "wind_kmh": 10, "condition": "Clear",       "visibility_km": 10},
    9:  {"temp_c": 29.5, "humidity": 47, "wind_kmh": 10, "condition": "Haze",        "visibility_km": 8},
    10: {"temp_c": 25.0, "humidity": 52, "wind_kmh": 11, "condition": "Clear",       "visibility_km": 9},
    11: {"temp_c": 20.0, "humidity": 57, "wind_kmh": 12, "condition": "Partly cloudy","visibility_km": 9},
    12: {"temp_c": 15.5, "humidity": 60, "wind_kmh": 11, "condition": "Clear",       "visibility_km": 10},
}

# Conditions that affect vehicle speed (multiplier applied in simulator)
CONDITION_SPEED_FACTOR = {
    "Clear":        1.00,
    "Partly cloudy": 1.00,
    "Clouds":       0.98,
    "Haze":         0.95,
    "Dust":         0.85,
    "Sand":         0.80,
    "Rain":         0.80,
    "Thunderstorm": 0.70,
    "Fog":          0.65,
    "Unknown":      0.95,
}


@dataclass
class WeatherReading:
    location: str
    latitude: float
    longitude: float
    temp_c: float
    feels_like_c: float
    humidity_pct: int
    wind_kmh: float
    wind_direction_deg: int
    condition: str
    description: str
    visibility_km: float
    uv_index: float
    pressure_hpa: int
    timestamp_unix: int
    source: str          # "api" | "cache" | "fallback"

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def speed_factor(self) -> float:
        """Multiplier applied to vehicle speed based on weather."""
        return CONDITION_SPEED_FACTOR.get(self.condition, 0.95)

    @property
    def ac_load_factor(self) -> float:
        """Extra fuel consumption factor from A/C — high in Egyptian summers."""
        if self.temp_c >= 30:
            return 1.5   # Heavy A/C usage: +1.5 L/100km
        elif self.temp_c >= 22:
            return 1.0   # Moderate A/C
        return 0.0


# ─── Cache (simple in-memory, TTL-based) ─────────────────────

class _WeatherCache:
    def __init__(self, ttl_seconds: int):
        self._ttl   = ttl_seconds
        self._store: dict[str, tuple[float, WeatherReading]] = {}

    def get(self, key: str) -> Optional[WeatherReading]:
        if key in self._store:
            ts, reading = self._store[key]
            if time.time() - ts < self._ttl:
                return reading
        return None

    def set(self, key: str, reading: WeatherReading) -> None:
        self._store[key] = (time.time(), reading)


_cache = _WeatherCache(ttl_seconds=WEATHER_CACHE_TTL)

# ─── Locations supported ─────────────────────────────────────

LOCATIONS = {
    "cairo": {
        "q":    "Cairo,EG",
        "lat":  30.0444,
        "lon":  31.2357,
        "name": "Cairo",
    },
    "new_capital": {
        "q":    None,        # No city name — use lat/lon
        "lat":  30.0200,
        "lon":  31.7400,
        "name": "New Administrative Capital",
    },
}


# ─── Main fetcher ─────────────────────────────────────────────

def fetch_weather(location: str = "cairo") -> WeatherReading:
    """
    Fetch current weather for the given location.

    Args:
        location: "cairo" | "new_capital"

    Returns:
        WeatherReading (from API, cache, or seasonal fallback)

    Notes:
        - Cached for WEATHER_CACHE_TTL seconds (default 300s / 5 min)
        - Falls back to realistic Cairo seasonal averages if API key missing
        - Free tier: 1,000 calls/day — with 5-min cache and 5 vehicles,
          you'll use ~288 calls/day well within the free limit
    """
    loc = LOCATIONS.get(location, LOCATIONS["cairo"])
    cache_key = location

    # 1. Check cache
    cached = _cache.get(cache_key)
    if cached:
        reading = WeatherReading(**{**cached.to_dict(), "source": "cache"})
        logger.debug(f"[Weather] Cache hit for {location}")
        return reading

    # 2. Try API
    if OPENWEATHER_API_KEY:
        try:
            reading = _fetch_from_api(loc)
            _cache.set(cache_key, reading)
            logger.info(f"[Weather] API fetch OK: {reading.condition} {reading.temp_c}°C")
            return reading
        except Exception as e:
            logger.warning(f"[Weather] API error: {e} — using seasonal fallback")

    # 3. Seasonal fallback
    return _seasonal_fallback(loc)


def _fetch_from_api(loc: dict) -> WeatherReading:
    base_url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
    }
    if loc["q"]:
        params["q"] = loc["q"]
    else:
        params["lat"] = loc["lat"]
        params["lon"] = loc["lon"]

    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    d = resp.json()

    condition = d["weather"][0]["main"] if d.get("weather") else "Unknown"
    description = d["weather"][0]["description"] if d.get("weather") else "unknown"
    visibility_km = d.get("visibility", 10000) / 1000

    return WeatherReading(
        location          = loc["name"],
        latitude          = d["coord"]["lat"],
        longitude         = d["coord"]["lon"],
        temp_c            = round(d["main"]["temp"], 1),
        feels_like_c      = round(d["main"]["feels_like"], 1),
        humidity_pct      = d["main"]["humidity"],
        wind_kmh          = round(d["wind"]["speed"] * 3.6, 1),
        wind_direction_deg= d["wind"].get("deg", 0),
        condition         = condition,
        description       = description,
        visibility_km     = round(visibility_km, 1),
        uv_index          = 0.0,   # Requires separate UV endpoint (One Call API)
        pressure_hpa      = d["main"]["pressure"],
        timestamp_unix    = d["dt"],
        source            = "api",
    )


def _seasonal_fallback(loc: dict) -> WeatherReading:
    import datetime
    month = datetime.datetime.now().month
    defaults = _CAIRO_SEASONAL_DEFAULTS[month]

    logger.warning(
        f"[Weather] Using seasonal fallback for {loc['name']} "
        f"(month={month}). Set OPENWEATHER_API_KEY in .env for real data."
    )
    return WeatherReading(
        location           = loc["name"],
        latitude           = loc["lat"],
        longitude          = loc["lon"],
        temp_c             = defaults["temp_c"],
        feels_like_c       = defaults["temp_c"] - 2,
        humidity_pct       = defaults["humidity"],
        wind_kmh           = defaults["wind_kmh"],
        wind_direction_deg = 180,
        condition          = defaults["condition"],
        description        = defaults["condition"].lower(),
        visibility_km      = defaults["visibility_km"],
        uv_index           = 5.0,
        pressure_hpa       = 1013,
        timestamp_unix     = int(time.time()),
        source             = "fallback",
    )


# ─── Quick sanity test ────────────────────────────────────────

if __name__ == "__main__":
    import json, sys
    logging.basicConfig(level=logging.INFO)

    print("\n── Weather fetcher test ──────────────────────────")
    for loc_name in ["cairo", "new_capital"]:
        w = fetch_weather(loc_name)
        print(f"\n{w.location}:")
        print(f"  Source      : {w.source}")
        print(f"  Condition   : {w.condition} ({w.description})")
        print(f"  Temperature : {w.temp_c}°C (feels like {w.feels_like_c}°C)")
        print(f"  Humidity    : {w.humidity_pct}%")
        print(f"  Wind        : {w.wind_kmh} km/h")
        print(f"  Visibility  : {w.visibility_km} km")
        print(f"  Speed factor: {w.speed_factor} (vehicle speed multiplier)")
        print(f"  A/C load    : +{w.ac_load_factor} L/100km")

    print("\nJSON payload sample:")
    print(json.dumps(fetch_weather("cairo").to_dict(), indent=2))
