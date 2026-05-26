"""
smart_city/simulator/traffic_fetcher.py

Fetches real-time traffic flow data for Cairo from the TomTom Traffic API.
Free tier: 2,500 calls/day.
API docs: https://developer.tomtom.com/traffic-api/documentation/traffic-flow/flow-segment-data

Features:
  - Per-segment traffic flow (speed, congestion ratio)
  - 60-second cache per GPS point (avoids quota burn)
  - Cairo rush-hour simulation fallback (no API key required)
  - Traffic density scoring 0–10 (used by vehicle simulator)
"""

from __future__ import annotations

import time
import math
import logging
import random
import datetime
from dataclasses import dataclass, asdict
from typing import Optional

import requests

from config import TOMTOM_API_KEY

logger = logging.getLogger(__name__)

CACHE_TTL = 60   # seconds


# ─────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────

@dataclass
class TrafficReading:
    latitude: float
    longitude: float
    current_speed_kmh: float       # actual speed of traffic on this road
    free_flow_speed_kmh: float     # speed when road is empty
    congestion_ratio: float        # 0.0 (free flow) → 1.0 (gridlock)
    traffic_density: int           # 0–10 score (used by simulator)
    confidence: float              # 0.0–1.0 (TomTom data quality)
    road_closure: bool
    timestamp_unix: int
    source: str                    # "api" | "cache" | "simulated"

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def speed_factor(self) -> float:
        """Multiplier for vehicle speed: 1.0 = free flow, 0.1 = near gridlock."""
        return max(0.1, 1.0 - self.congestion_ratio * 0.9)


# ─────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────

class _TrafficCache:
    BUCKET_DEG = 0.01   # ~1.1 km grid — same bucket = same cached reading

    def __init__(self, ttl: int):
        self._ttl   = ttl
        self._store: dict[tuple, tuple[float, TrafficReading]] = {}

    def _key(self, lat: float, lon: float) -> tuple:
        return (round(lat / self.BUCKET_DEG), round(lon / self.BUCKET_DEG))

    def get(self, lat: float, lon: float) -> Optional[TrafficReading]:
        k = self._key(lat, lon)
        if k in self._store:
            ts, reading = self._store[k]
            if time.time() - ts < self._ttl:
                return reading
        return None

    def set(self, lat: float, lon: float, reading: TrafficReading) -> None:
        self._store[self._key(lat, lon)] = (time.time(), reading)


_cache = _TrafficCache(ttl=CACHE_TTL)


# ─────────────────────────────────────────────────────────────
# Cairo rush-hour traffic model (fallback / supplement)
# ─────────────────────────────────────────────────────────────

def _cairo_traffic_score(lat: float, lon: float) -> int:
    """
    Returns a 0–10 congestion score based on:
      - Time of day (Cairo rush hours: 8–10 AM, 4–7 PM)
      - Day of week (Friday = low, Saturday–Thursday = normal)
      - Location zone (downtown = higher congestion)

    No API call needed. Realistic for Cairo.
    """
    now         = datetime.datetime.now()
    hour        = now.hour + now.minute / 60
    weekday     = now.weekday()    # 0=Monday … 6=Sunday; Friday=4 in Cairo

    # Cairo weekend is Friday–Saturday
    is_weekend  = weekday in (4, 5)   # Friday, Saturday

    # ── Time-of-day congestion curve ──
    if is_weekend:
        if 14 <= hour <= 18:
            time_score = 5.0      # Friday afternoon family outings
        elif 10 <= hour <= 14:
            time_score = 3.0      # Friday prayers dispersal
        else:
            time_score = 2.0
    else:
        # Weekday rush-hour curves
        if 8.0 <= hour <= 10.5:
            # Morning peak: sharp rise and fall
            peak = 9.0
            time_score = 9.0 - abs(hour - peak) * 2.5
        elif 16.0 <= hour <= 19.5:
            # Evening peak: broader, heavier
            peak = 17.5
            time_score = 9.5 - abs(hour - peak) * 1.8
        elif 12.0 <= hour <= 14.0:
            # Lunch dip
            time_score = 4.5
        elif 22.0 <= hour or hour <= 5.0:
            # Night — light traffic
            time_score = 1.5
        else:
            time_score = 4.0

    # ── Location zone factor ──
    # Downtown Cairo (Tahrir area) heavier than Ring Road
    downtown_lat, downtown_lon = 30.0444, 31.2357
    dist_km = math.sqrt(
        ((lat - downtown_lat) * 111) ** 2 +
        ((lon - downtown_lon) * 111 * math.cos(math.radians(lat))) ** 2
    )
    if dist_km < 3:
        zone_factor = 1.3    # downtown core
    elif dist_km < 8:
        zone_factor = 1.1    # inner ring
    elif dist_km < 20:
        zone_factor = 0.85   # Ring Road / arterials
    else:
        zone_factor = 0.65   # desert highways (Airport Rd, Cairo–Suez)

    score = min(10, max(0, round(time_score * zone_factor + random.gauss(0, 0.5))))
    return score


def _density_to_congestion(density: int) -> float:
    """Convert 0–10 density score to 0.0–1.0 congestion ratio."""
    return density / 10.0


# ─────────────────────────────────────────────────────────────
# Main fetcher
# ─────────────────────────────────────────────────────────────

def fetch_traffic(lat: float, lon: float) -> TrafficReading:
    """
    Fetch traffic conditions at the given GPS coordinates.

    Priority:
      1. In-memory cache (60-second TTL)
      2. TomTom API (if TOMTOM_API_KEY is set)
      3. Cairo rush-hour simulation model

    Args:
        lat: latitude (Cairo range: 29.9 – 30.2)
        lon: longitude (Cairo range: 31.1 – 31.8)

    Returns:
        TrafficReading with speed, congestion, density scores
    """
    # 1. Cache
    cached = _cache.get(lat, lon)
    if cached:
        logger.debug(f"[Traffic] Cache hit ({lat:.3f}, {lon:.3f})")
        return TrafficReading(**{**cached.to_dict(), "source": "cache"})

    # 2. TomTom API
    if TOMTOM_API_KEY:
        try:
            reading = _fetch_from_tomtom(lat, lon)
            _cache.set(lat, lon, reading)
            logger.info(f"[Traffic] TomTom: density={reading.traffic_density} congestion={reading.congestion_ratio:.2f}")
            return reading
        except Exception as e:
            logger.warning(f"[Traffic] TomTom API error: {e} — using simulation")

    # 3. Simulation fallback
    return _simulated_reading(lat, lon)


def _fetch_from_tomtom(lat: float, lon: float) -> TrafficReading:
    """
    TomTom Flow Segment Data API.
    Returns real-time speed and free-flow speed for the nearest road segment.
    """
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/"
        f"relative0/10/json"
    )
    params = {
        "point":  f"{lat},{lon}",
        "unit":   "KMPH",
        "key":    TOMTOM_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("flowSegmentData", {})

    current_speed   = float(data.get("currentSpeed",   50))
    free_flow_speed = float(data.get("freeFlowSpeed",  80))
    confidence      = float(data.get("confidence",      1))
    road_closure    = bool(data.get("roadClosure",   False))

    congestion = max(0.0, min(1.0, 1.0 - current_speed / max(free_flow_speed, 1)))
    density    = round(congestion * 10)

    return TrafficReading(
        latitude           = lat,
        longitude          = lon,
        current_speed_kmh  = round(current_speed,   1),
        free_flow_speed_kmh= round(free_flow_speed, 1),
        congestion_ratio   = round(congestion,       3),
        traffic_density    = density,
        confidence         = round(confidence,       2),
        road_closure       = road_closure,
        timestamp_unix     = int(time.time()),
        source             = "api",
    )


def _simulated_reading(lat: float, lon: float) -> TrafficReading:
    density    = _cairo_traffic_score(lat, lon)
    congestion = _density_to_congestion(density)

    # Estimate speeds from congestion
    free_flow  = 90.0  # typical Cairo arterial free-flow speed
    current    = max(5.0, free_flow * (1 - congestion * 0.85))

    return TrafficReading(
        latitude           = lat,
        longitude          = lon,
        current_speed_kmh  = round(current,     1),
        free_flow_speed_kmh= free_flow,
        congestion_ratio   = round(congestion,  3),
        traffic_density    = density,
        confidence         = 0.75,
        road_closure       = False,
        timestamp_unix     = int(time.time()),
        source             = "simulated",
    )


# ─────────────────────────────────────────────────────────────
# Quick test
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)

    test_points = [
        ("Tahrir Square",        30.0444, 31.2357),
        ("Ring Road East",       30.1100, 31.3920),
        ("New Capital",          30.0200, 31.7400),
        ("6th October Highway",  29.9343, 30.9274),
    ]

    print("\n── Traffic fetcher test ──────────────────────────")
    now = datetime.datetime.now()
    print(f"  Time: {now.strftime('%A %H:%M')} (Cairo time)\n")

    for name, lat, lon in test_points:
        t = fetch_traffic(lat, lon)
        bar = "█" * t.traffic_density + "░" * (10 - t.traffic_density)
        print(f"  {name}")
        print(f"    Source     : {t.source}")
        print(f"    Density    : [{bar}] {t.traffic_density}/10")
        print(f"    Speed      : {t.current_speed_kmh} km/h (free-flow: {t.free_flow_speed_kmh})")
        print(f"    Congestion : {t.congestion_ratio:.0%}")
        print(f"    Spd factor : {t.speed_factor:.2f}")
        print()
