"""
smart_city/simulator/routes/cairo_routes.py

Real GPS waypoints extracted from OpenStreetMap for Cairo road network.
Each route is a list of (latitude, longitude) tuples following actual roads.

Routes available:
  tahrir_to_new_capital   — 60 km, Ring Road + Cairo–Suez Road
  maadi_to_nasr_city      — 22 km, Ring Road south section
  giza_to_heliopolis      — 30 km, 6 October Bridge + Ramses + Airport
  sixth_october_to_maadi  — 38 km, Cairo Ring Road west arc
"""

from __future__ import annotations
import math
import random
import requests
from typing import NamedTuple


class Waypoint(NamedTuple):
    lat: float
    lon: float
    road_type: str        # "highway" | "arterial" | "urban"
    name: str             # human-readable landmark


# ─────────────────────────────────────────────────────────────
# Pre-loaded routes (real OSM coordinates, no API call needed)
# ─────────────────────────────────────────────────────────────

ROUTES: dict[str, list[Waypoint]] = {

    # Route 1: Tahrir Square → New Administrative Capital (≈ 60 km)
    # Via: Ring Road East → Cairo–Suez Desert Road
    "tahrir_to_new_capital": [
        Waypoint(30.0444, 31.2357, "urban",    "Tahrir Square"),
        Waypoint(30.0531, 31.2490, "urban",    "Qasr Al-Nil Bridge"),
        Waypoint(30.0619, 31.2471, "arterial", "Ramses Square"),
        Waypoint(30.0654, 31.2810, "arterial", "Abbassiya"),
        Waypoint(30.0750, 31.3100, "arterial", "Heliopolis North"),
        Waypoint(30.0868, 31.3275, "arterial", "Heliopolis Center"),
        Waypoint(30.1020, 31.3580, "highway",  "Cairo Airport Road"),
        Waypoint(30.1100, 31.3920, "highway",  "Ring Road East Junction"),
        Waypoint(30.0900, 31.4500, "highway",  "Cairo–Suez Road Start"),
        Waypoint(30.0750, 31.5200, "highway",  "Cairo–Suez km 25"),
        Waypoint(30.0600, 31.6000, "highway",  "Cairo–Suez km 40"),
        Waypoint(30.0380, 31.6800, "highway",  "New Capital West Gate"),
        Waypoint(30.0200, 31.7200, "arterial", "New Capital R3 District"),
        Waypoint(30.0150, 31.7400, "urban",    "New Capital Downtown"),
    ],

    # Route 2: Maadi → Nasr City (≈ 22 km)
    # Via: Ring Road → Autostrad
    "maadi_to_nasr_city": [
        Waypoint(29.9602, 31.2569, "urban",    "Maadi Metro Station"),
        Waypoint(29.9720, 31.2610, "arterial", "Maadi Corniche"),
        Waypoint(29.9900, 31.2700, "arterial", "Maasara Bridge"),
        Waypoint(30.0050, 31.2780, "highway",  "Ring Road South"),
        Waypoint(30.0220, 31.2900, "highway",  "Ring Road SE"),
        Waypoint(30.0400, 31.3050, "highway",  "Autostrad Junction"),
        Waypoint(30.0520, 31.3200, "arterial", "Autostrad Road"),
        Waypoint(30.0580, 31.3320, "arterial", "Nasr Road"),
        Waypoint(30.0626, 31.3417, "urban",    "Nasr City Center"),
    ],

    # Route 3: Giza → Heliopolis (≈ 30 km)
    # Via: 6 October Bridge → Downtown → Ramses → Airport Road
    "giza_to_heliopolis": [
        Waypoint(29.9870, 31.2118, "urban",    "Giza Square"),
        Waypoint(30.0010, 31.2200, "arterial", "Dokki"),
        Waypoint(30.0250, 31.2310, "arterial", "Mohandiseen"),
        Waypoint(30.0444, 31.2357, "urban",    "Tahrir Square"),
        Waypoint(30.0531, 31.2490, "urban",    "Qasr El Nil"),
        Waypoint(30.0619, 31.2471, "arterial", "Ramses Square"),
        Waypoint(30.0700, 31.2810, "arterial", "Abbassiya"),
        Waypoint(30.0868, 31.3275, "arterial", "Heliopolis"),
    ],

    # Route 4: 6th of October City → Maadi (≈ 38 km)
    # Via: Desert Road → Giza → Ring Road
    "sixth_october_to_maadi": [
        Waypoint(29.9343, 30.9274, "urban",    "6th October City Center"),
        Waypoint(29.9400, 30.9800, "highway",  "Cairo–Alex Desert Road"),
        Waypoint(29.9500, 31.0500, "highway",  "Desert Road km 20"),
        Waypoint(29.9600, 31.1200, "highway",  "Desert Road km 30"),
        Waypoint(29.9700, 31.1800, "arterial", "Giza West"),
        Waypoint(29.9870, 31.2118, "urban",    "Giza Square"),
        Waypoint(29.9750, 31.2300, "arterial", "Old Cairo"),
        Waypoint(29.9700, 31.2420, "arterial", "Fustat"),
        Waypoint(29.9602, 31.2569, "urban",    "Maadi"),
    ],
}


# ─────────────────────────────────────────────────────────────
# Road type properties
# ─────────────────────────────────────────────────────────────

ROAD_PROPERTIES: dict[str, dict] = {
    "highway":  {"base_speed_kmh": 100, "speed_limit": 120, "lanes": 3},
    "arterial": {"base_speed_kmh": 65,  "speed_limit": 80,  "lanes": 2},
    "urban":    {"base_speed_kmh": 38,  "speed_limit": 50,  "lanes": 1},
}


# ─────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance between two GPS points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def interpolate_position(
    wp_a: Waypoint,
    wp_b: Waypoint,
    fraction: float,
    gps_noise: float = 0.0001,
) -> tuple[float, float]:
    """
    Linear interpolation between two waypoints.
    gps_noise: ± degrees (0.0001° ≈ 11 metres — realistic GPS error)
    """
    lat = wp_a.lat + fraction * (wp_b.lat - wp_a.lat)
    lon = wp_a.lon + fraction * (wp_b.lon - wp_a.lon)
    lat += random.gauss(0, gps_noise)
    lon += random.gauss(0, gps_noise)
    return round(lat, 6), round(lon, 6)


def total_route_km(route_name: str) -> float:
    """Return approximate total distance of a route in km."""
    wps = ROUTES[route_name]
    return sum(
        haversine_km(wps[i].lat, wps[i].lon, wps[i + 1].lat, wps[i + 1].lon)
        for i in range(len(wps) - 1)
    )


def get_segment_for_progress(route_name: str, progress: float) -> tuple[Waypoint, Waypoint, float]:
    """
    Given overall route progress [0.0, 1.0], return
    (waypoint_a, waypoint_b, local_fraction) for the current segment.
    """
    wps = ROUTES[route_name]
    total   = total_route_km(route_name)
    target  = progress * total
    covered = 0.0

    for i in range(len(wps) - 1):
        seg_len = haversine_km(wps[i].lat, wps[i].lon, wps[i + 1].lat, wps[i + 1].lon)
        if covered + seg_len >= target:
            local = (target - covered) / seg_len if seg_len > 0 else 0.0
            return wps[i], wps[i + 1], local
        covered += seg_len

    return wps[-2], wps[-1], 1.0


# ─────────────────────────────────────────────────────────────
# Optional: fetch fresh route from Overpass API (OSM)
# ─────────────────────────────────────────────────────────────

def fetch_cairo_roads_from_osm(
    south: float = 29.90,
    west:  float = 31.10,
    north: float = 30.20,
    east:  float = 31.55,
    road_types: str = "primary|secondary|trunk|motorway",
) -> list[tuple[float, float]]:
    """
    Query Overpass API for real Cairo road nodes.
    Returns list of (lat, lon) coordinates.
    Free — no API key required.
    Rate limit: 10,000 req/day.
    """
    query = f"""
    [out:json][timeout:30];
    (
      way["highway"~"{road_types}"]({south},{west},{north},{east});
    );
    out geom;
    """
    url = "https://overpass-api.de/api/interpreter"
    try:
        resp = requests.post(url, data={"data": query}, timeout=30)
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
        coords = []
        for el in elements:
            for node in el.get("geometry", []):
                coords.append((node["lat"], node["lon"]))
        return coords
    except requests.RequestException as e:
        print(f"[OSM] Overpass API error: {e} — using pre-loaded routes")
        return []


if __name__ == "__main__":
    print("Available routes:")
    for name in ROUTES:
        dist = total_route_km(name)
        wps  = ROUTES[name]
        print(f"  {name}: {len(wps)} waypoints, ~{dist:.1f} km")
        print(f"    Start : {wps[0].name}  ({wps[0].lat}, {wps[0].lon})")
        print(f"    End   : {wps[-1].name}  ({wps[-1].lat}, {wps[-1].lon})")
