"""
smart_city/simulator/vehicle_simulator.py

Physics-based vehicle telemetry simulator for the Smart City platform.

Key design principle — CORRELATED variables:
  Speed → RPM → Fuel consumption
  Traffic density → Speed slowdown
  Weather condition → Speed factor
  Engine temp → Warmup curve + traffic heat
  GPS → Interpolated along real Cairo OSM routes

All formulas are based on realistic automotive engineering values
calibrated for typical Egyptian sedan/SUV in Cairo conditions.
"""

from __future__ import annotations

import math
import random
import time
import uuid
import logging
from dataclasses import dataclass, asdict, field
from typing import Optional

from routes.cairo_routes import (
    ROUTES, ROAD_PROPERTIES, Waypoint,
    get_segment_for_progress, interpolate_position, total_route_km,
)
from weather_fetcher import WeatherReading
from traffic_fetcher import TrafficReading

logger = logging.getLogger(__name__)

# ─── Vehicle type profiles ────────────────────────────────────

VEHICLE_PROFILES = {
    "sedan": {
        "fuel_base_l100km":  8.0,
        "engine_cc":         1600,
        "tire_radius_m":     0.31,     # 195/65 R15
        "gear_ratios":       [3.8, 2.1, 1.4, 1.0, 0.80, 0.65],
        "final_drive":       3.9,
        "tank_size_l":       55.0,
        "max_rpm":           6500,
        "idle_rpm":          800,
        "mass_kg":           1300,
    },
    "suv": {
        "fuel_base_l100km":  11.0,
        "engine_cc":         2000,
        "tire_radius_m":     0.34,     # 235/60 R17
        "gear_ratios":       [3.6, 2.2, 1.5, 1.0, 0.82, 0.67],
        "final_drive":       3.7,
        "tank_size_l":       70.0,
        "max_rpm":           6000,
        "idle_rpm":          750,
        "mass_kg":           1900,
    },
    "bus": {
        "fuel_base_l100km":  22.0,
        "engine_cc":         6700,
        "tire_radius_m":     0.50,
        "gear_ratios":       [6.0, 3.5, 2.1, 1.4, 1.0, 0.78],
        "final_drive":       5.1,
        "tank_size_l":       200.0,
        "max_rpm":           2800,
        "idle_rpm":          600,
        "mass_kg":           12000,
    },
    "microbus": {
        "fuel_base_l100km":  12.0,
        "engine_cc":         2200,
        "tire_radius_m":     0.33,
        "gear_ratios":       [4.5, 2.5, 1.6, 1.0, 0.75],
        "final_drive":       4.1,
        "tank_size_l":       70.0,
        "max_rpm":           5000,
        "idle_rpm":          700,
        "mass_kg":           2500,
    },
}

ROAD_EVENTS = ["NONE", "NONE", "NONE", "NONE", "NONE",
               "ACCIDENT", "ROADWORK", "BREAKDOWN", "CONGESTION_INCIDENT"]


@dataclass
class VehicleTelemetry:
    """One telemetry reading from a simulated vehicle — sent to Kafka."""
    event_id:          str
    vehicle_id:        str
    vehicle_type:      str
    route_name:        str
    timestamp_iso:     str
    timestamp_unix:    int

    # Position
    latitude:          float
    longitude:         float
    altitude_m:        float
    heading_deg:       float
    gps_accuracy_m:    float

    # Motion
    speed_kmh:         float
    acceleration_ms2:  float

    # Powertrain
    rpm:               int
    gear:              int
    engine_temp_c:     float
    engine_on:         bool

    # Fuel
    fuel_level_pct:    float
    fuel_consumed_l:   float
    fuel_rate_l100km:  float

    # Environment
    road_type:         str
    road_event:        str
    traffic_density:   int

    # Trip metadata
    trip_id:           str
    odometer_km:       float
    trip_distance_km:  float
    engine_runtime_s:  int

    def to_dict(self) -> dict:
        return asdict(self)

    def is_anomaly(self) -> bool:
        """True if any alert threshold is exceeded."""
        from config import (ALERT_SPEED_KMH, ALERT_ENGINE_TEMP_C,
                            ALERT_FUEL_PCT, ALERT_RPM)
        return (
            self.speed_kmh    > ALERT_SPEED_KMH    or
            self.engine_temp_c > ALERT_ENGINE_TEMP_C or
            self.fuel_level_pct < ALERT_FUEL_PCT   or
            self.rpm          > ALERT_RPM
        )


class VehicleSimulator:
    """
    Simulates a single vehicle travelling along a Cairo route.
    Call .step() every EMIT_INTERVAL seconds to get a new telemetry reading.
    """

    def __init__(
        self,
        vehicle_id: Optional[str] = None,
        vehicle_type: str = "sedan",
        route_name: str = "tahrir_to_new_capital",
        initial_fuel_pct: float = 85.0,
        start_progress: float = 0.0,
    ):
        self.vehicle_id    = vehicle_id or f"CAR-{random.randint(1000, 9999)}"
        self.vehicle_type  = vehicle_type
        self.profile       = VEHICLE_PROFILES[vehicle_type]
        self.route_name    = route_name
        self.trip_id       = str(uuid.uuid4())[:8].upper()

        # State
        self._progress      = start_progress       # 0.0 → 1.0 along route
        self._route_len_km  = total_route_km(route_name)
        self._fuel_l        = self.profile["tank_size_l"] * initial_fuel_pct / 100
        self._engine_temp   = 25.0                 # cold start
        self._odometer_km   = 0.0
        self._trip_km       = 0.0
        self._engine_on_s   = 0
        self._prev_speed    = 0.0
        self._elapsed_s     = 0.0
        self._current_speed = 0.0

        # Latest external data (injected from fetchers)
        self._weather: Optional[WeatherReading] = None
        self._traffic: Optional[TrafficReading] = None

        logger.info(f"[Sim] {self.vehicle_id} ({vehicle_type}) on {route_name} "
                    f"({self._route_len_km:.1f} km), fuel={initial_fuel_pct:.0f}%")

    # ── External data injection ───────────────────────────────

    def update_weather(self, weather: WeatherReading) -> None:
        self._weather = weather

    def update_traffic(self, traffic: TrafficReading) -> None:
        self._traffic = traffic

    # ── Core step ─────────────────────────────────────────────

    def step(self, dt: float = 1.0) -> VehicleTelemetry:
        """
        Advance simulation by dt seconds and return a telemetry reading.

        Args:
            dt: time step in seconds (default 1.0)
        """
        self._elapsed_s   += dt
        self._engine_on_s += int(dt)

        # 1. Get current road segment
        wp_a, wp_b, frac = get_segment_for_progress(self.route_name, self._progress)
        road_type = wp_b.road_type

        # 2. Compute speed (correlated with traffic + weather)
        speed = self._compute_speed(road_type)

        # 3. Move along route
        dist_km = speed * dt / 3600.0
        self._progress   += dist_km / self._route_len_km
        self._odometer_km += dist_km
        self._trip_km     += dist_km

        if self._progress >= 1.0:
            self._progress = 0.0   # loop route (continuous simulation)
            logger.info(f"[Sim] {self.vehicle_id} completed trip, restarting")

        # 4. GPS position with noise
        lat, lon = interpolate_position(wp_a, wp_b, frac, gps_noise=0.00008)

        # 5. Heading (degrees from north)
        heading = self._compute_heading(wp_a, wp_b)

        # 6. RPM
        rpm, gear = self._compute_rpm(speed)

        # 7. Engine temperature
        engine_temp = self._compute_engine_temp(speed, dt)

        # 8. Fuel
        fuel_rate    = self._compute_fuel_rate(speed, rpm, engine_temp)
        fuel_used_l  = fuel_rate * dist_km / 100.0
        self._fuel_l = max(0.0, self._fuel_l - fuel_used_l)
        fuel_pct     = self._fuel_l / self.profile["tank_size_l"] * 100

        # 9. Acceleration
        accel = (speed - self._prev_speed) / 3.6 / max(dt, 0.001)
        self._prev_speed  = speed
        self._current_speed = speed

        # 10. Road event (probabilistic)
        road_event = self._random_road_event(road_type)

        import datetime as dt_module
        now = dt_module.datetime.utcnow()

        return VehicleTelemetry(
            event_id         = str(uuid.uuid4()),
            vehicle_id       = self.vehicle_id,
            vehicle_type     = self.vehicle_type,
            route_name       = self.route_name,
            timestamp_iso    = now.isoformat() + "Z",
            timestamp_unix   = int(now.timestamp()),
            latitude         = lat,
            longitude        = lon,
            altitude_m       = round(random.gauss(28, 3), 1),    # Cairo ~28m ASL
            heading_deg      = heading,
            gps_accuracy_m   = round(random.uniform(3, 8), 1),
            speed_kmh        = round(speed, 2),
            acceleration_ms2 = round(accel, 3),
            rpm              = rpm,
            gear             = gear,
            engine_temp_c    = round(engine_temp, 1),
            engine_on        = True,
            fuel_level_pct   = round(fuel_pct, 2),
            fuel_consumed_l  = round(fuel_used_l, 5),
            fuel_rate_l100km = round(fuel_rate, 2),
            road_type        = road_type,
            road_event       = road_event,
            traffic_density  = self._traffic.traffic_density if self._traffic else 5,
            trip_id          = self.trip_id,
            odometer_km      = round(self._odometer_km, 3),
            trip_distance_km = round(self._trip_km, 3),
            engine_runtime_s = self._engine_on_s,
        )

    # ── Physics helpers ───────────────────────────────────────

    def _compute_speed(self, road_type: str) -> float:
        """
        Speed = base_speed × traffic_factor × weather_factor + noise
        Fully correlated, never independently random.
        """
        props      = ROAD_PROPERTIES[road_type]
        base_speed = props["base_speed_kmh"]

        # Traffic slowdown
        traffic_factor = self._traffic.speed_factor if self._traffic else 0.7
        # Weather slowdown
        weather_factor = self._weather.speed_factor if self._weather else 1.0

        # Cairo rush-hour factor (time-of-day, even without TomTom)
        import datetime
        hour = datetime.datetime.now().hour + datetime.datetime.now().minute / 60
        if 8.0 <= hour <= 10.5 or 16.0 <= hour <= 19.5:
            rush_factor = 0.75
        else:
            rush_factor = 1.0

        # Gaussian noise (σ = 3 km/h — realistic speed variation)
        noise = random.gauss(0, 3)

        speed = base_speed * traffic_factor * weather_factor * rush_factor + noise
        # Physical limits
        speed = max(2.0, min(speed, props["speed_limit"] * 1.05))
        return round(speed, 2)

    def _compute_rpm(self, speed_kmh: float) -> tuple[int, int]:
        """
        RPM = (speed_m_s / tire_circ_m) × gear_ratio × final_drive × 60
        Gear is selected based on speed thresholds (simplified automatic gearbox).
        """
        gear_thresholds = [0, 20, 40, 65, 90, 120]    # km/h shift points
        gear = 1
        for i, threshold in enumerate(gear_thresholds):
            if speed_kmh >= threshold:
                gear = min(i + 1, len(self.profile["gear_ratios"]))

        if speed_kmh < 3:
            return self.profile["idle_rpm"], 0    # engine running, not moving

        speed_ms       = speed_kmh / 3.6
        tire_circ      = 2 * math.pi * self.profile["tire_radius_m"]
        gear_ratio     = self.profile["gear_ratios"][gear - 1]
        final_drive    = self.profile["final_drive"]

        rpm = (speed_ms / tire_circ) * gear_ratio * final_drive * 60
        rpm = int(max(self.profile["idle_rpm"], min(rpm, self.profile["max_rpm"])))
        rpm += int(random.gauss(0, 30))   # sensor noise

        return max(0, rpm), gear

    def _compute_engine_temp(self, speed_kmh: float, dt: float) -> float:
        """
        Cold start: 25°C → 90°C over ~5 minutes.
        Idling in traffic: slow rise toward 102°C.
        Moving at speed: stabilizes at 88–92°C (airflow cooling).
        """
        target_temp = 90.0

        if speed_kmh < 5:
            # Idling in traffic — slow heat build-up
            target_temp = 98.0 if self._engine_on_s > 300 else 90.0
            rate = 0.03 * dt
        elif speed_kmh > 80:
            # Good airflow — cool slightly below normal
            target_temp = 87.0
            rate = 0.05 * dt
        else:
            target_temp = 91.0
            rate = 0.04 * dt

        # Move toward target at rate
        diff = target_temp - self._engine_temp
        self._engine_temp += diff * rate
        self._engine_temp += random.gauss(0, 0.1)   # sensor noise

        return max(20.0, min(self._engine_temp, 115.0))

    def _compute_fuel_rate(self, speed_kmh: float, rpm: int, engine_temp: float) -> float:
        """
        Fuel consumption in L/100km.
        Base + RPM penalty + idle penalty + A/C load + warm-up enrichment.
        """
        base = self.profile["fuel_base_l100km"]

        # RPM penalty (above 2500 RPM is inefficient)
        rpm_penalty = max(0.0, (rpm - 2500) * 0.0015)

        # Traffic idle penalty (fuel wasted while stopped)
        if speed_kmh < 5:
            idle_penalty = 3.5    # litres/100km equivalent at idle
        else:
            idle_penalty = 0.0

        # A/C load (hot Egyptian climate)
        ac_load = self._weather.ac_load_factor if self._weather else 1.2

        # Cold engine enrichment (extra fuel while warming up)
        cold_penalty = max(0.0, (90.0 - engine_temp) * 0.08) if engine_temp < 80 else 0.0

        return round(base + rpm_penalty + idle_penalty + ac_load + cold_penalty, 2)

    def _compute_heading(self, wp_a: Waypoint, wp_b: Waypoint) -> int:
        """Compute compass heading (0–360°) from waypoint A to B."""
        dlat = wp_b.lat - wp_a.lat
        dlon = wp_b.lon - wp_a.lon
        angle = math.degrees(math.atan2(dlon, dlat))
        return int((angle + 360) % 360)

    def _random_road_event(self, road_type: str) -> str:
        """Probabilistic road events — calibrated for Cairo conditions."""
        event_probs = {
            "highway":  0.003,
            "arterial": 0.008,
            "urban":    0.015,
        }
        if random.random() < event_probs.get(road_type, 0.005):
            return random.choice(["ACCIDENT", "ROADWORK", "BREAKDOWN", "CONGESTION_INCIDENT"])
        return "NONE"


# ─── Fleet manager (multiple vehicles) ───────────────────────

class FleetSimulator:
    """Manages N vehicles across Cairo routes."""

    ROUTE_NAMES = list(ROUTES.keys())
    VEHICLE_TYPES = ["sedan", "sedan", "sedan", "suv", "microbus"]

    def __init__(self, vehicle_count: int = 5):
        self.vehicles: list[VehicleSimulator] = []

        for i in range(vehicle_count):
            v = VehicleSimulator(
                vehicle_id    = f"CAR-{1001 + i}",
                vehicle_type  = self.VEHICLE_TYPES[i % len(self.VEHICLE_TYPES)],
                route_name    = self.ROUTE_NAMES[i % len(self.ROUTE_NAMES)],
                initial_fuel_pct = random.uniform(40, 95),
                start_progress   = random.uniform(0, 0.9),
            )
            self.vehicles.append(v)

        logger.info(f"[Fleet] {vehicle_count} vehicles initialised")

    def step_all(self, dt: float = 1.0) -> list[VehicleTelemetry]:
        return [v.step(dt) for v in self.vehicles]

    def inject_weather(self, weather: WeatherReading) -> None:
        for v in self.vehicles:
            v.update_weather(weather)

    def inject_traffic(self, lat: float, lon: float, traffic: TrafficReading) -> None:
        """Update traffic for any vehicle currently near this GPS point."""
        for v in self.vehicles:
            v.update_traffic(traffic)


# ─── Quick test ───────────────────────────────────────────────

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    print("\n── Vehicle simulator test (5 steps) ─────────────")
    sim = VehicleSimulator(
        vehicle_id   = "CAR-1001",
        vehicle_type = "sedan",
        route_name   = "tahrir_to_new_capital",
    )

    for step in range(5):
        t = sim.step(dt=1.0)
        anomaly = " ⚠  ANOMALY" if t.is_anomaly() else ""
        print(
            f"  t={step+1}s | {t.latitude:.5f},{t.longitude:.5f} | "
            f"{t.speed_kmh:.1f} km/h | RPM {t.rpm} | "
            f"{t.engine_temp_c:.1f}°C | "
            f"fuel {t.fuel_level_pct:.1f}% | "
            f"{t.road_type} | {t.road_event}{anomaly}"
        )

    print("\n── Fleet test (3 vehicles, 2 steps each) ─────────")
    fleet = FleetSimulator(vehicle_count=3)
    for step in range(2):
        readings = fleet.step_all(dt=1.0)
        for r in readings:
            print(f"  {r.vehicle_id} | {r.route_name[:20]} | "
                  f"{r.speed_kmh:.1f} km/h | {r.fuel_level_pct:.1f}%")

    print("\n── Full telemetry JSON sample ─────────────────────")
    t = VehicleSimulator("CAR-0001", "sedan", "tahrir_to_new_capital").step()
    print(json.dumps(t.to_dict(), indent=2))
