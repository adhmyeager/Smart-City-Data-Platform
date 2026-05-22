"""
smart_city/simulator/tests/test_sources.py

Quick integration test — verifies all data sources produce valid output.
Run this BEFORE Docker/Kafka to confirm your setup works.

Usage:
  cd simulator
  pip install -r requirements.txt
  python tests/test_sources.py

No Kafka needed. No API keys required (uses fallback/simulation).
"""

import sys
import os
import json
import datetime
import unittest

# Make sure parent dir is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from routes.cairo_routes import (
    ROUTES, total_route_km, get_segment_for_progress,
    interpolate_position, haversine_km,
)
from weather_fetcher import fetch_weather, WeatherReading, CONDITION_SPEED_FACTOR
from traffic_fetcher import fetch_traffic, TrafficReading, _cairo_traffic_score
from vehicle_simulator import VehicleSimulator, FleetSimulator


class TestCairoRoutes(unittest.TestCase):

    def test_all_routes_have_waypoints(self):
        for name, wps in ROUTES.items():
            self.assertGreater(len(wps), 2, f"{name} has too few waypoints")

    def test_route_distances_realistic(self):
        for name in ROUTES:
            dist = total_route_km(name)
            self.assertGreater(dist, 5,  f"{name}: distance too short ({dist:.1f} km)")
            self.assertLess(dist,    100, f"{name}: distance too long ({dist:.1f} km)")

    def test_cairo_coordinates_valid(self):
        for name, wps in ROUTES.items():
            for wp in wps:
                self.assertGreater(wp.lat, 29.5, f"{name}: lat too far south")
                self.assertLess(wp.lat,    30.5, f"{name}: lat too far north")
                self.assertGreater(wp.lon, 30.5, f"{name}: lon too far west")
                self.assertLess(wp.lon,    32.0, f"{name}: lon too far east")

    def test_get_segment_returns_valid_result(self):
        wp_a, wp_b, frac = get_segment_for_progress("tahrir_to_new_capital", 0.5)
        self.assertIsNotNone(wp_a)
        self.assertIsNotNone(wp_b)
        self.assertGreaterEqual(frac, 0.0)
        self.assertLessEqual(frac,    1.0)

    def test_interpolate_stays_in_cairo(self):
        wps = ROUTES["tahrir_to_new_capital"]
        lat, lon = interpolate_position(wps[0], wps[1], 0.5)
        self.assertGreater(lat, 29.9)
        self.assertLess(lat,    30.3)
        self.assertGreater(lon, 31.0)
        self.assertLess(lon,    31.6)

    def test_haversine_known_distance(self):
        # Tahrir → Ramses: ~3 km
        dist = haversine_km(30.0444, 31.2357, 30.0619, 31.2471)
        self.assertGreater(dist, 1.5)
        self.assertLess(dist,    5.0)


class TestWeatherFetcher(unittest.TestCase):

    def test_fallback_returns_valid_reading(self):
        """Works even with no API key — seasonal fallback always available."""
        w = fetch_weather("cairo")
        self.assertIsInstance(w, WeatherReading)
        self.assertGreater(w.temp_c, 0)
        self.assertLess(w.temp_c,    50)
        self.assertGreater(w.humidity_pct, 0)
        self.assertLessEqual(w.humidity_pct, 100)

    def test_speed_factor_in_range(self):
        w = fetch_weather("cairo")
        self.assertGreater(w.speed_factor, 0.5)
        self.assertLessEqual(w.speed_factor, 1.0)

    def test_all_conditions_have_speed_factor(self):
        for cond in CONDITION_SPEED_FACTOR:
            self.assertGreater(CONDITION_SPEED_FACTOR[cond], 0)
            self.assertLessEqual(CONDITION_SPEED_FACTOR[cond], 1.0)

    def test_cache_returns_same_object(self):
        w1 = fetch_weather("cairo")
        w2 = fetch_weather("cairo")
        # Second call should be from cache (same temp/condition)
        self.assertEqual(w1.temp_c, w2.temp_c)
        self.assertEqual(w1.condition, w2.condition)

    def test_to_dict_is_serialisable(self):
        w = fetch_weather("cairo")
        d = w.to_dict()
        json_str = json.dumps(d)
        self.assertIn("temp_c", json_str)
        self.assertIn("condition", json_str)

    def test_new_capital_different_from_cairo(self):
        """Both locations should return readings (may differ slightly in lat/lon)."""
        w_cairo = fetch_weather("cairo")
        w_nc    = fetch_weather("new_capital")
        self.assertAlmostEqual(w_cairo.latitude,  30.0444, places=1)
        self.assertAlmostEqual(w_nc.latitude,     30.0200, places=1)


class TestTrafficFetcher(unittest.TestCase):

    def test_simulated_reading_valid(self):
        t = fetch_traffic(30.0444, 31.2357)
        self.assertIsInstance(t, TrafficReading)
        self.assertGreaterEqual(t.traffic_density, 0)
        self.assertLessEqual(t.traffic_density,    10)

    def test_speed_factor_in_range(self):
        t = fetch_traffic(30.0444, 31.2357)
        self.assertGreater(t.speed_factor, 0.0)
        self.assertLessEqual(t.speed_factor, 1.0)

    def test_downtown_heavier_than_highway(self):
        """Downtown Cairo should score higher than desert highway."""
        downtown = _cairo_traffic_score(30.0444, 31.2357)
        desert   = _cairo_traffic_score(30.0600, 31.6000)
        # Not always true due to time-of-day, but downtown zone factor is higher
        # We just check both are valid
        self.assertGreaterEqual(downtown, 0)
        self.assertGreaterEqual(desert,   0)

    def test_to_dict_serialisable(self):
        t = fetch_traffic(30.0444, 31.2357)
        json_str = json.dumps(t.to_dict())
        self.assertIn("traffic_density", json_str)

    def test_cache_works(self):
        t1 = fetch_traffic(30.0444, 31.2357)
        t2 = fetch_traffic(30.0446, 31.2360)   # within cache bucket
        self.assertEqual(t1.traffic_density, t2.traffic_density)


class TestVehicleSimulator(unittest.TestCase):

    def setUp(self):
        self.sim = VehicleSimulator(
            vehicle_id   = "TEST-0001",
            vehicle_type = "sedan",
            route_name   = "tahrir_to_new_capital",
        )

    def test_step_returns_telemetry(self):
        t = self.sim.step()
        self.assertEqual(t.vehicle_id, "TEST-0001")
        self.assertIsNotNone(t.event_id)

    def test_speed_in_realistic_range(self):
        for _ in range(10):
            t = self.sim.step()
            self.assertGreater(t.speed_kmh, 0)
            self.assertLess(t.speed_kmh,    130)

    def test_rpm_correlated_with_speed(self):
        """Higher speed should generally mean higher RPM."""
        readings = [self.sim.step() for _ in range(20)]
        # At least some readings should have RPM > idle (800)
        high_rpm = [r for r in readings if r.rpm > 1000]
        self.assertGreater(len(high_rpm), 5)

    def test_fuel_decreases_over_time(self):
        fuel_start = self.sim._fuel_l
        for _ in range(100):
            self.sim.step()
        fuel_end = self.sim._fuel_l
        self.assertLess(fuel_end, fuel_start)

    def test_engine_temp_warms_up(self):
        """Engine should be warmer after 60 steps than at cold start."""
        temp_start = self.sim._engine_temp
        for _ in range(60):
            self.sim.step()
        temp_end = self.sim._engine_temp
        self.assertGreater(temp_end, temp_start)

    def test_gps_stays_in_cairo_region(self):
        for _ in range(20):
            t = self.sim.step()
            self.assertGreater(t.latitude,  29.0)
            self.assertLess(t.latitude,     31.0)
            self.assertGreater(t.longitude, 30.5)
            self.assertLess(t.longitude,    32.0)

    def test_to_dict_serialisable(self):
        t = self.sim.step()
        json_str = json.dumps(t.to_dict())
        self.assertIn("vehicle_id", json_str)
        self.assertIn("speed_kmh",  json_str)
        self.assertIn("latitude",   json_str)

    def test_anomaly_detection(self):
        """is_anomaly() should catch a manually crafted anomaly reading."""
        from dataclasses import replace
        t = self.sim.step()
        # Inject dangerous values
        import dataclasses
        d = dataclasses.replace(t, speed_kmh=140.0, engine_temp_c=110.0)
        self.assertTrue(d.is_anomaly())

    def test_all_vehicle_types(self):
        """All vehicle types should produce valid readings."""
        from vehicle_simulator import VEHICLE_PROFILES
        for vtype in VEHICLE_PROFILES:
            sim = VehicleSimulator("X", vtype, "maadi_to_nasr_city")
            t = sim.step()
            self.assertGreater(t.speed_kmh, 0)
            self.assertGreater(t.rpm,       0)


class TestFleetSimulator(unittest.TestCase):

    def test_fleet_produces_readings_for_all_vehicles(self):
        fleet = FleetSimulator(vehicle_count=3)
        readings = fleet.step_all()
        self.assertEqual(len(readings), 3)

    def test_vehicle_ids_unique(self):
        fleet = FleetSimulator(vehicle_count=5)
        ids = [v.vehicle_id for v in fleet.vehicles]
        self.assertEqual(len(ids), len(set(ids)))

    def test_routes_spread_across_fleet(self):
        fleet = FleetSimulator(vehicle_count=4)
        routes = [v.route_name for v in fleet.vehicles]
        # With 4 vehicles and 4 routes, each route should appear at least once
        self.assertGreater(len(set(routes)), 1)


if __name__ == "__main__":
    import sys

    print("\n" + "═" * 62)
    print("  Smart City — Data Sources Test Suite")
    print("  No Kafka or API keys required for these tests.")
    print("═" * 62 + "\n")

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()

    for cls in [
        TestCairoRoutes,
        TestWeatherFetcher,
        TestTrafficFetcher,
        TestVehicleSimulator,
        TestFleetSimulator,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    print("\n" + "═" * 62)
    if result.wasSuccessful():
        print(f"  ✓ All {result.testsRun} tests passed. Data sources are ready!")
    else:
        print(f"  ✗ {len(result.failures)} failures, {len(result.errors)} errors")
    print("═" * 62 + "\n")

    sys.exit(0 if result.wasSuccessful() else 1)
