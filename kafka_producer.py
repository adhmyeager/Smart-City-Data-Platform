"""
smart_city/simulator/kafka_producer.py

Main entry point for the Smart City simulator.
Sends vehicle telemetry, weather, and traffic data to Kafka topics.

Run modes:
  python kafka_producer.py           — full Kafka mode
  python kafka_producer.py --dry-run — print JSON to console (no Kafka needed)

Kafka topics written:
  vehicle-telemetry   (every EMIT_INTERVAL seconds per vehicle)
  weather-data        (every 5 minutes, cached)
  traffic-events      (every 60 seconds per location)
  alerts              (only when anomaly detected)
"""

from __future__ import annotations

import sys
import json
import time
import signal
import logging
import argparse
import datetime
from typing import Optional

import colorlog

from config import (
    KAFKA_BOOTSTRAP_SERVERS, TOPICS,
    VEHICLE_COUNT, EMIT_INTERVAL, DEFAULT_ROUTE,
)
from vehicle_simulator import FleetSimulator, VehicleTelemetry
from weather_fetcher import fetch_weather
from traffic_fetcher import fetch_traffic


# ─── Logging setup ────────────────────────────────────────────

def setup_logging(verbose: bool = False) -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "red,bg_white",
        }
    ))
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(handlers=[handler], level=level)


logger = logging.getLogger(__name__)


# ─── Kafka producer wrapper ───────────────────────────────────

class SmartCityProducer:
    """
    Wraps kafka-python KafkaProducer with:
      - JSON serialisation
      - Retry logic (exponential back-off)
      - Dry-run mode (no Kafka connection needed)
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run  = dry_run
        self._producer = None

        if not dry_run:
            self._connect()

    def _connect(self, retries: int = 5) -> None:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        for attempt in range(1, retries + 1):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
                    acks              = "all",
                    retries           = 3,
                    compression_type  = "gzip",
                    linger_ms         = 100,        # micro-batch up to 100ms
                    batch_size        = 16384,
                )
                logger.info(f"[Kafka] Connected to {KAFKA_BOOTSTRAP_SERVERS}")
                return
            except NoBrokersAvailable:
                wait = 2 ** attempt
                logger.warning(f"[Kafka] No broker (attempt {attempt}/{retries}), "
                               f"retry in {wait}s …")
                time.sleep(wait)

        logger.error("[Kafka] Could not connect after retries. Use --dry-run to test without Kafka.")
        sys.exit(1)

    def send(self, topic: str, payload: dict, key: Optional[str] = None) -> None:
        if self.dry_run:
            print(f"[DRY-RUN] topic={topic} | {json.dumps(payload)[:120]} …")
            return

        try:
            self._producer.send(
                topic,
                value = payload,
                key   = key.encode("utf-8") if key else None,
            )
        except Exception as e:
            logger.error(f"[Kafka] Send error on topic {topic}: {e}")

    def flush(self) -> None:
        if self._producer:
            self._producer.flush()

    def close(self) -> None:
        if self._producer:
            self._producer.close()
            logger.info("[Kafka] Producer closed")


# ─── Stats tracker ────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.telemetry_sent = 0
        self.weather_sent   = 0
        self.traffic_sent   = 0
        self.alerts_sent    = 0
        self.started_at     = time.time()

    def report(self) -> None:
        elapsed = time.time() - self.started_at
        rate    = self.telemetry_sent / max(elapsed, 1)
        logger.info(
            f"[Stats] runtime={elapsed:.0f}s | "
            f"telemetry={self.telemetry_sent} ({rate:.1f}/s) | "
            f"weather={self.weather_sent} | "
            f"traffic={self.traffic_sent} | "
            f"alerts={self.alerts_sent}"
        )


# ─── Main loop ────────────────────────────────────────────────

def run(dry_run: bool = False, verbose: bool = False) -> None:
    setup_logging(verbose)

    logger.info("═" * 60)
    logger.info("  Smart City Data Platform — Vehicle Simulator")
    logger.info(f"  Vehicles      : {VEHICLE_COUNT}")
    logger.info(f"  Emit interval : {EMIT_INTERVAL}s")
    logger.info(f"  Kafka broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Dry-run mode  : {dry_run}")
    logger.info("═" * 60)

    producer = SmartCityProducer(dry_run=dry_run)
    fleet    = FleetSimulator(vehicle_count=VEHICLE_COUNT)
    stats    = Stats()

    # Tracking counters for throttled fetches
    last_weather_fetch  = 0.0
    last_traffic_fetch  = 0.0
    last_stats_report   = 0.0
    WEATHER_INTERVAL    = 300.0    # 5 min
    TRAFFIC_INTERVAL    = 60.0    # 1 min
    STATS_INTERVAL      = 30.0    # 30 sec

    # Graceful shutdown
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        logger.warning("\n[Main] Shutdown signal received — stopping …")
        running = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("[Main] Simulator started. Press Ctrl+C to stop.\n")

    loop_start = time.time()

    while running:
        tick_start = time.time()
        now_ts     = tick_start

        # ── 1. Fetch weather every 5 minutes ─────────────────
        if now_ts - last_weather_fetch >= WEATHER_INTERVAL:
            weather = fetch_weather("cairo")
            fleet.inject_weather(weather)
            producer.send(
                TOPICS["weather"],
                weather.to_dict(),
                key = "cairo",
            )
            stats.weather_sent += 1
            last_weather_fetch  = now_ts
            logger.info(f"[Weather] {weather.condition} {weather.temp_c}°C "
                        f"(source={weather.source})")

        # ── 2. Step all vehicles ──────────────────────────────
        readings: list[VehicleTelemetry] = fleet.step_all(dt=EMIT_INTERVAL)

        for reading in readings:
            payload = reading.to_dict()

            # Send telemetry
            producer.send(
                TOPICS["telemetry"],
                payload,
                key = reading.vehicle_id,
            )
            stats.telemetry_sent += 1

            # Send road event (if non-trivial)
            if reading.road_event != "NONE":
                event_payload = {
                    "event_id":    reading.event_id,
                    "vehicle_id":  reading.vehicle_id,
                    "event_type":  reading.road_event,
                    "latitude":    reading.latitude,
                    "longitude":   reading.longitude,
                    "road_type":   reading.road_type,
                    "timestamp":   reading.timestamp_iso,
                }
                producer.send(TOPICS["road_events"], event_payload)
                logger.warning(f"[Event] {reading.vehicle_id}: {reading.road_event} "
                               f"at ({reading.latitude:.4f}, {reading.longitude:.4f})")

            # Send alert if anomaly
            if reading.is_anomaly():
                alert_payload = {
                    "alert_id":     reading.event_id,
                    "vehicle_id":   reading.vehicle_id,
                    "timestamp":    reading.timestamp_iso,
                    "speed_kmh":    reading.speed_kmh,
                    "engine_temp_c":reading.engine_temp_c,
                    "fuel_pct":     reading.fuel_level_pct,
                    "rpm":          reading.rpm,
                    "latitude":     reading.latitude,
                    "longitude":    reading.longitude,
                }
                producer.send(TOPICS["alerts"], alert_payload, key=reading.vehicle_id)
                stats.alerts_sent += 1
                logger.warning(f"[ALERT] {reading.vehicle_id} anomaly detected!")

        # ── 3. Fetch traffic every 60 seconds ─────────────────
        if now_ts - last_traffic_fetch >= TRAFFIC_INTERVAL:
            for vehicle in fleet.vehicles:
                t = fetch_traffic(
                    lat=vehicle._engine_temp,     # placeholder — see below
                    lon=31.2357,
                )
                # Send traffic event
                producer.send(
                    TOPICS["traffic"],
                    t.to_dict(),
                )
            stats.traffic_sent += 1
            last_traffic_fetch  = now_ts

        # ── 4. Flush & sleep ──────────────────────────────────
        producer.flush()

        # ── 5. Log stats every 30 seconds ─────────────────────
        if now_ts - last_stats_report >= STATS_INTERVAL:
            stats.report()
            last_stats_report = now_ts

        # Precise sleep (compensate for processing time)
        elapsed  = time.time() - tick_start
        sleep_for = max(0.0, EMIT_INTERVAL - elapsed)
        time.sleep(sleep_for)

    # ── Shutdown ──────────────────────────────────────────────
    logger.info("[Main] Flushing and closing producer …")
    producer.flush()
    producer.close()
    stats.report()
    logger.info("[Main] Simulator stopped cleanly.")


# ─── Entry point ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart City vehicle simulator")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print messages to console without connecting to Kafka"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging"
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run, verbose=args.verbose)
