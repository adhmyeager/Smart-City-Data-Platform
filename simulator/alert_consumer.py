"""
simulator/alert_consumer.py  (v2 — writes to Kafka + PostgreSQL)

Reads vehicle-telemetry from Kafka, checks thresholds,
writes alerts to:
  1. Kafka alerts topic  (for downstream consumers)
  2. PostgreSQL realtime_alerts table  (for Grafana dashboard)

Run:
  python simulator\\alert_consumer.py

The PostgreSQL connection uses the same sc_postgres container
already running in your Docker stack.
"""

import json
import time
import signal
import logging
import argparse
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("alert_consumer")

# ── Thresholds (match config.py exactly) ─────────────────────
T_SPEED       = 120.0
T_ENGINE_TEMP = 105.0
T_FUEL        = 10.0
T_RPM         = 5000
T_HARD_BRAKE  = -4.0
T_IDLE_TEMP   = 98.0

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_IN        = "vehicle-telemetry"
TOPIC_OUT       = "alerts"

# PostgreSQL connection (sc_postgres container)
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "airflow"
PG_USER = "airflow"
PG_PASS = "airflow"

stats = {"processed": 0, "alerts_sent": 0, "pg_written": 0, "started_at": time.time()}


# ─────────────────────────────────────────────────────────────
# Alert detection
# ─────────────────────────────────────────────────────────────

def check_thresholds(payload: dict) -> list:
    alerts = []
    vid   = payload.get("vehicle_id", "UNKNOWN")
    ts    = payload.get("timestamp_iso", datetime.now(timezone.utc).isoformat())
    ts_u  = payload.get("timestamp_unix", int(time.time()))
    speed = float(payload.get("speed_kmh", 0) or 0)
    temp  = float(payload.get("engine_temp_c", 0) or 0)
    fuel  = float(payload.get("fuel_level_pct", 100) or 100)
    rpm   = int(payload.get("rpm", 0) or 0)
    accel = float(payload.get("acceleration_ms2", 0) or 0)
    lat   = float(payload.get("latitude", 0) or 0)
    lon   = float(payload.get("longitude", 0) or 0)

    def make_alert(rule, alert_type, severity):
        return {
            "alert_id":        f"{vid}_{ts_u}_{rule}",
            "vehicle_id":      vid,
            "alert_type":      alert_type,
            "severity":        severity,
            "rule_name":       rule,
            "timestamp":       ts,
            "timestamp_unix":  ts_u,
            "speed_kmh":       round(speed, 2),
            "engine_temp_c":   round(temp, 1),
            "fuel_pct":        round(fuel, 2),
            "rpm":             rpm,
            "acceleration_ms2": round(accel, 3),
            "latitude":        round(lat, 6),
            "longitude":       round(lon, 6),
            "road_type":       payload.get("road_type", ""),
            "vehicle_type":    payload.get("vehicle_type", ""),
            "route_name":      payload.get("route_name", ""),
            "trip_id":         payload.get("trip_id", ""),
        }

    if speed > T_SPEED:
        alerts.append(make_alert("overspeed", "SPEED_ALERT", "HIGH"))
    if temp > T_ENGINE_TEMP:
        alerts.append(make_alert("engine_overheat", "ENGINE_TEMP_ALERT", "CRITICAL"))
    if fuel < T_FUEL:
        alerts.append(make_alert("low_fuel", "FUEL_ALERT", "MEDIUM"))
    if rpm > T_RPM:
        alerts.append(make_alert("high_rpm", "RPM_ALERT", "HIGH"))
    if accel < T_HARD_BRAKE:
        alerts.append(make_alert("hard_braking", "HARD_BRAKE_ALERT", "HIGH"))
    if temp > T_IDLE_TEMP and speed < 5:
        alerts.append(make_alert("idle_overheat", "IDLE_OVERHEAT_ALERT", "HIGH"))

    return alerts


# ─────────────────────────────────────────────────────────────
# PostgreSQL writer
# ─────────────────────────────────────────────────────────────

def connect_postgres():
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DB, user=PG_USER, password=PG_PASS,
        )
        conn.autocommit = True
        log.info("PostgreSQL connected ✅")
        return conn
    except Exception as e:
        log.warning(f"PostgreSQL connection failed: {e} — alerts will not appear in Grafana")
        return None


def write_alert_to_pg(conn, alert: dict) -> bool:
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO realtime_alerts
              (alert_id, vehicle_id, alert_type, severity, rule_name,
               ts, speed_kmh, engine_temp_c, fuel_pct, rpm,
               latitude, longitude, road_type, vehicle_type, route_name)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            alert["alert_id"],    alert["vehicle_id"],  alert["alert_type"],
            alert["severity"],    alert["rule_name"],
            alert["timestamp"],   alert["speed_kmh"],   alert["engine_temp_c"],
            alert["fuel_pct"],    alert["rpm"],
            alert["latitude"],    alert["longitude"],   alert["road_type"],
            alert["vehicle_type"],alert["route_name"],
        ))
        cur.close()
        return True
    except Exception as e:
        log.warning(f"PG write error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False


# ─────────────────────────────────────────────────────────────
# Also write telemetry to PostgreSQL for Grafana vehicle panel
# ─────────────────────────────────────────────────────────────

def ensure_telemetry_table(conn):
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_telemetry (
                id              SERIAL PRIMARY KEY,
                vehicle_id      VARCHAR(50),
                vehicle_type    VARCHAR(20),
                route_name      VARCHAR(100),
                ts              TIMESTAMP,
                speed_kmh       FLOAT,
                engine_temp_c   FLOAT,
                fuel_level_pct  FLOAT,
                rpm             INTEGER,
                traffic_density INTEGER,
                latitude        FLOAT,
                longitude       FLOAT,
                road_type       VARCHAR(20),
                road_event      VARCHAR(50),
                created_at      TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_tel_ts
                ON realtime_telemetry(ts DESC);
            CREATE INDEX IF NOT EXISTS idx_tel_vehicle
                ON realtime_telemetry(vehicle_id, ts DESC);
        """)
        cur.close()
        log.info("realtime_telemetry table ready")
    except Exception as e:
        log.warning(f"Could not create telemetry table: {e}")


def write_telemetry_to_pg(conn, payload: dict, batch_buffer: list) -> None:
    """Buffer telemetry and write every 10 readings to reduce PG load."""
    batch_buffer.append(payload)
    if len(batch_buffer) < 10:
        return
    if conn is None:
        batch_buffer.clear()
        return
    try:
        cur = conn.cursor()
        for p in batch_buffer:
            ts_raw = p.get("timestamp_iso", "")
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO realtime_telemetry
                  (vehicle_id, vehicle_type, route_name, ts,
                   speed_kmh, engine_temp_c, fuel_level_pct, rpm,
                   traffic_density, latitude, longitude,
                   road_type, road_event)
                VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s)
            """, (
                p.get("vehicle_id"),    p.get("vehicle_type"), p.get("route_name"), ts,
                p.get("speed_kmh"),     p.get("engine_temp_c"),p.get("fuel_level_pct"),
                p.get("rpm"),
                p.get("traffic_density"), p.get("latitude"),   p.get("longitude"),
                p.get("road_type"),     p.get("road_event"),
            ))
        cur.close()
        batch_buffer.clear()
    except Exception as e:
        log.warning(f"Telemetry batch write error: {e}")
        batch_buffer.clear()


# ─────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────

def run_kafka():
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import NoBrokersAvailable

    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                TOPIC_IN,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                group_id="python-alert-consumer-v2",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
            )
            log.info("Kafka connected ✅")
            break
        except NoBrokersAvailable:
            log.warning(f"No broker (attempt {attempt}/5), retrying in {2**attempt}s...")
            time.sleep(2 ** attempt)
    else:
        log.error("Could not connect to Kafka")
        return

    pg_conn       = connect_postgres()
    tel_buffer    = []
    running       = True
    last_stats    = time.time()

    # Ensure telemetry table exists
    ensure_telemetry_table(pg_conn)

    def _shutdown(sig, frame):
        nonlocal running
        log.warning("Shutdown — stopping...")
        running = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("=" * 55)
    log.info("  Smart City — Alert Consumer v2")
    log.info(f"  Kafka  : {TOPIC_IN} → {TOPIC_OUT}")
    log.info(f"  Grafana: PostgreSQL realtime_alerts table")
    log.info("=" * 55)

    while running:
        try:
            batch = consumer.poll(timeout_ms=1000, max_records=100)
            for tp, messages in batch.items():
                for msg in messages:
                    payload = msg.value
                    stats["processed"] += 1

                    # Write telemetry to PG (buffered)
                    write_telemetry_to_pg(pg_conn, payload, tel_buffer)

                    # Check alert thresholds
                    alerts = check_thresholds(payload)
                    for alert in alerts:
                        # Send to Kafka alerts topic
                        producer.send(TOPIC_OUT, alert,
                                     key=alert["vehicle_id"].encode())
                        stats["alerts_sent"] += 1

                        # Write to PostgreSQL for Grafana
                        if write_alert_to_pg(pg_conn, alert):
                            stats["pg_written"] += 1

                        log.warning(
                            f"🚨 [{alert['severity']:8s}] "
                            f"{alert['vehicle_id']} | "
                            f"{alert['alert_type']:25s} | "
                            f"speed={alert['speed_kmh']:.1f} "
                            f"temp={alert['engine_temp_c']:.1f}°C "
                            f"fuel={alert['fuel_pct']:.1f}%"
                        )

            if time.time() - last_stats >= 30:
                elapsed = time.time() - stats["started_at"]
                log.info(
                    f"[Stats] processed={stats['processed']:,} "
                    f"({stats['processed']/max(elapsed,1):.1f}/s) | "
                    f"alerts={stats['alerts_sent']} | "
                    f"pg_rows={stats['pg_written']} | "
                    f"runtime={elapsed:.0f}s"
                )
                last_stats = time.time()

        except Exception as e:
            if running:
                log.error(f"Error: {e}")
                time.sleep(1)

    consumer.close()
    producer.flush()
    producer.close()
    if pg_conn:
        pg_conn.close()
    elapsed = time.time() - stats["started_at"]
    log.info(f"Stopped. processed={stats['processed']:,} "
             f"alerts={stats['alerts_sent']} pg_rows={stats['pg_written']} "
             f"runtime={elapsed:.0f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        log.info("DRY-RUN: use run_kafka() for real mode")
    else:
        run_kafka()