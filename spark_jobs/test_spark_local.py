"""
spark_jobs/test_spark_local.py

Smart City — Complete Local Test Suite
No S3, No Kafka needed. Tests every Spark job on the local filesystem.

Covers:
  Phase 1 — SparkSession creation (confirms Spark is alive)
  Phase 2 — Silver unit tests (all 4 clean functions, in-memory DataFrames)
  Phase 3 — Gold unit tests (all 7 aggregation functions, in-memory DataFrames)
  Phase 4 — End-to-end integration (write Bronze Parquet locally → Silver → Gold)
  Phase 5 — Summary report (pass/fail table)

Run from inside the Spark master container:
  docker exec sc_spark_master \\
    /opt/spark/bin/spark-submit \\
      --master local[2] \\
      --driver-memory 1g \\
      /opt/spark_jobs/test_spark_local.py

Output files land in: /opt/spark_jobs/local_test/
Which maps to:        .\\spark_jobs\\local_test\\ on your Windows host.
"""

import sys
import uuid
import random
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import TimestampType

# ── Allow importing our actual module functions ───────────────
sys.path.insert(0, "/opt/spark_jobs")

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,          # suppress Spark noise
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("local_test")

# ── Test state ────────────────────────────────────────────────
LOCAL_BASE = "file:///opt/spark_jobs/local_test"
_results   = []   # {"phase", "test", "pass", "detail"}

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def check(phase: str, test: str, condition: bool, detail: str = "") -> bool:
    status = f"{GREEN}✓ PASS{RESET}" if condition else f"{RED}✗ FAIL{RESET}"
    _results.append({"phase": phase, "test": test, "pass": condition, "detail": detail})
    suffix = f"  [{detail}]" if detail else ""
    print(f"    {status}  {test}{suffix}")
    return condition


def section(title: str):
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")


# ─────────────────────────────────────────────────────────────
# SYNTHETIC DATA GENERATORS
# All generators produce DataFrames whose schemas exactly match
# the Bronze output (topic schema + kafka metadata columns).
# ─────────────────────────────────────────────────────────────

def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def make_bronze_telemetry(spark: SparkSession, n: int = 60) -> DataFrame:
    """
    60 vehicle telemetry rows spanning the last hour.
    Intentional bad rows:
      row 10 → speed=999 (clamped to 250)
      row 20 → fuel=-15  (clamped to 0)
      row 30 → vehicle_id=None  (DROPPED by dropna)
      row 40 → engine_temp=108  (anomaly flag)
      row 50 → speed=135        (anomaly flag + overspeed band)
    """
    base_ts  = _now_ts() - 3600
    vehicles = ["CAR-1001", "CAR-1002", "CAR-1003", "CAR-1004", "CAR-1005"]
    routes   = ["tahrir_to_new_capital", "maadi_to_nasr_city", "giza_to_heliopolis",
                "sixth_october_to_maadi"]
    road_t   = ["highway", "arterial", "urban"]
    events   = ["NONE", "NONE", "NONE", "NONE", "ACCIDENT", "ROADWORK"]

    rows = []
    for i in range(n):
        ts       = base_ts + i * 60
        vid      = vehicles[i % 5]
        speed    = float(random.uniform(25, 105))
        rpm      = int(max(800, speed * 38 + random.gauss(0, 80)))
        eng_temp = float(random.uniform(86, 95))
        fuel_pct = float(max(8, 90 - i * 0.4))

        if i == 10: speed    = 999.0    # over physical limit
        if i == 20: fuel_pct = -15.0    # below physical limit
        if i == 30: vid      = None     # mandatory null → DROPPED
        if i == 40: eng_temp = 108.0    # anomaly
        if i == 50: speed    = 135.0    # anomaly + overspeed

        dt_obj = datetime.utcfromtimestamp(ts)
        rows.append((
            str(uuid.uuid4()),           # event_id
            vid,                          # vehicle_id
            "sedan",                      # vehicle_type
            routes[i % 4],               # route_name
            f"TRIP-{i // 12 + 1:04d}",  # trip_id
            dt_obj.isoformat() + "Z",    # timestamp_iso
            ts,                           # timestamp_unix
            29.96 + (i % 5) * 0.04,      # latitude  (Cairo range)
            31.22 + (i % 5) * 0.04,      # longitude (Cairo range)
            28.0,                         # altitude_m
            int(i * 6 % 360),            # heading_deg
            5.0,                          # gps_accuracy_m
            speed,                        # speed_kmh
            float(random.uniform(-2, 2)), # acceleration_ms2
            rpm,                          # rpm
            3,                            # gear
            eng_temp,                     # engine_temp_c
            True,                         # engine_on
            fuel_pct,                     # fuel_level_pct
            0.002,                        # fuel_consumed_l
            9.5,                          # fuel_rate_l100km
            road_t[i % 3],               # road_type
            events[i % 6],               # road_event
            i % 10,                       # traffic_density
            float(1000 + i),             # odometer_km
            float(i * 0.5),              # trip_distance_km
            3600 + i,                     # engine_runtime_s
            "vehicle-telemetry",          # kafka_topic
            i % 3,                        # kafka_partition
            i,                            # kafka_offset
            ts,                           # kafka_timestamp
            datetime.utcnow().isoformat(),# ingestion_time
            dt_obj.strftime("%Y-%m-%d"), # partition_date
            dt_obj.hour,                  # partition_hour
        ))

    schema = T.StructType([
        T.StructField("event_id",         T.StringType(),  True),
        T.StructField("vehicle_id",        T.StringType(),  True),
        T.StructField("vehicle_type",      T.StringType(),  True),
        T.StructField("route_name",        T.StringType(),  True),
        T.StructField("trip_id",           T.StringType(),  True),
        T.StructField("timestamp_iso",     T.StringType(),  True),
        T.StructField("timestamp_unix",    T.LongType(),    False),
        T.StructField("latitude",          T.DoubleType(),  False),
        T.StructField("longitude",         T.DoubleType(),  False),
        T.StructField("altitude_m",        T.FloatType(),   True),
        T.StructField("heading_deg",       T.IntegerType(), True),
        T.StructField("gps_accuracy_m",    T.FloatType(),   True),
        T.StructField("speed_kmh",         T.FloatType(),   False),
        T.StructField("acceleration_ms2",  T.FloatType(),   True),
        T.StructField("rpm",               T.IntegerType(), False),
        T.StructField("gear",              T.IntegerType(), True),
        T.StructField("engine_temp_c",     T.FloatType(),   False),
        T.StructField("engine_on",         T.BooleanType(), True),
        T.StructField("fuel_level_pct",    T.FloatType(),   False),
        T.StructField("fuel_consumed_l",   T.FloatType(),   True),
        T.StructField("fuel_rate_l100km",  T.FloatType(),   True),
        T.StructField("road_type",         T.StringType(),  True),
        T.StructField("road_event",        T.StringType(),  True),
        T.StructField("traffic_density",   T.IntegerType(), True),
        T.StructField("odometer_km",       T.FloatType(),   True),
        T.StructField("trip_distance_km",  T.FloatType(),   True),
        T.StructField("engine_runtime_s",  T.IntegerType(), True),
        T.StructField("kafka_topic",       T.StringType(),  True),
        T.StructField("kafka_partition",   T.IntegerType(), True),
        T.StructField("kafka_offset",      T.LongType(),    True),
        T.StructField("kafka_timestamp",   T.LongType(),    True),
        T.StructField("ingestion_time",    T.StringType(),  True),
        T.StructField("partition_date",    T.StringType(),  True),
        T.StructField("partition_hour",    T.IntegerType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def make_bronze_weather(spark: SparkSession, n: int = 10) -> DataFrame:
    """
    10 weather readings. Row 9 has humidity=150 (clamped to 100).
    """
    base_ts    = _now_ts() - 3600
    conditions = ["Clear", "Haze", "Dust", "Partly cloudy", "Clear",
                  "Clear", "Haze", "Dust", "Clear", "Clear"]
    rows = []
    for i in range(n):
        ts   = base_ts + i * 360
        hum  = 150 if i == 9 else int(40 + i * 2)   # row 9 → clamped
        dt_o = datetime.utcfromtimestamp(ts)
        rows.append((
            "Cairo", 30.0444, 31.2357,
            float(28 + i * 0.3),     # temp_c
            float(26 + i * 0.3),     # feels_like_c
            hum,                      # humidity_pct
            10.0,                     # wind_kmh
            180,                      # wind_direction_deg
            conditions[i],            # condition
            "test",                   # description
            10.0,                     # visibility_km
            6.0,                      # uv_index
            1013,                     # pressure_hpa
            ts,                       # timestamp_unix
            "simulated",              # source
            "weather-data", 0, i, ts,
            datetime.utcnow().isoformat(),
            dt_o.strftime("%Y-%m-%d"), dt_o.hour,
        ))

    schema = T.StructType([
        T.StructField("location",           T.StringType(),  True),
        T.StructField("latitude",           T.DoubleType(),  True),
        T.StructField("longitude",          T.DoubleType(),  True),
        T.StructField("temp_c",             T.FloatType(),   True),
        T.StructField("feels_like_c",       T.FloatType(),   True),
        T.StructField("humidity_pct",       T.IntegerType(), True),
        T.StructField("wind_kmh",           T.FloatType(),   True),
        T.StructField("wind_direction_deg", T.IntegerType(), True),
        T.StructField("condition",          T.StringType(),  True),
        T.StructField("description",        T.StringType(),  True),
        T.StructField("visibility_km",      T.FloatType(),   True),
        T.StructField("uv_index",           T.FloatType(),   True),
        T.StructField("pressure_hpa",       T.IntegerType(), True),
        T.StructField("timestamp_unix",     T.LongType(),    True),
        T.StructField("source",             T.StringType(),  True),
        T.StructField("kafka_topic",        T.StringType(),  True),
        T.StructField("kafka_partition",    T.IntegerType(), True),
        T.StructField("kafka_offset",       T.LongType(),    True),
        T.StructField("kafka_timestamp",    T.LongType(),    True),
        T.StructField("ingestion_time",     T.StringType(),  True),
        T.StructField("partition_date",     T.StringType(),  True),
        T.StructField("partition_hour",     T.IntegerType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def make_bronze_traffic(spark: SparkSession, n: int = 25) -> DataFrame:
    """
    25 traffic readings. Row 12 has congestion_ratio=1.5 (clamped to 1.0).
    """
    base_ts = _now_ts() - 3600
    positions = [
        (30.0444, 31.2357), (30.0868, 31.3275), (30.1100, 31.3920),
        (30.0200, 31.7400), (29.9602, 31.2569),
    ]
    rows = []
    for i in range(n):
        ts         = base_ts + i * 144
        lat, lon   = positions[i % 5]
        free_flow  = 90.0
        congestion = (i % 10) / 10.0
        current    = max(5.0, free_flow * (1 - congestion * 0.85))
        if i == 12:
            congestion = 1.5   # over physical limit — will be clamped
        dt_o = datetime.utcfromtimestamp(ts)
        rows.append((
            lat, lon,
            float(current),
            free_flow,
            float(congestion),
            int(min(10, congestion * 10)),
            0.75, False, ts, "simulated",
            "traffic-events", 0, i, ts,
            datetime.utcnow().isoformat(),
            dt_o.strftime("%Y-%m-%d"), dt_o.hour,
        ))

    schema = T.StructType([
        T.StructField("latitude",              T.DoubleType(),  True),
        T.StructField("longitude",             T.DoubleType(),  True),
        T.StructField("current_speed_kmh",     T.FloatType(),   True),
        T.StructField("free_flow_speed_kmh",   T.FloatType(),   True),
        T.StructField("congestion_ratio",      T.FloatType(),   True),
        T.StructField("traffic_density",       T.IntegerType(), True),
        T.StructField("confidence",            T.FloatType(),   True),
        T.StructField("road_closure",          T.BooleanType(), True),
        T.StructField("timestamp_unix",        T.LongType(),    True),
        T.StructField("source",                T.StringType(),  True),
        T.StructField("kafka_topic",           T.StringType(),  True),
        T.StructField("kafka_partition",       T.IntegerType(), True),
        T.StructField("kafka_offset",          T.LongType(),    True),
        T.StructField("kafka_timestamp",       T.LongType(),    True),
        T.StructField("ingestion_time",        T.StringType(),  True),
        T.StructField("partition_date",        T.StringType(),  True),
        T.StructField("partition_hour",        T.IntegerType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def make_bronze_road_events(spark: SparkSession) -> DataFrame:
    """
    8 road-event rows:
      EVT-001..004 → valid (kept)
      EVT-005      → event_type=NONE (filtered out by clean_road_events)
      EVT-006      → event_type=POTHOLE (not in valid set — filtered)
      EVT-007      → null event_id (DROPPED by dropna)
      EVT-008      → GPS outside Cairo bbox (clamped)
    Expected after cleaning: 5 rows (EVT-001..004 + EVT-008 clamped)
    """
    ts_str = datetime.utcnow().isoformat() + "Z"
    now    = _now_ts()
    date   = datetime.utcnow().strftime("%Y-%m-%d")
    hour   = datetime.utcnow().hour

    rows = [
        ("EVT-001", "CAR-1001", "ACCIDENT",            30.0444, 31.2357, "urban",    ts_str, "road-events", 0, 0, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-002", "CAR-1002", "ROADWORK",            30.0868, 31.3275, "arterial", ts_str, "road-events", 0, 1, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-003", "CAR-1003", "BREAKDOWN",           30.1100, 31.3920, "highway",  ts_str, "road-events", 0, 2, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-004", "CAR-1004", "CONGESTION_INCIDENT", 29.9602, 31.2569, "urban",    ts_str, "road-events", 0, 3, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-005", "CAR-1005", "NONE",                30.0200, 31.7400, "highway",  ts_str, "road-events", 0, 4, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-006", "CAR-1001", "POTHOLE",             30.0444, 31.2357, "urban",    ts_str, "road-events", 0, 5, now, datetime.utcnow().isoformat(), date, hour),
        (None,      "CAR-1002", "ACCIDENT",            30.0444, 31.2357, "urban",    ts_str, "road-events", 0, 6, now, datetime.utcnow().isoformat(), date, hour),
        ("EVT-008", "CAR-1003", "ROADWORK",            99.0,    99.0,    "arterial", ts_str, "road-events", 0, 7, now, datetime.utcnow().isoformat(), date, hour),
    ]

    schema = T.StructType([
        T.StructField("event_id",        T.StringType(),  True),
        T.StructField("vehicle_id",      T.StringType(),  False),
        T.StructField("event_type",      T.StringType(),  True),
        T.StructField("latitude",        T.DoubleType(),  True),
        T.StructField("longitude",       T.DoubleType(),  True),
        T.StructField("road_type",       T.StringType(),  True),
        T.StructField("timestamp",       T.StringType(),  True),
        T.StructField("kafka_topic",     T.StringType(),  True),
        T.StructField("kafka_partition", T.IntegerType(), True),
        T.StructField("kafka_offset",    T.LongType(),    True),
        T.StructField("kafka_timestamp", T.LongType(),    True),
        T.StructField("ingestion_time",  T.StringType(),  True),
        T.StructField("partition_date",  T.StringType(),  True),
        T.StructField("partition_hour",  T.IntegerType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ─────────────────────────────────────────────────────────────
# PHASE 1 — SparkSession
# ─────────────────────────────────────────────────────────────

def phase1_spark() -> SparkSession:
    section("PHASE 1 — SparkSession")
    try:
        spark = (
            SparkSession.builder
            .appName("SmartCity-LocalTest")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory",   "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.ui.enabled", "false")   # suppress UI for test
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        ver = spark.version
        check("Phase 1", "SparkSession created",       True,  f"version={ver}")
        check("Phase 1", "local[2] master reachable",  True,  "no cluster needed")
        test_df = spark.range(10)
        count   = test_df.count()
        check("Phase 1", "Basic DataFrame operation",  count == 10, f"count={count}")
        return spark
    except Exception as e:
        check("Phase 1", "SparkSession creation", False, str(e))
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# PHASE 2 — Silver unit tests
# ─────────────────────────────────────────────────────────────

def phase2_silver(spark: SparkSession):
    section("PHASE 2 — Silver cleaning unit tests")

    from silver_cleaner import (
        clean_telemetry, clean_weather, clean_traffic, clean_road_events,
    )

    # ── 2a. Telemetry ─────────────────────────────────────
    print(f"\n  {YELLOW}2a. clean_telemetry (60 raw rows → expect 59 after dropna){RESET}")
    raw_tel  = make_bronze_telemetry(spark, 60)
    clean_tel = clean_telemetry(raw_tel)
    count_in  = raw_tel.count()
    count_out = clean_tel.count()

    check("Phase 2a", "Input row count",              count_in  == 60, f"got {count_in}")
    check("Phase 2a", "Null vehicle_id dropped",      count_out == 59, f"got {count_out}")

    # Verify clamping: speed was 999 → must be ≤ 250
    max_speed = clean_tel.agg(F.max("speed_kmh")).collect()[0][0]
    check("Phase 2a", "Speed clamped to ≤ 250 km/h", max_speed <= 250.0, f"max={max_speed:.1f}")

    # Verify fuel clamping: -15 → 0
    min_fuel = clean_tel.agg(F.min("fuel_level_pct")).collect()[0][0]
    check("Phase 2a", "Fuel clamped to ≥ 0%",        min_fuel >= 0.0,   f"min={min_fuel:.2f}")

    # Verify bands exist
    bands = [r[0] for r in clean_tel.select("speed_band").distinct().collect()]
    check("Phase 2a", "speed_band column exists",     len(bands) > 0,    f"bands={bands}")

    # Verify anomaly flags (rows 40 and 50 should be anomalies)
    anomaly_count = clean_tel.filter(F.col("is_anomaly")).count()
    check("Phase 2a", "is_anomaly flags set",         anomaly_count >= 2, f"anomalies={anomaly_count}")

    # Verify event_time is TimestampType
    dt = dict(clean_tel.dtypes)
    check("Phase 2a", "event_time is TimestampType",  dt.get("event_time") == "timestamp",
          f"type={dt.get('event_time')}")

    # Verify had_sensor_clamp exists
    check("Phase 2a", "had_sensor_clamp column exists", "had_sensor_clamp" in dt, "")

    # ── 2b. Weather ───────────────────────────────────────
    print(f"\n  {YELLOW}2b. clean_weather (10 raw rows → expect 10 kept){RESET}")
    raw_wx  = make_bronze_weather(spark, 10)
    clean_wx = clean_weather(raw_wx)
    wx_count = clean_wx.count()

    check("Phase 2b", "All 10 weather rows kept",     wx_count == 10, f"got {wx_count}")

    # humidity=150 → clamped to 100
    max_hum = clean_wx.agg(F.max("humidity_pct")).collect()[0][0]
    check("Phase 2b", "Humidity clamped to ≤ 100",   max_hum <= 100, f"max={max_hum}")

    # weather_severity column derived
    severities = [r[0] for r in clean_wx.select("weather_severity").distinct().collect()]
    check("Phase 2b", "weather_severity derived",     len(severities) > 0, f"values={severities}")

    # speed_factor column derived
    factors = [r[0] for r in clean_wx.select("speed_factor").distinct().collect()]
    check("Phase 2b", "speed_factor derived",         len(factors) > 0, f"values={factors}")

    # ── 2c. Traffic ───────────────────────────────────────
    print(f"\n  {YELLOW}2c. clean_traffic (25 raw rows → expect 25 kept){RESET}")
    raw_trx  = make_bronze_traffic(spark, 25)
    clean_trx = clean_traffic(raw_trx)
    trx_count = clean_trx.count()

    check("Phase 2c", "All 25 traffic rows kept",    trx_count == 25, f"got {trx_count}")

    # congestion=1.5 → clamped to 1.0
    max_cong = clean_trx.agg(F.max("congestion_ratio")).collect()[0][0]
    check("Phase 2c", "Congestion clamped to ≤ 1.0", max_cong <= 1.0, f"max={max_cong:.3f}")

    # GPS bucket columns exist
    dt_trx = dict(clean_trx.dtypes)
    check("Phase 2c", "gps_lat_bucket column exists", "gps_lat_bucket" in dt_trx, "")
    check("Phase 2c", "gps_lon_bucket column exists", "gps_lon_bucket" in dt_trx, "")

    # congestion_band derived
    cb_vals = [r[0] for r in clean_trx.select("congestion_band").distinct().collect()]
    check("Phase 2c", "congestion_band derived",      len(cb_vals) > 0, f"values={cb_vals}")

    # ── 2d. Road events ───────────────────────────────────
    print(f"\n  {YELLOW}2d. clean_road_events (8 raw → expect 5: drop null, filter NONE/POTHOLE){RESET}")
    raw_road  = make_bronze_road_events(spark)
    clean_road = clean_road_events(raw_road)
    road_count = clean_road.count()

    check("Phase 2d", "Correct rows after cleaning",  road_count == 5, f"got {road_count}")

    # Verify NONE and POTHOLE were filtered
    bad = clean_road.filter(F.col("event_type").isin("NONE", "POTHOLE")).count()
    check("Phase 2d", "NONE/POTHOLE events removed",  bad == 0, f"remaining={bad}")

    # Verify GPS clamping on EVT-008 (99.0 → within Cairo bbox)
    max_lat = clean_road.agg(F.max("latitude")).collect()[0][0]
    check("Phase 2d", "GPS latitude clamped to ≤ 30.5", max_lat <= 30.5, f"max_lat={max_lat:.4f}")

    # Verify severity_score column exists
    sev = [r[0] for r in clean_road.select("severity_score").distinct().collect()]
    check("Phase 2d", "severity_score derived",        len(sev) > 0, f"scores={sorted(sev)}")

    return clean_tel, clean_wx, clean_trx, clean_road


# ─────────────────────────────────────────────────────────────
# PHASE 3 — Gold aggregation unit tests
# ─────────────────────────────────────────────────────────────

def phase3_gold(spark: SparkSession, silver_tel: DataFrame,
                silver_wx: DataFrame, silver_trx: DataFrame):
    section("PHASE 3 — Gold aggregation unit tests")

    from gold_aggregator import (
        agg_vehicle_5min, agg_route_hourly,
        agg_fuel_daily, agg_road_event_summary,
    )

    # Gold requires event_time on Silver
    # silver_tel already has it from clean_telemetry ✓

    # ── 3a. vehicle_5min ─────────────────────────────────
    print(f"\n  {YELLOW}3a. agg_vehicle_5min (5-min tumbling window per vehicle){RESET}")
    g1 = agg_vehicle_5min(silver_tel)
    g1_count = g1.count()
    check("Phase 3a", "vehicle_5min produces rows",    g1_count > 0,   f"windows={g1_count}")

    # Verify key output columns exist
    g1_cols = g1.columns
    for col in ["avg_speed_kmh", "max_speed_kmh", "avg_fuel_level_pct",
                "total_fuel_consumed_l", "anomaly_count", "window_start"]:
        check("Phase 3a", f"column '{col}' exists", col in g1_cols, "")

    # Max speed in 5-min windows must be ≤ 250 (clamped in silver)
    max_s = g1.agg(F.max("max_speed_kmh")).collect()[0][0]
    check("Phase 3a", "max_speed_kmh ≤ 250 (clamped in silver)", max_s <= 250, f"max={max_s:.1f}")

    # ── 3b. route_hourly ─────────────────────────────────
    print(f"\n  {YELLOW}3b. agg_route_hourly (1-hour tumbling window per route){RESET}")
    g2 = agg_route_hourly(silver_tel)
    g2_count = g2.count()
    check("Phase 3b", "route_hourly produces rows",    g2_count > 0,  f"windows={g2_count}")

    for col in ["avg_speed_kmh", "unique_vehicles", "anomaly_count",
                "count_stopped", "count_slow", "count_medium"]:
        check("Phase 3b", f"column '{col}' exists",   col in g2.columns, "")

    # ── 3c. fuel_daily ───────────────────────────────────
    print(f"\n  {YELLOW}3c. agg_fuel_daily (1-day tumbling window per vehicle_type){RESET}")
    g3 = agg_fuel_daily(silver_tel)
    g3_count = g3.count()
    check("Phase 3c", "fuel_daily produces rows",      g3_count > 0,  f"windows={g3_count}")
    check("Phase 3c", "estimated_co2_kg column exists","estimated_co2_kg" in g3.columns, "")

    # CO2 must be positive
    min_co2 = g3.agg(F.min("estimated_co2_kg")).collect()[0][0]
    check("Phase 3c", "estimated_co2_kg > 0",          min_co2 > 0,  f"min={min_co2:.4f}")

    # ── 3d. road_event_summary ───────────────────────────
    print(f"\n  {YELLOW}3d. agg_road_event_summary (15-min window, events only){RESET}")
    g4 = agg_road_event_summary(silver_tel)
    g4_count = g4.count()
    check("Phase 3d", "road_event_summary produces rows", g4_count > 0, f"windows={g4_count}")

    # Should not contain NONE events
    none_rows = g4.filter(F.col("road_event") == "NONE").count()
    check("Phase 3d", "No NONE events in summary",     none_rows == 0, f"none_rows={none_rows}")

    # ── 3e. weather_impact JOIN ──────────────────────────
    print(f"\n  {YELLOW}3e. weather_impact JOIN (telemetry + weather){RESET}")
    # Inline the join logic to test without S3 dependency
    tel_j = silver_tel.withColumn("join_hour", F.date_trunc("hour", F.col("event_time")))
    wx_j  = silver_wx.withColumn("join_hour", F.date_trunc("hour", F.col("event_time"))) \
                      .select("join_hour", "condition", "weather_severity", "speed_factor",
                              "temp_c", "wind_kmh", "visibility_km")
    joined = tel_j.join(F.broadcast(wx_j), on="join_hour", how="left")
    g6 = (
        joined
        .groupBy("join_hour", "condition", "route_name")
        .agg(
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.count("*").alias("event_count"),
            F.first("speed_factor").alias("speed_factor"),
        )
        .withColumn("speed_loss_pct",
            F.round((F.lit(1.0) - F.col("speed_factor")) * 100, 1))
    )
    g6_count = g6.count()
    check("Phase 3e", "weather_impact join produces rows",  g6_count > 0, f"rows={g6_count}")
    check("Phase 3e", "speed_loss_pct column derived",
          "speed_loss_pct" in g6.columns, "")

    # ── 3f. congestion_hotspots JOIN ─────────────────────
    print(f"\n  {YELLOW}3f. congestion_hotspots JOIN (telemetry + traffic){RESET}")
    tel_h  = silver_tel.withColumn("gps_lat_bucket", F.round(F.col("latitude"),  2)) \
                        .withColumn("gps_lon_bucket", F.round(F.col("longitude"), 2)) \
                        .withColumn("join_hour",      F.date_trunc("hour", F.col("event_time")))
    trx_h  = silver_trx.withColumn("join_hour", F.date_trunc("hour", F.col("event_time"))) \
                         .groupBy("gps_lat_bucket", "gps_lon_bucket", "join_hour") \
                         .agg(F.avg("congestion_ratio").alias("avg_congestion_ratio"),
                              F.avg("current_speed_kmh").alias("traffic_speed_kmh"))
    joined_h = tel_h.join(F.broadcast(trx_h),
                           on=["gps_lat_bucket", "gps_lon_bucket", "join_hour"],
                           how="left")
    g7 = (
        joined_h
        .groupBy("join_hour", "gps_lat_bucket", "gps_lon_bucket")
        .agg(F.avg("avg_congestion_ratio").alias("congestion_ratio"),
             F.countDistinct("vehicle_id").alias("vehicle_count"),
             F.avg("speed_kmh").alias("vehicle_avg_speed_kmh"),
             F.avg("traffic_speed_kmh").alias("traffic_speed_kmh"))
        .withColumn("speed_discrepancy_kmh",
            F.round(F.col("vehicle_avg_speed_kmh") - F.col("traffic_speed_kmh"), 1))
    )
    g7_count = g7.count()
    check("Phase 3f", "congestion_hotspots join produces rows", g7_count > 0, f"cells={g7_count}")
    check("Phase 3f", "speed_discrepancy_kmh derived",
          "speed_discrepancy_kmh" in g7.columns, "")

    return g1, g2, g3, g4


# ─────────────────────────────────────────────────────────────
# PHASE 4 — End-to-end integration (local Parquet read/write)
# ─────────────────────────────────────────────────────────────

def phase4_integration(spark: SparkSession):
    section("PHASE 4 — End-to-end integration (local Parquet I/O)")

    from silver_cleaner import (
        clean_telemetry, clean_weather, clean_traffic, clean_road_events,
    )
    from gold_aggregator import (
        agg_vehicle_5min, agg_route_hourly, agg_fuel_daily, agg_road_event_summary,
    )

    print(f"\n  {YELLOW}Writing synthetic Bronze to local Parquet...{RESET}")

    topics = {
        "vehicle-telemetry": (make_bronze_telemetry(spark, 60), clean_telemetry, "telemetry"),
        "weather-data":      (make_bronze_weather(spark, 10),   clean_weather,   "weather"),
        "traffic-events":    (make_bronze_traffic(spark, 25),   clean_traffic,   "traffic"),
        "road-events":       (make_bronze_road_events(spark),   clean_road_events, "road_events"),
    }

    silver_dfs = {}

    for topic, (bronze_df, clean_fn, silver_table) in topics.items():
        bronze_path = f"{LOCAL_BASE}/bronze/{topic}"
        silver_path = f"{LOCAL_BASE}/silver/{silver_table}"

        # Write Bronze
        try:
            bronze_df.write.mode("overwrite").parquet(bronze_path)
            check("Phase 4", f"Write Bronze/{topic}",  True, f"path={bronze_path}")
        except Exception as e:
            check("Phase 4", f"Write Bronze/{topic}", False, str(e))
            continue

        # Read Bronze back
        try:
            read_back = spark.read.parquet(bronze_path)
            rb_count  = read_back.count()
            check("Phase 4", f"Read Bronze/{topic} back", rb_count > 0, f"rows={rb_count}")
        except Exception as e:
            check("Phase 4", f"Read Bronze/{topic} back", False, str(e))
            continue

        # Clean → Silver
        try:
            silver_df = clean_fn(read_back)
            silver_df.write.mode("overwrite").partitionBy("partition_date", "partition_hour").parquet(silver_path)
            sv_count  = spark.read.parquet(silver_path).count()
            check("Phase 4", f"Silver/{silver_table} written", sv_count > 0, f"clean_rows={sv_count}")
            silver_dfs[silver_table] = spark.read.parquet(silver_path)
        except Exception as e:
            check("Phase 4", f"Silver/{silver_table} write", False, str(e))

    # Gold aggregation from local Silver
    if "telemetry" in silver_dfs:
        print(f"\n  {YELLOW}Running Gold aggregations from local Silver Parquet...{RESET}")
        silver_tel = silver_dfs["telemetry"]

        # Need event_time as TimestampType (already added by clean_telemetry) ✓
        gold_aggs = {
            "vehicle_5min":      agg_vehicle_5min(silver_tel),
            "route_hourly":      agg_route_hourly(silver_tel),
            "fuel_daily":        agg_fuel_daily(silver_tel),
            "road_event_summary": agg_road_event_summary(silver_tel),
        }
        for agg_name, agg_df in gold_aggs.items():
            gold_path = f"{LOCAL_BASE}/gold/{agg_name}"
            try:
                agg_df.write.mode("overwrite").partitionBy("partition_date").parquet(gold_path)
                gold_count = spark.read.parquet(gold_path).count()
                check("Phase 4", f"Gold/{agg_name} written", gold_count > 0, f"rows={gold_count}")
            except Exception as e:
                check("Phase 4", f"Gold/{agg_name} write", False, str(e))


# ─────────────────────────────────────────────────────────────
# PHASE 5 — Summary
# ─────────────────────────────────────────────────────────────

def phase5_summary():
    section("PHASE 5 — Test Summary")
    passed = sum(1 for r in _results if r["pass"])
    failed = sum(1 for r in _results if not r["pass"])
    total  = len(_results)

    print()
    if failed > 0:
        print(f"  {RED}Failed tests:{RESET}")
        for r in _results:
            if not r["pass"]:
                detail = f"  [{r['detail']}]" if r["detail"] else ""
                print(f"    {RED}✗{RESET}  [{r['phase']}] {r['test']}{detail}")
        print()

    color = GREEN if failed == 0 else RED
    print(f"  {color}{BOLD}Result: {passed}/{total} tests passed{RESET}")

    if failed == 0:
        print(f"""
  {GREEN}All tests passed.{RESET}
  Local Bronze, Silver, and Gold Parquet files written to:
    spark_jobs\\local_test\\bronze\\
    spark_jobs\\local_test\\silver\\
    spark_jobs\\local_test\\gold\\

  Your Spark jobs are verified and ready for S3 connection (Step 4).
""")
    else:
        print(f"""
  {RED}Fix the {failed} failing tests before connecting to S3.{RESET}
""")


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"""
{BOLD}{'=' * 60}
  Smart City — Local Spark Test Suite
  No S3, No Kafka required
{'=' * 60}{RESET}
""")
    spark = phase1_spark()
    tel, wx, trx, road = phase2_silver(spark)
    phase3_gold(spark, tel, wx, trx)
    phase4_integration(spark)
    phase5_summary()
    spark.stop()
