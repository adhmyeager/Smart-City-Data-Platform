"""
spark_jobs/utils/schemas.py

Shared PySpark schemas for all Smart City topics.
Mirrors the simulator dataclasses exactly — any field change in the
simulator MUST be reflected here.

Topics covered:
  vehicle-telemetry  →  TELEMETRY_SCHEMA
  weather-data       →  WEATHER_SCHEMA
  traffic-events     →  TRAFFIC_SCHEMA
  road-events        →  ROAD_EVENT_SCHEMA
  alerts             →  ALERT_SCHEMA
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType,
    TimestampType,
)

# ─────────────────────────────────────────────────────────────
# vehicle-telemetry  (mirrors VehicleTelemetry dataclass)
# ─────────────────────────────────────────────────────────────

TELEMETRY_SCHEMA = StructType([
    # Identifiers
    StructField("event_id",          StringType(),  nullable=False),
    StructField("vehicle_id",        StringType(),  nullable=False),
    StructField("vehicle_type",      StringType(),  nullable=True),
    StructField("route_name",        StringType(),  nullable=True),
    StructField("trip_id",           StringType(),  nullable=True),

    # Timestamps
    StructField("timestamp_iso",     StringType(),  nullable=True),
    StructField("timestamp_unix",    LongType(),    nullable=False),

    # Position
    StructField("latitude",          DoubleType(),  nullable=False),
    StructField("longitude",         DoubleType(),  nullable=False),
    StructField("altitude_m",        FloatType(),   nullable=True),
    StructField("heading_deg",       IntegerType(), nullable=True),
    StructField("gps_accuracy_m",    FloatType(),   nullable=True),

    # Motion
    StructField("speed_kmh",         FloatType(),   nullable=False),
    StructField("acceleration_ms2",  FloatType(),   nullable=True),

    # Powertrain
    StructField("rpm",               IntegerType(), nullable=False),
    StructField("gear",              IntegerType(), nullable=True),
    StructField("engine_temp_c",     FloatType(),   nullable=False),
    StructField("engine_on",         BooleanType(), nullable=True),

    # Fuel
    StructField("fuel_level_pct",    FloatType(),   nullable=False),
    StructField("fuel_consumed_l",   FloatType(),   nullable=True),
    StructField("fuel_rate_l100km",  FloatType(),   nullable=True),

    # Environment
    StructField("road_type",         StringType(),  nullable=True),
    StructField("road_event",        StringType(),  nullable=True),
    StructField("traffic_density",   IntegerType(), nullable=True),

    # Trip metadata
    StructField("odometer_km",       FloatType(),   nullable=True),
    StructField("trip_distance_km",  FloatType(),   nullable=True),
    StructField("engine_runtime_s",  IntegerType(), nullable=True),
])

# ─────────────────────────────────────────────────────────────
# weather-data  (mirrors WeatherReading dataclass)
# ─────────────────────────────────────────────────────────────

WEATHER_SCHEMA = StructType([
    StructField("location",           StringType(),  nullable=True),
    StructField("latitude",           DoubleType(),  nullable=True),
    StructField("longitude",          DoubleType(),  nullable=True),
    StructField("temp_c",             FloatType(),   nullable=True),
    StructField("feels_like_c",       FloatType(),   nullable=True),
    StructField("humidity_pct",       IntegerType(), nullable=True),
    StructField("wind_kmh",           FloatType(),   nullable=True),
    StructField("wind_direction_deg", IntegerType(), nullable=True),
    StructField("condition",          StringType(),  nullable=True),
    StructField("description",        StringType(),  nullable=True),
    StructField("visibility_km",      FloatType(),   nullable=True),
    StructField("uv_index",           FloatType(),   nullable=True),
    StructField("pressure_hpa",       IntegerType(), nullable=True),
    StructField("timestamp_unix",     LongType(),    nullable=True),
    StructField("source",             StringType(),  nullable=True),
])

# ─────────────────────────────────────────────────────────────
# traffic-events  (mirrors TrafficReading dataclass)
# ─────────────────────────────────────────────────────────────

TRAFFIC_SCHEMA = StructType([
    StructField("latitude",              DoubleType(),  nullable=True),
    StructField("longitude",             DoubleType(),  nullable=True),
    StructField("current_speed_kmh",     FloatType(),   nullable=True),
    StructField("free_flow_speed_kmh",   FloatType(),   nullable=True),
    StructField("congestion_ratio",      FloatType(),   nullable=True),
    StructField("traffic_density",       IntegerType(), nullable=True),
    StructField("confidence",            FloatType(),   nullable=True),
    StructField("road_closure",          BooleanType(), nullable=True),
    StructField("timestamp_unix",        LongType(),    nullable=True),
    StructField("source",                StringType(),  nullable=True),
])

# ─────────────────────────────────────────────────────────────
# road-events
# ─────────────────────────────────────────────────────────────

ROAD_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),  nullable=False),
    StructField("vehicle_id",  StringType(),  nullable=False),
    StructField("event_type",  StringType(),  nullable=True),
    StructField("latitude",    DoubleType(),  nullable=True),
    StructField("longitude",   DoubleType(),  nullable=True),
    StructField("road_type",   StringType(),  nullable=True),
    StructField("timestamp",   StringType(),  nullable=True),
])

# ─────────────────────────────────────────────────────────────
# alerts
# ─────────────────────────────────────────────────────────────

ALERT_SCHEMA = StructType([
    StructField("alert_id",      StringType(),  nullable=False),
    StructField("vehicle_id",    StringType(),  nullable=False),
    StructField("timestamp",     StringType(),  nullable=True),
    StructField("speed_kmh",     FloatType(),   nullable=True),
    StructField("engine_temp_c", FloatType(),   nullable=True),
    StructField("fuel_pct",      FloatType(),   nullable=True),
    StructField("rpm",           IntegerType(), nullable=True),
    StructField("latitude",      DoubleType(),  nullable=True),
    StructField("longitude",     DoubleType(),  nullable=True),
])

# ─────────────────────────────────────────────────────────────
# Silver-layer schema — adds derived columns
# ─────────────────────────────────────────────────────────────

SILVER_TELEMETRY_SCHEMA = StructType(
    TELEMETRY_SCHEMA.fields + [
        StructField("event_time",     TimestampType(), nullable=True),
        StructField("ingestion_time", TimestampType(), nullable=True),
        StructField("is_anomaly",     BooleanType(),   nullable=True),
        StructField("speed_band",     StringType(),    nullable=True),
        StructField("fuel_band",      StringType(),    nullable=True),
        StructField("partition_date", StringType(),    nullable=True),
        StructField("partition_hour", IntegerType(),   nullable=True),
    ]
)
