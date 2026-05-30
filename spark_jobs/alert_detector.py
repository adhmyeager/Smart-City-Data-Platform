"""
spark_jobs/alert_detector.py

Layer: Kafka vehicle-telemetry  →  Kafka alerts topic
       (real-time, sub-second latency path — bypasses S3 entirely)

Why a separate job?
  bronze_writer.py writes to S3 every 30s (micro-batch).
  Grafana/operators need alerts in seconds, not minutes.
  This job reads from Kafka directly and publishes anomalies
  back to the `alerts` Kafka topic immediately.

Alert rules (match simulator config.py):
  SPEED      speed_kmh      > 120.0
  ENGINE     engine_temp_c  > 105.0
  FUEL       fuel_level_pct < 10.0
  RPM        rpm            > 5000

Additional complex rules (not in simulator):
  RAPID_DECEL   acceleration_ms2 < -4.0  (hard braking / collision indicator)
  IDLE_OVERHEAT engine_temp_c > 98 AND speed_kmh < 5 (stuck in traffic, overheating)

Output to Kafka `alerts` topic — each row is a JSON alert with:
  alert_id, vehicle_id, alert_type, severity, timestamp,
  speed_kmh, engine_temp_c, fuel_pct, rpm, latitude, longitude

Run:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark_jobs/alert_detector.py
"""

import os
import sys
import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType, IntegerType

sys.path.insert(0, "/opt/spark_jobs")
from utils.schemas import TELEMETRY_SCHEMA
from utils.s3_utils import checkpoint_path, configure_spark_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("alert_detector")

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP:  str   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_IN:         str   = "vehicle-telemetry"
TOPIC_OUT:        str   = "alerts"
TRIGGER_SECONDS:  int   = int(os.getenv("ALERT_TRIGGER_SECONDS", "5"))   # near-real-time

# Thresholds (match config.py)
T_SPEED:       float = float(os.getenv("ALERT_SPEED_KMH",     "120.0"))
T_ENGINE_TEMP: float = float(os.getenv("ALERT_ENGINE_TEMP_C", "105.0"))
T_FUEL:        float = float(os.getenv("ALERT_FUEL_PCT",      "10.0"))
T_RPM:         int   = int(os.getenv("ALERT_RPM",             "5000"))

# Complex rule thresholds
T_HARD_BRAKE:  float = -4.0    # m/s²  (hard braking)
T_IDLE_TEMP:   float = 98.0    # °C    (overheating while stopped)


# ─────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-AlertDetector")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", "2")   # keep low for fast micro-batches
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.kafka.consumer.cache.enabled", "false")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Alert Detector")
    return spark


# ─────────────────────────────────────────────────────────────
# Alert rule engine
# ─────────────────────────────────────────────────────────────

# Each rule produces one row per triggered vehicle reading.
# Severity: CRITICAL > HIGH > MEDIUM > LOW

ALERT_RULES = [
    # (rule_name, alert_type, severity, condition_expr)
    (
        "overspeed",
        "SPEED_ALERT",
        "HIGH",
        F.col("speed_kmh") > T_SPEED,
    ),
    (
        "engine_overheat",
        "ENGINE_TEMP_ALERT",
        "CRITICAL",
        F.col("engine_temp_c") > T_ENGINE_TEMP,
    ),
    (
        "low_fuel",
        "FUEL_ALERT",
        "MEDIUM",
        F.col("fuel_level_pct") < T_FUEL,
    ),
    (
        "high_rpm",
        "RPM_ALERT",
        "HIGH",
        F.col("rpm") > T_RPM,
    ),
    (
        "hard_braking",
        "HARD_BRAKE_ALERT",
        "HIGH",
        F.col("acceleration_ms2") < T_HARD_BRAKE,
    ),
    (
        "idle_overheat",
        "IDLE_OVERHEAT_ALERT",
        "HIGH",
        (F.col("engine_temp_c") > T_IDLE_TEMP) & (F.col("speed_kmh") < 5),
    ),
]


def apply_alert_rules(df: DataFrame) -> DataFrame:
    """
    For each row that triggers one or more rules, produce one alert per rule.
    Returns a narrow DataFrame with only the fields needed for the alert payload.
    """
    alert_frames = []

    for rule_name, alert_type, severity, condition in ALERT_RULES:
        triggered = (
            df
            .filter(condition)
            .withColumn("alert_type", F.lit(alert_type))
            .withColumn("severity",   F.lit(severity))
            .withColumn("rule_name",  F.lit(rule_name))
            .select(
                "vehicle_id",
                "timestamp_iso",
                "timestamp_unix",
                "speed_kmh",
                "engine_temp_c",
                "fuel_level_pct",
                "rpm",
                "acceleration_ms2",
                "latitude",
                "longitude",
                "road_type",
                "road_event",
                "vehicle_type",
                "route_name",
                "trip_id",
                "alert_type",
                "severity",
                "rule_name",
            )
        )
        alert_frames.append(triggered)

    if not alert_frames:
        return df.limit(0)   # empty DataFrame with correct structure

    # Union all triggered rules
    from functools import reduce
    combined = reduce(DataFrame.union, alert_frames)
    return combined


def build_alert_payload(df: DataFrame) -> DataFrame:
    """
    Convert the alerts DataFrame to a JSON string column (`value`)
    suitable for writing directly to Kafka.
    Adds a unique alert_id per row.
    """
    # Generate unique alert_id (deterministic from vehicle_id + timestamp + rule)
    df = df.withColumn(
        "alert_id",
        F.concat(
            F.col("vehicle_id"), F.lit("_"),
            F.col("timestamp_unix").cast(StringType()), F.lit("_"),
            F.col("rule_name"),
        ),
    )

    # Build the JSON payload — only include fields Grafana needs
    alert_struct = F.struct(
        F.col("alert_id"),
        F.col("vehicle_id"),
        F.col("alert_type"),
        F.col("severity"),
        F.col("rule_name"),
        F.col("timestamp_iso").alias("timestamp"),
        F.col("timestamp_unix"),
        F.round(F.col("speed_kmh"),         2).alias("speed_kmh"),
        F.round(F.col("engine_temp_c"),     1).alias("engine_temp_c"),
        F.round(F.col("fuel_level_pct"),    2).alias("fuel_pct"),
        F.col("rpm"),
        F.round(F.col("acceleration_ms2"),  3).alias("acceleration_ms2"),
        F.round(F.col("latitude"),          6).alias("latitude"),
        F.round(F.col("longitude"),         6).alias("longitude"),
        F.col("road_type"),
        F.col("road_event"),
        F.col("vehicle_type"),
        F.col("route_name"),
        F.col("trip_id"),
    )

    return (
        df
        .withColumn("value", F.to_json(alert_struct))
        .withColumn("key",   F.col("vehicle_id"))   # partition Kafka by vehicle_id
        .select("key", "value")
    )


# ─────────────────────────────────────────────────────────────
# Streaming pipeline
# ─────────────────────────────────────────────────────────────

def run(spark: SparkSession):
    ckpt = checkpoint_path("alert_detector")

    log.info(f"Kafka source  : {KAFKA_BOOTSTRAP} topic={TOPIC_IN}")
    log.info(f"Kafka sink    : {TOPIC_OUT}")
    log.info(f"Trigger       : {TRIGGER_SECONDS}s")
    log.info(f"Checkpoint    : {ckpt}")

    # ── 1. Read raw telemetry from Kafka ─────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 2000)
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "spark-alert-detector")
        .load()
        .withColumn("value", F.col("value").cast(StringType()))
    )

    # ── 2. Parse JSON ─────────────────────────────────────────
    parsed_df = (
        raw_df
        .withColumn("payload", F.from_json(F.col("value"), TELEMETRY_SCHEMA))
        .select("payload.*")
        # Drop rows with null mandatory fields
        .dropna(subset=["vehicle_id", "timestamp_unix", "speed_kmh"])
    )

    # ── 3. Apply alert rules ──────────────────────────────────
    alerts_df = apply_alert_rules(parsed_df)

    # ── 4. Build Kafka payload ────────────────────────────────
    kafka_payload_df = build_alert_payload(alerts_df)

    # ── 5. Write alerts back to Kafka ─────────────────────────
    query = (
        kafka_payload_df.writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", ckpt)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName("alert_detector")
        .start()
    )

    log.info(f"Alert detector query started — id={query.id}")
    log.info("Monitoring for: " + ", ".join(r[0] for r in ALERT_RULES))

    # ── 6. Progress logging ───────────────────────────────────
    import time
    while query.isActive:
        progress = query.lastProgress
        if progress:
            num_rows   = progress.get("numInputRows", 0)
            batch_id   = progress.get("batchId", "?")
            batch_dur  = progress.get("durationMs", {}).get("triggerExecution", 0)
            log.info(
                f"Batch {batch_id} | "
                f"input={num_rows} rows | "
                f"duration={batch_dur}ms"
            )
        time.sleep(TRIGGER_SECONDS)

    query.awaitTermination()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Smart City — Alert Detector")
    log.info(f"  Kafka broker  : {KAFKA_BOOTSTRAP}")
    log.info(f"  Source topic  : {TOPIC_IN}")
    log.info(f"  Output topic  : {TOPIC_OUT}")
    log.info(f"  Trigger       : {TRIGGER_SECONDS}s")
    log.info("")
    log.info("  Alert thresholds:")
    log.info(f"    SPEED      > {T_SPEED} km/h")
    log.info(f"    ENGINE_TEMP> {T_ENGINE_TEMP} °C")
    log.info(f"    FUEL       < {T_FUEL} %")
    log.info(f"    RPM        > {T_RPM}")
    log.info(f"    HARD_BRAKE < {T_HARD_BRAKE} m/s²")
    log.info(f"    IDLE_OVERHEAT > {T_IDLE_TEMP} °C while stopped")
    log.info("=" * 60)

    spark = build_spark()
    try:
        run(spark)
    except KeyboardInterrupt:
        log.warning("Interrupted — stopping alert detector")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
