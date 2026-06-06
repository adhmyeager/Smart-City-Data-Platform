"""
spark_jobs/bronze_writer.py

Layer: Kafka  →  S3 Bronze  (raw, no transformation)

What it does:
  - Reads all 4 producer topics from Kafka (vehicle-telemetry, weather-data,
    traffic-events, road-events) as raw JSON strings.
  - Parses each JSON against its known schema.
  - Writes raw Parquet files to S3 Bronze layer, partitioned by date + hour.
  - One Spark Structured Streaming query per topic (parallel micro-batch).
  - Uses S3A multipart upload + directory committer (no rename issues).

Run inside the Spark master container:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
             /opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar \
      /opt/spark_jobs/bronze_writer.py

Environment variables required (already in docker-compose):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME
  KAFKA_BOOTSTRAP_SERVERS  (defaults to kafka:29092)
"""

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Allow imports from spark_jobs/utils when running via spark-submit ─
sys.path.insert(0, "/opt/spark_jobs")
from utils.schemas import (
    TELEMETRY_SCHEMA, WEATHER_SCHEMA,
    TRAFFIC_SCHEMA, ROAD_EVENT_SCHEMA,
)
from utils.s3_utils import (
    bronze_path, checkpoint_path, configure_spark_s3,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("bronze_writer")

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

TOPICS = {
    "vehicle-telemetry": TELEMETRY_SCHEMA,
    "weather-data":      WEATHER_SCHEMA,
    "traffic-events":    TRAFFIC_SCHEMA,
    "road-events":       ROAD_EVENT_SCHEMA,
}

# Micro-batch trigger: process any new data every N seconds.
# 30s is a good balance — small files vs write overhead on S3.
TRIGGER_SECONDS: int = int(os.getenv("BRONZE_TRIGGER_SECONDS", "30"))

# How many Kafka offsets to read per micro-batch (back-pressure)
MAX_OFFSETS_PER_TRIGGER: int = int(os.getenv("BRONZE_MAX_OFFSETS", "5000"))


# ─────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-BronzeWriter")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))

        # Kafka source settings
        .config("spark.sql.streaming.kafka.consumer.cache.enabled", "false")

        # Shuffle partitions — keep low for streaming (avoid tiny tasks)
        .config("spark.sql.shuffle.partitions", "4")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Bronze Writer")
    return spark


# ─────────────────────────────────────────────────────────────
# Per-topic streaming query
# ─────────────────────────────────────────────────────────────

def build_kafka_stream(spark: SparkSession, topic: str):
    """
    Read a single Kafka topic as a raw Structured Stream.
    Returns a DataFrame with columns: value(string), kafka_timestamp, partition, offset.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")          # don't replay history on restart
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")            # continue if offsets expired
        .option("kafka.group.id", f"spark-bronze-{topic}")
        .load()
        # Kafka value is binary → decode to string
        .withColumn("value", F.col("value").cast(StringType()))
        # Keep Kafka metadata for lineage
        .withColumn("kafka_topic",     F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset",    F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        # Drop raw binary key (we use vehicle_id inside the JSON payload)
        .drop("key", "topic", "partition", "offset", "timestamp", "timestampType")
    )


def parse_json_stream(raw_df, schema, topic: str):
    """
    Parse the JSON 'value' column against the given schema.
    Keeps the raw value alongside for debugging.
    Adds ingestion_time and partition_date/hour columns.
    """
    parsed = (
        raw_df
        .withColumn("payload",        F.from_json(F.col("value"), schema))
        .withColumn("ingestion_time", F.current_timestamp())
        # Partition columns derived from Kafka arrival time (not event time)
        # — ensures data is always written even if event timestamps drift
        .withColumn("partition_date", F.date_format(F.col("kafka_timestamp"), "yyyy-MM-dd"))
        .withColumn("partition_hour", F.hour(F.col("kafka_timestamp")))
        # Flatten payload fields to top level
        .select(
            "payload.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_time",
            "partition_date",
            "partition_hour",
        )
    )
    return parsed


def start_bronze_query(spark: SparkSession, topic: str, schema):
    """
    Build and start a streaming query that writes one Kafka topic to S3 Bronze.
    Returns the StreamingQuery object.
    """
    # S3 path is fixed at job start; Parquet writer appends within the folder.
    # Each micro-batch writes its own files — no data is overwritten.
    s3_dest = f"s3a://{os.getenv('S3_BUCKET_NAME', 'smart-city-datalake')}/bronze/{topic}"
    ckpt    = checkpoint_path(f"bronze_{topic.replace('-', '_')}")

    log.info(f"[{topic}] Starting bronze query → {s3_dest}")
    log.info(f"[{topic}] Checkpoint           → {ckpt}")

    raw_df    = build_kafka_stream(spark, topic)
    parsed_df = parse_json_stream(raw_df, schema, topic)

    query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path",              s3_dest)
        .option("checkpointLocation", ckpt)
        # Partition on date + hour → efficient downstream reads
        .partitionBy("partition_date", "partition_hour")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(f"bronze_{topic.replace('-', '_')}")
        .start()
    )

    log.info(f"[{topic}] Query started — id={query.id}")
    return query


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Smart City — Bronze Writer")
    log.info(f"  Kafka broker   : {KAFKA_BOOTSTRAP}")
    log.info(f"  S3 bucket      : {os.getenv('S3_BUCKET_NAME', 'NOT SET')}")
    log.info(f"  Trigger        : {TRIGGER_SECONDS}s")
    log.info(f"  Max offsets    : {MAX_OFFSETS_PER_TRIGGER}/trigger")
    log.info("=" * 60)

    spark = build_spark()

    queries = []
    for topic, schema in TOPICS.items():
        try:
            q = start_bronze_query(spark, topic, schema)
            queries.append(q)
        except Exception as e:
            log.error(f"[{topic}] Failed to start query: {e}", exc_info=True)
            raise

    log.info(f"All {len(queries)} bronze queries running. Awaiting termination …")

    # Block until all queries stop (or one fails)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.warning("Interrupted — stopping all queries …")
        for q in queries:
            q.stop()

    log.info("Bronze Writer stopped.")


if __name__ == "__main__":
    main()