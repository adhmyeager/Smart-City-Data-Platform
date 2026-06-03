"""
spark_jobs/utils/s3_utils.py

S3 helper functions for the Smart City data lake.

Layer structure:
  s3://<bucket>/bronze/<topic>/date=yyyy-MM-dd/hour=HH/   ← raw Parquet, 24h retention
  s3://<bucket>/silver/telemetry/date=yyyy-MM-dd/hour=HH/ ← cleaned, 30d retention
  s3://<bucket>/gold/<agg_name>/date=yyyy-MM-dd/           ← aggregated, 1yr retention

All paths are deterministic so Airflow can trigger targeted dbt/Glue jobs.
"""

import os
from datetime import datetime, timezone
from typing import Optional


# ── Bucket name from env (set in docker-compose / .env) ──────

S3_BUCKET: str = os.getenv("S3_BUCKET_NAME", "smart-city-datalake")
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")


# ─────────────────────────────────────────────────────────────
# Path builders
# ─────────────────────────────────────────────────────────────

def bronze_path(topic: str, dt: Optional[datetime] = None) -> str:
    """
    s3://bucket/bronze/<topic>/date=yyyy-MM-dd/hour=HH/
    If dt is None, uses current UTC time.
    """
    dt = dt or datetime.now(timezone.utc)
    return (
        f"s3a://{S3_BUCKET}/bronze/{topic}"
        f"/date={dt.strftime('%Y-%m-%d')}"
        f"/hour={dt.strftime('%H')}"
    )


def silver_path(table: str, dt: Optional[datetime] = None) -> str:
    """
    s3://bucket/silver/<table>/date=yyyy-MM-dd/hour=HH/
    table examples: telemetry, weather, traffic, road_events
    """
    dt = dt or datetime.now(timezone.utc)
    return (
        f"s3a://{S3_BUCKET}/silver/{table}"
        f"/date={dt.strftime('%Y-%m-%d')}"
        f"/hour={dt.strftime('%H')}"
    )


def gold_path(agg_name: str, dt: Optional[datetime] = None) -> str:
    """
    s3://bucket/gold/<agg_name>/date=yyyy-MM-dd/
    agg_name examples: vehicle_5min, route_hourly, fuel_daily
    """
    dt = dt or datetime.now(timezone.utc)
    return (
        f"s3a://{S3_BUCKET}/gold/{agg_name}"
        f"/date={dt.strftime('%Y-%m-%d')}"
    )


def checkpoint_path(job_name: str) -> str:
    """
    s3://bucket/_checkpoints/<job_name>/
    Spark Streaming checkpoints — never delete manually.
    """
    return f"s3a://{S3_BUCKET}/_checkpoints/{job_name}"


# ─────────────────────────────────────────────────────────────
# Spark S3 configuration helper
# ─────────────────────────────────────────────────────────────

def configure_spark_s3(spark_builder):
    """
    Add S3A (hadoop-aws) configuration to a SparkSession builder.

    Usage:
        builder = SparkSession.builder.appName("MyJob")
        builder = configure_spark_s3(builder)
        spark   = builder.getOrCreate()
    """
    aws_key    = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region     = os.getenv("AWS_REGION", "us-east-1")

    return (
        spark_builder
        # S3A filesystem implementation
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key",    aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",    aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")

        # Performance tuning (multipart upload, connection pool)
        .config("spark.hadoop.fs.s3a.multipart.size",        "67108864")   # 64 MB
        .config("spark.hadoop.fs.s3a.fast.upload",           "true")
        .config("spark.hadoop.fs.s3a.connection.maximum",    "100")
        .config("spark.hadoop.fs.s3a.threads.max",           "20")

        # Committer — avoids rename issues on S3
    )


# ─────────────────────────────────────────────────────────────
# Extra JARs manifest (for reference / download script)
# ─────────────────────────────────────────────────────────────

REQUIRED_JARS = {
    "spark-sql-kafka":   "spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "kafka-clients":     "kafka-clients-3.5.0.jar",
    "hadoop-aws":        "hadoop-aws-3.3.4.jar",
    "aws-java-sdk":      "aws-java-sdk-bundle-1.12.261.jar",
}

MAVEN_BASE = "https://repo1.maven.org/maven2"

JAR_URLS = {
    "spark-sql-kafka": (
        f"{MAVEN_BASE}/org/apache/spark"
        f"/spark-sql-kafka-0-10_2.12/3.5.0"
        f"/spark-sql-kafka-0-10_2.12-3.5.0.jar"
    ),
    "kafka-clients": (
        f"{MAVEN_BASE}/org/apache/kafka"
        f"/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"
    ),
    "hadoop-aws": (
        f"{MAVEN_BASE}/org/apache/hadoop"
        f"/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ),
    "aws-java-sdk": (
        f"{MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle"
        f"/1.12.261/aws-java-sdk-bundle-1.12.261.jar"
    ),
}


if __name__ == "__main__":
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    print("S3 path examples (current UTC time):")
    print(f"  Bronze telemetry : {bronze_path('vehicle-telemetry', now)}")
    print(f"  Silver telemetry : {silver_path('telemetry', now)}")
    print(f"  Gold 5-min       : {gold_path('vehicle_5min', now)}")
    print(f"  Checkpoint       : {checkpoint_path('bronze_writer')}")
    print()
    print("Required JARs (download to spark/jars/):")
    for name, url in JAR_URLS.items():
        print(f"  {REQUIRED_JARS[name]}")
        print(f"    {url}")
