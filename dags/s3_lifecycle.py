"""
airflow/dags/s3_lifecycle.py

Smart City — S3 Lifecycle Management DAG

Schedule: Daily at 2 AM UTC
Tasks:
  1. report_s3_usage    — logs bucket size per layer
  2. enforce_bronze_ttl — deletes Bronze partitions older than 7 days
  3. enforce_silver_ttl — deletes Silver partitions older than 30 days
  4. cleanup_checkpoints — removes orphaned Spark checkpoint files > 14 days

NOTE: Gold and _checkpoints are NOT deleted by this DAG.
Gold retention (1 year) is handled by S3 bucket lifecycle rules.
This DAG only handles Bronze and Silver to keep costs low.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "smartcity",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

S3_BUCKET = "smart-city-datalake"

BRONZE_TTL_DAYS  = 7
SILVER_TTL_DAYS  = 30
CKPT_TTL_DAYS    = 14


def _get_s3_client():
    import boto3, os
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def report_s3_usage(**context):
    """Log bucket size and object count per layer."""
    import os

    s3 = _get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME", S3_BUCKET)
    layers = ["bronze", "silver", "gold", "_checkpoints"]

    print(f"\n{'='*50}")
    print(f"S3 Bucket Usage Report: s3://{bucket}")
    print(f"{'='*50}")

    total_size = 0
    total_objects = 0

    for layer in layers:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=f"{layer}/")

        layer_size = 0
        layer_count = 0
        for page in pages:
            for obj in page.get("Contents", []):
                layer_size += obj["Size"]
                layer_count += 1

        total_size += layer_size
        total_objects += layer_count
        print(f"  {layer:20s}: {layer_count:6d} files, {round(layer_size/1024/1024, 2):8.2f} MB")

    print(f"\n  {'TOTAL':20s}: {total_objects:6d} files, {round(total_size/1024/1024, 2):8.2f} MB")
    estimated_cost = round(total_size / 1024 / 1024 / 1024 * 0.023, 4)
    print(f"  Estimated S3 cost    : ~${estimated_cost}/month (S3 Standard)")


def delete_old_partitions(layer: str, topics_or_tables: list, ttl_days: int):
    """Delete S3 objects older than ttl_days for the given layer."""
    import os

    s3 = _get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME", S3_BUCKET)
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    total_deleted = 0

    print(f"\n{'='*50}")
    print(f"Cleaning {layer} layer (TTL={ttl_days} days)")
    print(f"Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*50}")

    for topic in topics_or_tables:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=f"{layer}/{topic}/")

        to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["LastModified"] < cutoff:
                    to_delete.append({"Key": obj["Key"]})

        if not to_delete:
            print(f"  {topic}: nothing to delete")
            continue

        # Delete in batches of 1000 (S3 API limit)
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i:i+1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            total_deleted += len(batch)

        print(f"  {topic}: deleted {len(to_delete)} files")

    print(f"\nTotal deleted from {layer}: {total_deleted} files")
    return total_deleted


def enforce_bronze_ttl(**context):
    """Delete Bronze partitions older than BRONZE_TTL_DAYS (default 7)."""
    topics = ["vehicle-telemetry", "weather-data", "traffic-events", "road-events"]
    count = delete_old_partitions("bronze", topics, BRONZE_TTL_DAYS)
    context["ti"].xcom_push(key="bronze_deleted", value=count)


def enforce_silver_ttl(**context):
    """Delete Silver partitions older than SILVER_TTL_DAYS (default 30)."""
    tables = ["telemetry", "weather", "traffic", "road_events"]
    count = delete_old_partitions("silver", tables, SILVER_TTL_DAYS)
    context["ti"].xcom_push(key="silver_deleted", value=count)


def lifecycle_summary(**context):
    """Print deletion summary."""
    ti = context["ti"]
    bronze_del = ti.xcom_pull(task_ids="enforce_bronze_ttl", key="bronze_deleted") or 0
    silver_del = ti.xcom_pull(task_ids="enforce_silver_ttl", key="silver_deleted") or 0

    print(f"\n{'='*50}")
    print(f"LIFECYCLE SUMMARY")
    print(f"{'='*50}")
    print(f"  Bronze files deleted : {bronze_del}")
    print(f"  Silver files deleted : {silver_del}")
    print(f"  Total cleaned up     : {bronze_del + silver_del}")
    print(f"  Run at               : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"\n✅ S3 lifecycle enforcement complete")


# ─── DAG ─────────────────────────────────────────────────────

with DAG(
    dag_id="smart_city_s3_lifecycle",
    description="Smart City — Delete old Bronze/Silver S3 partitions",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",    # 2 AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "s3", "lifecycle"],
) as dag:

    t_report  = PythonOperator(task_id="report_s3_usage",    python_callable=report_s3_usage)
    t_bronze  = PythonOperator(task_id="enforce_bronze_ttl", python_callable=enforce_bronze_ttl)
    t_silver  = PythonOperator(task_id="enforce_silver_ttl", python_callable=enforce_silver_ttl)
    t_summary = PythonOperator(task_id="lifecycle_summary",  python_callable=lifecycle_summary)

    t_report >> [t_bronze, t_silver] >> t_summary
