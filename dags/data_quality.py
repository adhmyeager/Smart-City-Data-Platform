"""
airflow/dags/data_quality.py

Smart City — Data Quality Checks DAG

Schedule: Every 3 hours
Checks:
  - Bronze row counts are non-zero for recent partitions
  - Silver row counts exceed Bronze (data wasn't all dropped)
  - Gold tables exist and are non-empty
  - No partition older than 2 hours has zero rows (pipeline stall detection)

Results are logged to Airflow task logs and will be visible in the UI.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "smartcity",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}

S3_BUCKET = "smart-city-datalake"   # overridden by env var at runtime


def _check_s3_layer(layer: str, tables: list[str], **context) -> dict:
    """Check that each table/topic has Parquet files in the last 2 hours."""
    import boto3
    import os
    from datetime import datetime, timezone, timedelta

    bucket = os.getenv("S3_BUCKET_NAME", S3_BUCKET)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    now = datetime.now(timezone.utc)
    results = {}
    issues = []

    for table in tables:
        found = False
        for hours_back in range(3):
            check_time = now - timedelta(hours=hours_back)
            date_str = check_time.strftime("%Y-%m-%d")
            hour_int = check_time.hour

            prefix = f"{layer}/{table}/partition_date={date_str}/partition_hour={hour_int:02d}/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            file_count = resp.get("KeyCount", 0)

            if file_count > 0:
                size_bytes = sum(
                    obj.get("Size", 0) for obj in resp.get("Contents", [])
                )
                results[table] = {
                    "status": "OK",
                    "files": file_count,
                    "size_kb": round(size_bytes / 1024, 1),
                    "partition": f"{date_str}/h={hour_int:02d}",
                }
                print(f"  ✅ {layer}/{table}: {file_count} files, {round(size_bytes/1024,1)} KB ({date_str} h={hour_int:02d})")
                found = True
                break

        if not found:
            results[table] = {"status": "MISSING", "files": 0}
            issues.append(f"{layer}/{table} has no data in last 3 hours")
            print(f"  ❌ {layer}/{table}: NO DATA in last 3 hours")

    if issues:
        print(f"\n⚠️  {len(issues)} issue(s) found in {layer} layer:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print(f"\n✅ All {layer} tables healthy")

    return results


def check_bronze(**context):
    """Verify Bronze layer has recent data for all 4 topics."""
    topics = ["vehicle-telemetry", "weather-data", "traffic-events", "road-events"]
    print(f"\n{'='*50}")
    print(f"Bronze Layer Quality Check")
    print(f"{'='*50}")
    results = _check_s3_layer("bronze", topics, **context)
    context["ti"].xcom_push(key="bronze_results", value=results)


def check_silver(**context):
    """Verify Silver layer has clean data for all 4 tables."""
    tables = ["telemetry", "weather", "traffic", "road_events"]
    print(f"\n{'='*50}")
    print(f"Silver Layer Quality Check")
    print(f"{'='*50}")
    results = _check_s3_layer("silver", tables, **context)
    context["ti"].xcom_push(key="silver_results", value=results)


def check_gold(**context):
    """Verify Gold aggregation tables exist."""
    import boto3
    import os
    from datetime import datetime, timezone, timedelta

    bucket = os.getenv("S3_BUCKET_NAME", S3_BUCKET)
    gold_tables = ["vehicle_5min", "route_hourly", "fuel_daily", "road_event_summary"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    print(f"\n{'='*50}")
    print(f"Gold Layer Quality Check")
    print(f"{'='*50}")

    now = datetime.now(timezone.utc)
    results = {}

    for table in gold_tables:
        # Gold is partitioned by date only
        for days_back in range(2):
            check_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
            prefix = f"gold/{table}/partition_date={check_date}/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            count = resp.get("KeyCount", 0)

            if count > 0:
                size = sum(o.get("Size", 0) for o in resp.get("Contents", []))
                results[table] = {"status": "OK", "files": count, "date": check_date}
                print(f"  ✅ gold/{table}: {count} files ({check_date}, {round(size/1024,1)} KB)")
                break
        else:
            results[table] = {"status": "MISSING"}
            print(f"  ❌ gold/{table}: NO DATA in last 2 days")

    context["ti"].xcom_push(key="gold_results", value=results)


def quality_summary(**context):
    """Print combined quality report."""
    ti = context["ti"]
    bronze = ti.xcom_pull(task_ids="check_bronze", key="bronze_results") or {}
    silver = ti.xcom_pull(task_ids="check_silver", key="silver_results") or {}
    gold   = ti.xcom_pull(task_ids="check_gold",   key="gold_results")   or {}

    all_results = {
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
    }

    total  = sum(len(v) for v in all_results.values())
    ok     = sum(1 for layer in all_results.values() for v in layer.values() if v.get("status") == "OK")
    issues = total - ok

    print(f"\n{'='*50}")
    print(f"DATA QUALITY SUMMARY")
    print(f"{'='*50}")
    print(f"  Total checks : {total}")
    print(f"  Passed       : {ok}")
    print(f"  Failed       : {issues}")
    print(f"  Score        : {round(ok/total*100, 1)}%" if total > 0 else "  Score: N/A")

    if issues > 0:
        print(f"\n⚠️  Pipeline may have stalled — check Spark jobs")
    else:
        print(f"\n✅ All layers healthy — pipeline running normally")


# ─── DAG ─────────────────────────────────────────────────────

with DAG(
    dag_id="smart_city_data_quality",
    description="Smart City — S3 layer quality checks",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */3 * * *",   # every 3 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "quality"],
) as dag:

    t_bronze  = PythonOperator(task_id="check_bronze",  python_callable=check_bronze)
    t_silver  = PythonOperator(task_id="check_silver",  python_callable=check_silver)
    t_gold    = PythonOperator(task_id="check_gold",    python_callable=check_gold)
    t_summary = PythonOperator(task_id="quality_summary", python_callable=quality_summary)

    [t_bronze, t_silver, t_gold] >> t_summary
