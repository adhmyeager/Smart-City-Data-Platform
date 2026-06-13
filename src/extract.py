import boto3
import pandas as pd
import io
import logging
from typing import List, Dict
from datetime import datetime, timedelta
from config import (
    S3_BUCKET, S3_SILVER_PREFIX, AWS_REGION, TRAINING_DAYS,
    ROAD_EVENTS_FOLDER, TELEMETRY_FOLDER, TRAFFIC_FOLDER, WEATHER_FOLDER
)

logger = logging.getLogger(__name__)
s3 = boto3.client("s3", region_name=AWS_REGION)


def list_parquet_files(folder: str) -> List[str]:
    """List all parquet files under a silver folder."""
    prefix = f"{S3_SILVER_PREFIX}{folder}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    logger.info(f"Found {len(keys)} files in {folder}")
    return keys


def read_parquet_from_s3(key: str) -> pd.DataFrame:
    """Read a single parquet file from S3 into a DataFrame."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def read_folder(folder: str, limit_days: int = TRAINING_DAYS) -> pd.DataFrame:
    """Read all parquet files from a silver folder, optionally filtered by date."""
    keys = list_parquet_files(folder)
    if not keys:
        logger.warning(f"No files found in {folder}")
        return pd.DataFrame()

    frames = []
    cutoff = datetime.utcnow() - timedelta(days=limit_days)

    for key in keys:
        try:
            df = read_parquet_from_s3(key)
            frames.append(df)
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # normalize timestamp column — adjust name if yours differs
    ts_col = next(
        (c for c in combined.columns if ("timestamp" in c.lower() or "time" in c.lower()) and "unix" not in c.lower()),
        None,
    )
    if ts_col:
        combined[ts_col] = pd.to_datetime(combined[ts_col], utc=True)
        combined = combined[combined[ts_col] >= pd.Timestamp(cutoff, tz="UTC")]
        combined = combined.rename(columns={ts_col: "timestamp"})

    return combined


def load_silver_data() -> Dict[str, pd.DataFrame]:
    """Load all relevant silver folders for alert prediction."""
    # SILVER:
    logger.info("Loading Silver data from S3...")
    return {
        "telemetry":   read_folder(TELEMETRY_FOLDER),
        "traffic":     read_folder(TRAFFIC_FOLDER),
        "weather":     read_folder(WEATHER_FOLDER),
        "road_events": read_folder(ROAD_EVENTS_FOLDER),
    }
