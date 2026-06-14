"""Test S3 connection and load silver data."""
import boto3
import pandas as pd
import logging
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get S3 config
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "smart-city-datalake")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "silver/")

print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID[:10]}...")
print(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY[:10]}...")
print(f"S3_BUCKET: {S3_BUCKET}")
print(f"S3_SILVER_PREFIX: {S3_SILVER_PREFIX}")
print(f"AWS_REGION: {AWS_REGION}\n")

try:
    # Create S3 client
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # Test connection
    logger.info("Testing S3 connection...")
    response = s3.head_bucket(Bucket=S3_BUCKET)
    logger.info("✓ S3 connection successful!")
    
    # List silver folders
    logger.info(f"\nListing objects in s3://{S3_BUCKET}/{S3_SILVER_PREFIX}...")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_SILVER_PREFIX, Delimiter="/")
    
    folders = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            folder_name = prefix["Prefix"].replace(S3_SILVER_PREFIX, "").rstrip("/")
            folders.append(folder_name)
            logger.info(f"  📁 {folder_name}/")
    
    if not folders:
        logger.warning("  ⚠️  No silver folders found!")
        sys.exit(1)
    
    # List parquet files in each folder
    logger.info(f"\n📦 Listing parquet files in silver folders...")
    for folder in folders:
        prefix = f"{S3_SILVER_PREFIX}{folder}/"
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
        
        files = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    files.append(obj["Key"])
        
        logger.info(f"  {folder}: {len(files)} parquet files")
        if files:
            logger.info(f"    └─ {files[0]}")
            
            # Try to read first file
            try:
                import io
                obj = s3.get_object(Bucket=S3_BUCKET, Key=files[0])
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                logger.info(f"    ✓ Sample: {df.shape[0]} rows × {df.shape[1]} columns")
                logger.info(f"    Columns: {list(df.columns)[:3]}...")
            except Exception as e:
                logger.error(f"    ✗ Error reading file: {e}")
    
    logger.info("\n✓ S3 Silver data is accessible!")
    
except Exception as e:
    logger.error(f"✗ Error: {e}")
    sys.exit(1)
