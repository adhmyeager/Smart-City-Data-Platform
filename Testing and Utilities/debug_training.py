"""Debug training pipeline step by step."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import MODEL_PATH, PREDICTIONS_DB
from src.extract import load_silver_data
from src.features import engineer_features, get_feature_columns
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info("=" * 60)
    logger.info("STEP 1: Loading silver data from S3...")
    logger.info("=" * 60)
    data = load_silver_data()
    for key, df in data.items():
        if not df.empty:
            logger.info(f"  ✓ {key}: {df.shape[0]} rows × {df.shape[1]} columns")
        else:
            logger.info(f"  ✗ {key}: EMPTY!")
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Engineering features...")
    logger.info("=" * 60)
    df = engineer_features(data)
    logger.info(f"  ✓ Feature matrix: {df.shape[0]} rows × {df.shape[1]} columns")
    logger.info(f"  Columns: {list(df.columns)[:5]}...")
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Getting feature columns...")
    logger.info("=" * 60)
    feature_cols = get_feature_columns(df)
    logger.info(f"  ✓ {len(feature_cols)} feature columns")
    
    if "will_have_incident" in df.columns:
        logger.info(f"  ✓ Target variable found: {df['will_have_incident'].sum()} positive class")
    else:
        logger.warning(f"  ✗ Target 'will_have_incident' not found!")
        logger.info(f"  Available columns: {df.columns.tolist()}")
    
    logger.info("\n✓ All steps completed successfully!")
    
except Exception as e:
    logger.error(f"✗ Error: {e}", exc_info=True)
    sys.exit(1)
