"""Simple train script with explicit output."""
import sys
import os

# Ensure we can import from src
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 70, flush=True)
print("STARTING TRAINING PIPELINE", flush=True)
print("=" * 70, flush=True)

try:
    print("\n[1/5] Importing modules...", flush=True)
    import pandas as pd
    import numpy as np
    import xgboost as xgb
    from sklearn.metrics import (
        classification_report, roc_auc_score,
        precision_score, recall_score, f1_score,
    )
    print("  ✓ All modules imported", flush=True)
    
    print("\n[2/5] Importing config...", flush=True)
    from src.config import (
        MLFLOW_TRACKING_URI, EXPERIMENT_NAME,
        MODEL_PATH, ALERT_THRESHOLD, MODEL_DIR, DATA_DIR
    )
    print(f"  ✓ MODEL_PATH: {MODEL_PATH}", flush=True)
    print(f"  ✓ PREDICTIONS_DB: {DATA_DIR}/predictions.db", flush=True)
    
    # Create directories if they don't exist
    os.makedirs(MODEL_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"  ✓ Created directories", flush=True)
    
    print("\n[3/5] Loading extract module...", flush=True)
    from src.extract import load_silver_data
    print("  ✓ Extract module loaded", flush=True)
    
    print("\n[4/5] Loading features module...", flush=True)
    from src.features import engineer_features, get_feature_columns
    print("  ✓ Features module loaded", flush=True)
    
    print("\n[5/5] Loading silver data from S3...", flush=True)
    data = load_silver_data()
    print("  ✓ Silver data loaded", flush=True)
    
    for key, df in data.items():
        if not df.empty:
            print(f"    - {key}: {df.shape[0]} rows × {df.shape[1]} cols", flush=True)
        else:
            print(f"    - {key}: EMPTY", flush=True)
    
    print("\n[ENGINEERING] Extracting features...", flush=True)
    df = engineer_features(data)
    print(f"  ✓ Feature matrix: {df.shape[0]} rows × {df.shape[1]} cols", flush=True)
    
    feature_cols = get_feature_columns(df)
    print(f"  ✓ Selected {len(feature_cols)} features", flush=True)
    
    if "will_have_incident" not in df.columns:
        print("  ⚠ Target column 'will_have_incident' not found!", flush=True)
        print(f"  Available columns: {df.columns.tolist()}", flush=True)
        print("\n✗ CANNOT TRAIN: Missing target variable", flush=True)
    else:
        print(f"  ✓ Target variable found: {df['will_have_incident'].sum()} positive", flush=True)
        print("\n✓ TRAINING PIPELINE READY!", flush=True)
    
except Exception as e:
    print(f"\n✗ ERROR: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)
