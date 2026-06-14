# ml/src/predict.py
import pandas as pd
import numpy as np
import xgboost as xgb
import sqlite3
import logging
from datetime import datetime, timezone

from config import MODEL_PATH, PREDICTIONS_DB, ALERT_THRESHOLD
from extract import load_silver_data
from features import engineer_features, get_feature_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_db():
    """Create predictions table if it doesn't exist."""
    con = sqlite3.connect(PREDICTIONS_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            segment         TEXT,
            alert_prob      REAL,
            will_alert      INTEGER,
            speed           REAL,
            congestion      REAL,
            weather_risk    REAL,
            active_events   REAL,
            scored_at       TEXT
        )
    """)
    con.commit()
    con.close()


def save_predictions(df_preds: pd.DataFrame):
    """Write predictions dataframe to SQLite."""
    con = sqlite3.connect(PREDICTIONS_DB)
    df_preds.to_sql("predictions", con, if_exists="append", index=False)
    con.close()
    logger.info(f"Saved {len(df_preds)} predictions to {PREDICTIONS_DB}")


def run_prediction():
    init_db()

    # ── load model ────────────────────────────────────────────────────────────
    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    logger.info("Model loaded")

    # ── load fresh silver data ──────────────────────────────────────────────────
    data = load_silver_data()
    df   = engineer_features(data)

    feature_cols = get_feature_columns(df)

    # keep only columns the model was trained on
    model_features = model.get_booster().feature_names
    missing = set(model_features) - set(df.columns)
    if missing:
        logger.warning(f"Missing features at inference: {missing} — filling with 0")
        for col in missing:
            df[col] = 0

    X = df[model_features]

    # ── score ─────────────────────────────────────────────────────────────────
    proba = model.predict_proba(X)[:, 1]

    now = datetime.now(timezone.utc).isoformat()

    seg_col  = next((c for c in df.columns if "segment" in c.lower()
                     or "location" in c.lower()), None)

    results = pd.DataFrame({
        "timestamp":     df["timestamp"].astype(str),
        "segment":       df[seg_col].astype(str) if seg_col else "unknown",
        "alert_prob":    np.round(proba, 4),
        "will_alert":    (proba >= ALERT_THRESHOLD).astype(int),
        "speed":         df.get("speed",         pd.Series(np.nan, index=df.index)),
        "congestion":    df.get("congestion_ratio",     pd.Series(np.nan, index=df.index)),
        "weather_risk":  df.get("weather_risk",   pd.Series(np.nan, index=df.index)),
        "active_events": df.get("active_events",   pd.Series(np.nan, index=df.index)),
        "scored_at":     now,
    })

    save_predictions(results)

    alerts = results[results["will_alert"] == 1]
    logger.info(f"Scored {len(results)} rows — {len(alerts)} alerts fired")
    return results


if __name__ == "__main__":
    run_prediction()