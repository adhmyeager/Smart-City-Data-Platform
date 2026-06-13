# ml/src/train.py
import pandas as pd
import numpy as np
import xgboost as xgb
import mlflow
import mlflow.xgboost
import logging
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    classification_report, roc_auc_score,
    precision_score, recall_score, f1_score,
)
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
# import shap  # Optional: for SHAP explanations

from config import (
    MLFLOW_TRACKING_URI, EXPERIMENT_NAME,
    MODEL_PATH, ALERT_THRESHOLD,
)
from extract import load_silver_data
from features import engineer_features, get_feature_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def train():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("alert_incident_prediction_silver")

    # ── 1. Load and engineer features ────────────────────────────────────────
    logger.info("Loading Silver data...")
    data = load_silver_data()

    logger.info("Engineering features...")
    df = engineer_features(data)

    feature_cols = get_feature_columns(df)
    X = df[feature_cols].copy()
    y = df["will_have_incident"].copy()

    logger.info(f"Training on {len(X)} rows, {len(feature_cols)} features")
    logger.info(f"Class balance — incidents: {y.mean():.2%}")

    # ── Handle edge case: if all positive or all negative, add synthetic samples ──
    if y.sum() == len(y) or y.sum() == 0:
        logger.warning("All samples are same class, adding synthetic negative examples...")
        # Create synthetic negative samples (flip incident_count to 0)
        X_synthetic = X.copy()
        X_synthetic[['incident_count', 'total_vehicles', 'avg_severity']] = 0
        X_synthetic['active_events'] = 0
        y_synthetic = pd.Series([1 - y.iloc[0]] * len(X_synthetic), index=X_synthetic.index)
        
        X = pd.concat([X, X_synthetic], ignore_index=True)
        y = pd.concat([y, y_synthetic], ignore_index=True)
        logger.info(f"After augmentation: {len(X)} rows, balance: {y.mean():.2%}")

    # ── 2. Temporal train/test split (no shuffling — future leaks past) ──────
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # handle class imbalance
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    logger.info(f"scale_pos_weight: {scale_pos_weight:.2f}")

    # ── 3. XGBoost params ────────────────────────────────────────────────────
    params = {
        "n_estimators":      300,
        "max_depth":         6,
        "learning_rate":     0.05,
        "subsample":         0.8,
        "colsample_bytree":  0.8,
        "scale_pos_weight":  scale_pos_weight,
        "random_state":      42,
        "eval_metric":       "auc",
        "early_stopping_rounds": 30,
        "use_label_encoder": False,
    }

    # ── 4. Train with MLflow tracking ────────────────────────────────────────
    with mlflow.start_run(run_name="xgb_alert_prediction"):
        mlflow.log_params(params)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size",  len(X_test))
        mlflow.log_param("class_balance", float(y.mean()))

        model = xgb.XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=50,
        )

        # ── 5. Evaluate ──────────────────────────────────────────────────────
        y_proba = model.predict_proba(X_test)[:, 1]
        y_pred  = (y_proba >= ALERT_THRESHOLD).astype(int)

        # Handle case where test set has only one class
        try:
            auc = roc_auc_score(y_test, y_proba)
        except ValueError:
            auc = 0.5  # neutral score if only one class
        
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall    = recall_score(y_test, y_pred, zero_division=0)
        f1        = f1_score(y_test, y_pred, zero_division=0)

        mlflow.log_metric("auc",       auc)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall",    recall)
        mlflow.log_metric("f1",        f1)

        logger.info(f"AUC={auc:.3f}  P={precision:.3f}  R={recall:.3f}  F1={f1:.3f}")
        print(classification_report(y_test, y_pred))

        # ── 6. Feature importance plot ───────────────────────────────────────
        fi = pd.Series(model.feature_importances_, index=feature_cols)
        top20 = fi.nlargest(20)
        fig, ax = plt.subplots(figsize=(8, 6))
        top20.sort_values().plot(kind="barh", ax=ax)
        ax.set_title("Top 20 feature importances")
        plt.tight_layout()
        mlflow.log_figure(fig, "feature_importance.png")
        plt.close()

        # ── 7. Save model ────────────────────────────────────────────────────
        model.save_model(MODEL_PATH)
        mlflow.xgboost.log_model(model, artifact_path="model")
        mlflow.log_param("feature_columns", ",".join(feature_cols))

        logger.info(f"Model saved to {MODEL_PATH}")
        logger.info(f"MLflow run complete")

    return model, feature_cols


if __name__ == "__main__":
    train()