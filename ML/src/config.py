# ml/src/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from parent directory (ml/ folder)
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# S3
S3_BUCKET = os.getenv("S3_BUCKET", "smart-city-datalake")
S3_SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "silver/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Silver folders
ROAD_EVENTS_FOLDER = "road_events"
TELEMETRY_FOLDER = "telemetry"
TRAFFIC_FOLDER = "traffic"
WEATHER_FOLDER = "weather"

# MLflow
mlruns_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/mlruns"
MLFLOW_TRACKING_URI = f"file:///{mlruns_dir}"
EXPERIMENT_NAME = "alert_incident_prediction"

# Model
MODEL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models"
DATA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/data"
MODEL_PATH = os.path.join(MODEL_DIR, "xgb_alert_model.json")
PREDICTIONS_DB = os.path.join(DATA_DIR, "predictions.db")

# Feature windows (days of history to train on)
TRAINING_DAYS = 30

# Alert threshold — probability above this = predicted incident
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "0.6"))