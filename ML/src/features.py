import pandas as pd
import numpy as np
import logging
from typing import Dict

logger = logging.getLogger(__name__)


def find_col(df: pd.DataFrame, keywords: list, default: str = None) -> str:
    """Find a column in df that matches any of the keywords case-insensitively, excluding unix columns for timestamps."""
    is_time_search = any(kw.lower() in ["timestamp", "time"] for kw in keywords)
    for col in df.columns:
        col_lower = col.lower()
        if is_time_search and "unix" in col_lower:
            continue
        if any(kw.lower() in col_lower for kw in keywords):
            return col
    return default


def engineer_features(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge silver tables and produce a feature matrix for alert prediction.
    Target: will_have_incident (1/0) based on incident presence in next window.
    """
    # SILVER:
    telemetry   = data.get("telemetry",   pd.DataFrame())
    traffic     = data.get("traffic",     pd.DataFrame())
    weather     = data.get("weather",     pd.DataFrame())
    road_events = data.get("road_events",  pd.DataFrame())

    if traffic.empty:
        raise ValueError("traffic is empty — check your S3 Silver path")

    # ── base: traffic resampling into 30-minute windows ─────────────────────
    # Silver data is raw cleaned records, not pre-aggregated KPIs.
    # We first find column names using fuzzy matching.
    traffic_ts = find_col(traffic, ["timestamp", "time"], "timestamp")
    speed_col = find_col(traffic, ["current_speed", "speed"], "current_speed_kmh")
    cong_col = find_col(traffic, ["congestion_ratio", "congestion"], "congestion_ratio")
    density_col = find_col(traffic, ["density"], "traffic_density")
    closure_col = find_col(traffic, ["closure", "closed"], "road_closure")
    band_col = find_col(traffic, ["band"], "congestion_band")
    lat_col = find_col(traffic, ["lat_bucket", "latitude"], "gps_lat_bucket")
    lon_col = find_col(traffic, ["lon_bucket", "longitude"], "gps_lon_bucket")

    # Create local copy of traffic
    traffic_df = traffic.copy()
    traffic_df["dt"] = pd.to_datetime(traffic_df[traffic_ts], utc=True)
    traffic_df["window_start"] = traffic_df["dt"].dt.floor("30T")

    # Bucket coordinates to construct segment if not already bucketed
    if "bucket" not in lat_col.lower():
        traffic_df["gps_lat_bucket"] = traffic_df[lat_col].round(2)
    else:
        traffic_df["gps_lat_bucket"] = traffic_df[lat_col]
        
    if "bucket" not in lon_col.lower():
        traffic_df["gps_lon_bucket"] = traffic_df[lon_col].round(2)
    else:
        traffic_df["gps_lon_bucket"] = traffic_df[lon_col]

    traffic_df["segment"] = (traffic_df["gps_lat_bucket"].astype(str) + "_" + 
                             traffic_df["gps_lon_bucket"].astype(str))

    # Resample to 30-minute windows per segment
    df = traffic_df.groupby(["segment", "window_start"]).agg({
        speed_col: "mean",
        cong_col: "mean",
        density_col: "mean",
        closure_col: "max",  # 1 if there was any closure in the window
        band_col: lambda x: x.mode()[0] if not x.empty and not x.mode().empty else "free_flow",
        "gps_lat_bucket": "first",
        "gps_lon_bucket": "first",
    }).reset_index()

    # Rename columns to match old feature expectations
    df = df.rename(columns={
        speed_col: "avg_current_speed_kmh",
        cong_col: "avg_congestion_ratio",
        density_col: "avg_traffic_density",
        closure_col: "road_closure_count",
        band_col: "congestion_band",
        "window_start": "timestamp"
    })
    df["road_closure_count"] = df["road_closure_count"].astype(int)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # ── time features ────────────────────────────────────────────────────────
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["month"]       = df["timestamp"].dt.month
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_rush_hour"]= df["hour"].isin([6,7,8,9,16,17,18,19]).astype(int)

    # cyclical encoding so hour 23 and hour 0 are adjacent
    df["hour_sin"]    = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]    = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]     = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]     = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # ── traffic features from traffic_30min ──────────────────────────────────
    df["speed"] = df["avg_current_speed_kmh"]
    df["congestion_ratio"] = df["avg_congestion_ratio"]
    df["traffic_density"] = df["avg_traffic_density"]
    df["road_closures"] = df["road_closure_count"]
    
    # Encode congestion_band
    congestion_band_map = {
        "free_flow": 0,
        "moderate": 1, 
        "heavy": 2,
        "standstill": 3
    }
    df["congestion_band_encoded"] = df["congestion_band"].map(congestion_band_map).fillna(0)

    # ── lag features for traffic metrics ─────────────────────────────────────
    for col, alias in [("speed", "speed"), 
                        ("congestion_ratio", "congestion"),
                        ("traffic_density", "density")]:
        if col not in df.columns:
            continue
        
        # Sort by segment and timestamp to ensure proper lag
        df = df.sort_values(["segment", "timestamp"])
        
        # Create lag features
        df[f"{alias}_lag1"] = df.groupby("segment")[col].shift(1)
        df[f"{alias}_lag2"] = df.groupby("segment")[col].shift(2) 
        df[f"{alias}_lag4"] = df.groupby("segment")[col].shift(4)
        
        # Rolling statistics
        df[f"{alias}_roll3"] = df.groupby("segment")[col].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        df[f"{alias}_roll6"] = df.groupby("segment")[col].transform(
            lambda x: x.rolling(6, min_periods=1).mean()
        )
        df[f"{alias}_std3"] = df.groupby("segment")[col].transform(
            lambda x: x.rolling(3, min_periods=1).std().fillna(0)
        )
        
        # Rate of change
        df[f"{alias}_delta"] = df[col] - df[f"{alias}_lag1"]

    # ── road event / incident features from road_events ──────────────────────
    if not road_events.empty:
        events_df = road_events.copy()
        events_ts = find_col(events_df, ["timestamp", "time"], "timestamp")
        events_df["dt"] = pd.to_datetime(events_df[events_ts], utc=True)
        events_df["window_start"] = events_df["dt"].dt.floor("30T")

        # Map road events to segment using fuzzy matching coordinates
        lat_col_ev = find_col(events_df, ["lat_bucket", "latitude"], "latitude")
        lon_col_ev = find_col(events_df, ["lon_bucket", "longitude"], "longitude")

        if "bucket" not in lat_col_ev.lower():
            events_df["gps_lat_bucket"] = events_df[lat_col_ev].round(2)
        else:
            events_df["gps_lat_bucket"] = events_df[lat_col_ev]
            
        if "bucket" not in lon_col_ev.lower():
            events_df["gps_lon_bucket"] = events_df[lon_col_ev].round(2)
        else:
            events_df["gps_lon_bucket"] = events_df[lon_col_ev]

        events_df["segment"] = (events_df["gps_lat_bucket"].astype(str) + "_" + 
                                events_df["gps_lon_bucket"].astype(str))

        # Group by segment and 30-minute window
        severity_col = find_col(events_df, ["severity"], "severity_score")
        vehicle_col = find_col(events_df, ["vehicle"], "vehicle_id")

        events_grouped = events_df.groupby(["segment", "window_start"]).agg({
            events_ts: "count",
            severity_col: "mean",
            vehicle_col: "nunique"
        }).reset_index()

        events_grouped.columns = ["segment", "timestamp", "event_count", "avg_severity", "total_vehicles"]

        df = pd.merge(df, events_grouped, on=["segment", "timestamp"], how="left")
        
        df["event_count"] = df["event_count"].fillna(0).astype(int)
        df["incident_count"] = df["event_count"]  # Rebuild incident_count from road_events
        df["active_events"] = (df["event_count"] > 0).astype(int)
        df["total_vehicles"] = df["total_vehicles"].fillna(0).astype(int)
        df["avg_severity"] = df["avg_severity"].fillna(0)
    else:
        df["event_count"] = 0
        df["incident_count"] = 0
        df["active_events"] = 0
        df["total_vehicles"] = 0
        df["avg_severity"] = 0

    # ── weather features ────────────────────────────────────────────────────
    if not weather.empty:
        weather_df = weather.copy()
        weather_ts = find_col(weather_df, ["timestamp", "time"], "timestamp")
        weather_df["dt"] = pd.to_datetime(weather_df[weather_ts], utc=True)
        weather_df["window_start"] = weather_df["dt"].dt.floor("30T")
        
        temp_col = find_col(weather_df, ["temp"], "temp_c")
        humidity_col = find_col(weather_df, ["humidity"], "humidity_pct")
        wind_col = find_col(weather_df, ["wind"], "wind_kmh")
        vis_col = find_col(weather_df, ["visibility"], "visibility_km")
        sf_col = find_col(weather_df, ["speed_factor", "factor"], "speed_factor")
        
        # Resample weather globally to 30-minute windows
        weather_resampled = weather_df.groupby("window_start").agg({
            temp_col: "mean",
            humidity_col: "mean",
            wind_col: "mean",
            vis_col: "mean",
            sf_col: "mean"
        }).reset_index()
        
        weather_resampled = weather_resampled.rename(columns={
            temp_col: "avg_temp_c",
            humidity_col: "avg_humidity_pct",
            wind_col: "avg_wind_kmh",
            vis_col: "avg_visibility_km",
            sf_col: "avg_speed_factor",
            "window_start": "timestamp"
        })
        
        df = pd.merge_asof(
            df.sort_values("timestamp"),
            weather_resampled.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("2h")
        )
        
        df["avg_visibility_km"] = df["avg_visibility_km"].fillna(10)
        df["avg_wind_kmh"] = df["avg_wind_kmh"].fillna(0)
        df["avg_speed_factor"] = df["avg_speed_factor"].fillna(1.0)
        
        df["weather_risk"] = (
            (10 - df["avg_visibility_km"]) / 10 * 0.3 +
            (df["avg_wind_kmh"] / 50).clip(0, 1) * 0.3 +
            (1 - df["avg_speed_factor"]) * 0.4
        ).clip(0, 1)
    else:
        df["avg_temp_c"] = 25
        df["avg_humidity_pct"] = 50
        df["avg_wind_kmh"] = 10
        df["avg_visibility_km"] = 10
        df["avg_speed_factor"] = 1.0
        df["weather_risk"] = 0

    # Weather x traffic interaction
    df["weather_traffic_interaction"] = df["weather_risk"] * df["congestion_ratio"]

    # ── telemetry / congestion features from telemetry ──────────────────────
    if not telemetry.empty:
        tel_df = telemetry.copy()
        tel_ts = find_col(tel_df, ["timestamp", "time"], "timestamp")
        tel_df["dt"] = pd.to_datetime(tel_df[tel_ts], utc=True)
        tel_df["window_start"] = tel_df["dt"].dt.floor("30T")
        
        lat_col_tel = find_col(tel_df, ["lat_bucket", "latitude"], "latitude")
        lon_col_tel = find_col(tel_df, ["lon_bucket", "longitude"], "longitude")

        if "bucket" not in lat_col_tel.lower():
            tel_df["gps_lat_bucket"] = tel_df[lat_col_tel].round(2)
        else:
            tel_df["gps_lat_bucket"] = tel_df[lat_col_tel]
            
        if "bucket" not in lon_col_tel.lower():
            tel_df["gps_lon_bucket"] = tel_df[lon_col_tel].round(2)
        else:
            tel_df["gps_lon_bucket"] = tel_df[lon_col_tel]

        tel_df["segment"] = (tel_df["gps_lat_bucket"].astype(str) + "_" + 
                             tel_df["gps_lon_bucket"].astype(str))
        
        anomaly_col = find_col(tel_df, ["anomaly"], "is_anomaly")
        speed_col_tel = find_col(tel_df, ["speed"], "speed_kmh")
        
        telemetry_grouped = tel_df.groupby(["segment", "window_start"]).agg({
            anomaly_col: "sum",
            speed_col_tel: "mean"
        }).reset_index()
        telemetry_grouped.columns = ["segment", "timestamp", "anomalies", "cong_speed_mean"]
        
        df = pd.merge(df, telemetry_grouped, on=["segment", "timestamp"], how="left")
        
        df["anomalies"] = df["anomalies"].fillna(0).astype(int)
        df["cong_speed_mean"] = df["cong_speed_mean"].fillna(df["avg_current_speed_kmh"])
        df["cong_ratio_mean"] = df["avg_congestion_ratio"]
    else:
        df["anomalies"] = 0
        df["cong_speed_mean"] = df["avg_current_speed_kmh"]
        df["cong_ratio_mean"] = df["avg_congestion_ratio"]

    # ── target variable: will_have_incident ──────────────────────────────────
    # Rebuild from the Silver road_events folder: flag any 30-minute window
    # that contains at least one road event record (incident_count > 0).
    df["will_have_incident"] = (df["incident_count"] > 0).astype(int)

    # ── fill NaNs ─────────────────────────────────────────────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median(numeric_only=True))

    logger.info(f"Feature matrix: {df.shape}, incidents: {df['will_have_incident'].sum()}")
    return df


def get_feature_columns(df: pd.DataFrame) -> list:
    """Return only numeric columns suitable for XGBoost, excluding target/meta."""
    exclude = {"timestamp", "will_have_incident", "segment", 
               "gps_lat_bucket", "gps_lon_bucket", "congestion_band",
               "window_start", "window_end"}
    return [
        c for c in df.columns 
        if df[c].dtype in [np.float64, np.float32, np.int64, np.int32]
        and c not in exclude
    ]
