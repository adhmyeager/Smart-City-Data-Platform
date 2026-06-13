# ml/src/dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import time
import os
import re
import json
from plotly.subplots import make_subplots
from config import PREDICTIONS_DB, ALERT_THRESHOLD

# ── 1. Page Configuration (Called ONLY once) ───────────────────────────────
st.set_page_config(
    page_title="Smart City — Incident Command",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 2. Dark Glassmorphism CSS Styles ─────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');

    /* Global styling overrides */
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif !important;
        font-size: 1.15rem;
        line-height: 1.6;
        color: #F8FAFC;
    }
    
    /* Overall Background */
    .stApp {
        background-color: #0B0F19 !important;
    }
    
    /* Main container background */
    [data-testid="stHeader"] {
        background-color: rgba(11, 15, 25, 0.8) !important;
        backdrop-filter: blur(8px);
    }
    
    /* Solid White Title */
    .main-title {
        font-size: 3rem;
        font-weight: 800;
        color: #F8FAFC;
        margin: 0;
        display: inline-block;
    }
    
    /* Section Headers */
    .section-header {
        font-size: 1.9rem;
        font-weight: 700;
        color: #F8FAFC;
        margin-top: 35px;
        margin-bottom: 20px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        padding-bottom: 8px;
    }
    
    /* Pulsing LIVE indicator */
    @keyframes pulse {
        0% {
            transform: scale(0.9);
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        }
        70% {
            transform: scale(1.1);
            box-shadow: 0 0 0 8px rgba(16, 185, 129, 0);
        }
        100% {
            transform: scale(0.9);
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
        }
    }
    .pulsing-dot {
        width: 10px;
        height: 10px;
        background-color: #10B981;
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
        vertical-align: middle;
        animation: pulse 2s infinite;
    }
    
    /* Glassmorphic card styling */
    .glass-card {
        background: rgba(30, 41, 59, 0.65) !important;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.07) !important;
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.4);
        transition: all 0.3s ease;
        margin-bottom: 20px;
    }
    .glass-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.5);
        border-color: rgba(255, 255, 255, 0.15);
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #0F172A !important;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    /* KPI Card structure */
    .kpi-card-custom {
        background: rgba(30, 41, 59, 0.65);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .kpi-card-custom:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.4);
        border-color: rgba(255, 255, 255, 0.15);
    }
    .kpi-title {
        font-size: 0.8rem;
        color: #94A3B8;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #F8FAFC;
        margin: 4px 0;
    }
    .kpi-delta {
        font-size: 0.8rem;
        font-weight: 500;
    }
    .delta-positive-green {
        color: #10B981;
    }
    .delta-negative-red {
        color: #EF4444;
    }
    .delta-neutral-gray {
        color: #64748B;
    }
    
    /* Badges */
    .risk-badge {
        padding: 6px 12px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-block;
        text-align: center;
    }
    .risk-badge-low {
        background-color: rgba(16, 185, 129, 0.2);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.4);
    }
    .risk-badge-moderate {
        background-color: rgba(245, 158, 11, 0.2);
        color: #FBBF24;
        border: 1px solid rgba(245, 158, 11, 0.4);
    }
    .risk-badge-high {
        background-color: rgba(249, 115, 22, 0.2);
        color: #FB923C;
        border: 1px solid rgba(249, 115, 22, 0.4);
    }
    .risk-badge-critical {
        background-color: rgba(239, 68, 68, 0.2);
        color: #F87171;
        border: 1px solid rgba(239, 68, 68, 0.4);
    }
    
    /* Red alert bar for ingestion delay */
    .warning-banner {
        background-color: rgba(239, 68, 68, 0.15);
        border: 1px solid rgba(239, 68, 68, 0.3);
        border-radius: 8px;
        padding: 12px;
        color: #FCA5A5;
        font-weight: 600;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)


# ── 3. Helper Functions & Safe Schema Enforcement ───────────────────────────
def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforces that all Silver schema columns are present in the DataFrame."""
    mapping = {
        "segment": "segment_id",
        "congestion": "congestion_level",
    }
    for old_col, new_col in mapping.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]
            
    required_cols = [
        "id", "timestamp", "vehicle_id", "segment_id", "speed", "heading", "acceleration",
        "congestion_level", "vehicle_count", "avg_speed", "temperature", "rain", "wind_speed",
        "visibility", "weather_risk", "active_events", "event_type", "severity",
        "alert_prob", "will_alert", "scored_at"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan
            
    # Apply standard type casting
    df["will_alert"] = df["will_alert"].fillna(0).astype(int)
    df["alert_prob"] = df["alert_prob"].fillna(0.0).astype(float)
    df["active_events"] = df["active_events"].fillna(0.0).astype(float)
    return df


def find_col(df: pd.DataFrame, keywords: list, default: str = None) -> str:
    """Finds a column in df that matches any of the keywords case-insensitively, excluding unix columns."""
    if df.empty:
        return default
    is_time_search = any(kw.lower() in ["timestamp", "time"] for kw in keywords)
    for col in df.columns:
        col_lower = col.lower()
        if is_time_search and "unix" in col_lower:
            continue
        if any(kw.lower() in col_lower for kw in keywords):
            return col
    return default


def predict_next_2_hours(df: pd.DataFrame, threshold: float = 0.5) -> tuple:
    """
    Analyzes current predictions to forecast risk for next 2 hours.
    Returns: (risk_label, risk_class, projected_probability, expected_alerts, primary_driver, vulnerable_segments)
    Uses dynamic threshold to identify vulnerable segments.
    """
    if df.empty:
        return "LOW", "low", 0.0, 0, "No Data", pd.Series(dtype=float)
    
    # Calculate overall risk metrics
    alert_prob_col = next((c for c in df.columns if "alert_prob" in c.lower()), "alert_prob")
    segment_col = next((c for c in df.columns if "segment" in c.lower()), "segment_id")
    
    avg_alert_prob = df[alert_prob_col].mean() if alert_prob_col in df.columns else 0.0
    max_alert_prob = df[alert_prob_col].max() if alert_prob_col in df.columns else 0.0
    
    # Determine risk level and class
    if max_alert_prob >= 0.7:
        risk_lbl = "CRITICAL"
        risk_cls = "critical"
    elif max_alert_prob >= 0.5:
        risk_lbl = "HIGH"
        risk_cls = "high"
    elif max_alert_prob >= 0.3:
        risk_lbl = "MEDIUM"
        risk_cls = "medium"
    else:
        risk_lbl = "LOW"
        risk_cls = "low"
    
    # Projected probability for next 2 hours (use average)
    proj_prob = avg_alert_prob
    
    # Expected alerts (count of segments with alert_prob > threshold)
    will_alert_col = next((c for c in df.columns if "will_alert" in c.lower()), "will_alert")
    exp_alerts = int(df[will_alert_col].sum()) if will_alert_col in df.columns else 0
    
    # Determine primary incident driver
    primary_driver = "Multiple Factors"
    drivers = {
        "weather_risk": (df.get("weather_risk", pd.Series()).mean() or 0),
        "congestion_level": (df.get("congestion_level", pd.Series()).mean() or 0),
        "active_events": (df.get("active_events", pd.Series()).mean() or 0),
    }
    
    if drivers:
        primary_driver = max(drivers.keys(), key=lambda k: drivers[k])
        # Format driver name
        primary_driver = primary_driver.replace("_", " ").title()
    
    # Find vulnerable segments (alert_prob > threshold - now dynamic based on sidebar threshold)
    vulnerable = pd.Series(dtype=float)
    if segment_col in df.columns and alert_prob_col in df.columns:
        seg_risks = df.groupby(segment_col)[alert_prob_col].mean()
        vulnerable = seg_risks[seg_risks > threshold].sort_values(ascending=False).head(5)
    
    return risk_lbl, risk_cls, proj_prob, exp_alerts, primary_driver, vulnerable


def clean_column_names(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Renames dataframe columns using keyword matches to normalize schema."""
    if df.empty:
        return df
    new_cols = {}
    for standard_name, keywords in mappings.items():
        found = find_col(df, keywords)
        if found:
            new_cols[found] = standard_name
    return df.rename(columns=new_cols)


def normalize_dataframe(df: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
    """Ensures all expected columns exist in the DataFrame, coercing missing ones to nan."""
    if df.empty:
        return pd.DataFrame(columns=expected_cols)
    # Add missing columns with NaN
    for col in expected_cols:
        if col not in df.columns:
            df[col] = np.nan
    # Select only expected columns (maintains order and ensures all exist)
    return df[[col for col in expected_cols if col in df.columns]]


def check_cols(df: pd.DataFrame, cols: list) -> bool:
    """Returns True if all requested columns exist in the DataFrame and contain at least some non-null values."""
    for col in cols:
        if col not in df.columns or df[col].isna().all():
            return False
    return True


def apply_plotly_dark_theme(fig):
    """Applies a consistent premium dark design to all Plotly visualizations."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#F8FAFC", family="Outfit, sans-serif"),
        title_font=dict(size=14, color="#E2E8F0"),
        xaxis=dict(
            gridcolor="#1E293B",
            linecolor="#334155",
            showgrid=True,
            zeroline=False,
            tickfont=dict(color="#94A3B8")
        ),
        yaxis=dict(
            gridcolor="#1E293B",
            linecolor="#334155",
            showgrid=True,
            zeroline=False,
            tickfont=dict(color="#94A3B8")
        ),
        legend=dict(
            bgcolor="rgba(15, 23, 42, 0.7)",
            bordercolor="rgba(255,255,255,0.07)",
            borderwidth=1,
            font=dict(color="#CBD5E1")
        ),
        margin=dict(l=40, r=40, t=50, b=40)
    )
    return fig


def parse_coords(seg_id):
    """Regex coordinate extractor centered around Cairo, Egypt."""
    if not isinstance(seg_id, str):
        return None, None
    matches = re.findall(r"[-+]?\d*\.\d+|\d+", seg_id)
    if len(matches) >= 2:
        try:
            val1 = float(matches[0])
            val2 = float(matches[1])
            if 28.0 <= val1 <= 32.0 and 29.0 <= val2 <= 34.0:
                return val1, val2
            if 28.0 <= val2 <= 32.0 and 29.0 <= val1 <= 34.0:
                return val2, val1
        except ValueError:
            pass
    return None, None


# ── PART 3 — Recommendation Engine Core Functions ──────────────────────────
def generate_recommendations(df: pd.DataFrame) -> list:
    """
    Analyzes predictions to generate actionable recommendations across 4 categories.
    Returns list of recommendation dicts, sorted by priority and confidence.
    """
    if df.empty:
        return [{"priority": "LOW", "category": "System Status", "action": "All systems nominal — no immediate action required", 
                 "segment_id": "N/A", "confidence": 0.0, "reasoning": "No prediction data available.", 
                 "triggered_by": [], "estimated_impact": "N/A", "time_window": "N/A"}]
    
    # Use only most recent scoring run
    df = df[df["scored_at"] == df["scored_at"].max()].copy()
    if df.empty:
        return [{"priority": "LOW", "category": "System Status", "action": "All systems nominal — no immediate action required", 
                 "segment_id": "N/A", "confidence": 0.0, "reasoning": "No recent predictions.", 
                 "triggered_by": [], "estimated_impact": "N/A", "time_window": "N/A"}]
    
    recommendations = []
    
    # Ensure segment_id is string
    if "segment_id" in df.columns:
        df["segment_id"] = df["segment_id"].astype(str)
    
    # ── Route Diversion ──────────────────────────────────────────────────────
    for idx, row in df.iterrows():
        congestion = float(row.get("congestion_level", 0) or 0)
        alert_prob = float(row.get("alert_prob", 0) or 0)
        will_alert = int(row.get("will_alert", 0) or 0)
        speed = float(row.get("speed", 0) or 0)
        active_events = float(row.get("active_events", 0) or 0)
        segment = str(row.get("segment_id", "unknown"))
        
        # Rule 1: Dynamic route diversion for severe congestion
        if congestion >= 0.4:
            recommendations.append({
                "priority": "HIGH" if congestion >= 0.6 else "MODERATE",
                "category": "Route Diversion",
                "action": f"Activate dynamic route diversion for {segment} — elevated congestion",
                "segment_id": segment,
                "confidence": float(congestion),
                "reasoning": f"Elevated congestion level ({congestion:.1%}) detected on {segment}. Recommend dynamic route diversion to relieve pressure.",
                "triggered_by": [f"congestion_level={congestion:.3f}"],
                "estimated_impact": "Reduces travel time by ~15%",
                "time_window": "Next 2 hours"
            })
        
        # Rule 2: Route advisory for active events
        elif active_events >= 1:
            recommendations.append({
                "priority": "HIGH",
                "category": "Route Diversion",
                "action": f"Issue route advisory for {segment} — active road events",
                "segment_id": segment,
                "confidence": 0.8,
                "reasoning": f"Active road event detected on {segment}. Drivers should be advised of alternative routes.",
                "triggered_by": [f"active_events={int(active_events)}"],
                "estimated_impact": "Reduces incident-related delays by ~30%",
                "time_window": "Next 2 hours"
            })
        
        # Rule 3: Near-standstill or critical alert condition
        elif (alert_prob >= 0.5 or will_alert == 1) or (speed > 0 and speed < 15):
            recommendations.append({
                "priority": "CRITICAL",
                "category": "Route Diversion",
                "action": f"Issue emergency route diversion for {segment} — high risk / near standstill",
                "segment_id": segment,
                "confidence": float(alert_prob or 0.8),
                "reasoning": f"Critical alert probability ({alert_prob:.1%}) or very low speed ({speed:.1f} km/h) detected. Immediate diversions necessary.",
                "triggered_by": [f"alert_prob={alert_prob:.3f}", f"speed={speed:.1f} km/h"],
                "estimated_impact": "Prevents secondary collisions; reduces incident cascade",
                "time_window": "Next 30 minutes"
            })
    
    # ── Maintenance Scheduling ───────────────────────────────────────────────
    for idx, row in df.iterrows():
        active_events = float(row.get("active_events", 0) or 0)
        alert_prob = float(row.get("alert_prob", 0) or 0)
        congestion = float(row.get("congestion_level", 0) or 0)
        segment = str(row.get("segment_id", "unknown"))
        
        # Rule 1: Maintenance inspection for active events
        if active_events >= 1:
            recommendations.append({
                "priority": "MODERATE",
                "category": "Maintenance",
                "action": f"Schedule maintenance crew inspection for {segment} — active event detected",
                "segment_id": segment,
                "confidence": 0.7,
                "reasoning": f"Active event ongoing on segment {segment}. Inspection needed to ensure safety and clear path.",
                "triggered_by": [f"active_events={int(active_events)}"],
                "estimated_impact": "Prevents maintenance-related incidents by ~40%",
                "time_window": "Next 2 hours"
            })
        
        # Rule 2: Road clearance for high-risk or high congestion
        elif alert_prob >= 0.4 or congestion >= 0.7:
            recommendations.append({
                "priority": "HIGH",
                "category": "Maintenance",
                "action": f"Dispatch road clearance team to {segment} — elevated safety risk",
                "segment_id": segment,
                "confidence": float(max(alert_prob, congestion)),
                "reasoning": f"Elevated safety risk (alert prob {alert_prob:.1%}, congestion {congestion:.1%}) requires preemptive dispatch to ensure road clearance.",
                "triggered_by": [f"alert_prob={alert_prob:.3f}", f"congestion_level={congestion:.3f}"],
                "estimated_impact": "Reduces clearance time by ~50%; prevents secondary incidents",
                "time_window": "Next 30 minutes"
            })
    
    # ── Weather Alerts ───────────────────────────────────────────────────────
    for idx, row in df.iterrows():
        weather_risk = float(row.get("weather_risk", 0) or 0)
        alert_prob = float(row.get("alert_prob", 0) or 0)
        segment = str(row.get("segment_id", "unknown"))
        
        # Rule 1: High composite weather risk
        if weather_risk >= 0.4:
            recommendations.append({
                "priority": "HIGH",
                "category": "Weather Alert",
                "action": f"Pre-position emergency response near {segment} — high composite weather risk",
                "segment_id": segment,
                "confidence": float(weather_risk),
                "reasoning": f"Composite weather risk ({weather_risk:.1%}) is high. Emergency services should be positioned nearby.",
                "triggered_by": [f"weather_risk={weather_risk:.3f}"],
                "estimated_impact": "Reduces weather incident response time by ~40%",
                "time_window": "Next 2 hours"
            })
        
        # Rule 2: Moderate weather risk + elevated alert probability
        elif weather_risk >= 0.15 or alert_prob >= 0.35:
            recommendations.append({
                "priority": "MODERATE",
                "category": "Weather Alert",
                "action": f"Issue weather advisory warning for {segment} — reduce speed advisory",
                "segment_id": segment,
                "confidence": float(max(weather_risk, alert_prob)),
                "reasoning": f"Weather risk ({weather_risk:.1%}) combined with elevated alert probability ({alert_prob:.1%}) suggests advisory warning.",
                "triggered_by": [f"weather_risk={weather_risk:.3f}", f"alert_prob={alert_prob:.3f}"],
                "estimated_impact": "Reduces weather-related accidents by ~45%",
                "time_window": "Next 2 hours"
            })
    
    # Fallback: Generate recommendations for any will_alert=1 that didn't match specific rules
    alert_segments = set(df[df["will_alert"] == 1]["segment_id"].astype(str))
    rec_segments = set(r["segment_id"] for r in recommendations)
    
    for segment in alert_segments - rec_segments:
        seg_data = df[df["segment_id"].astype(str) == segment].iloc[0]
        alert_prob = float(seg_data.get("alert_prob", 0) or 0)
        
        recommendations.append({
            "priority": "MODERATE" if alert_prob >= 0.4 else "LOW",
            "category": "Resource Deployment",
            "action": f"Increase monitoring for {segment} — elevated incident risk detected",
            "segment_id": segment,
            "confidence": alert_prob,
            "reasoning": f"Model prediction indicates elevated incident risk ({alert_prob:.1%}) for this segment. Recommend increased monitoring and readiness.",
            "triggered_by": [f"alert_prob={alert_prob:.3f}", f"will_alert=1"],
            "estimated_impact": "Early detection enables faster response",
            "time_window": "Next 30 minutes"
        })
    
    # Deduplication: same segment + category → keep highest priority/confidence
    seen = {}
    for rec in recommendations:
        key = (rec["segment_id"], rec["category"])
        if key not in seen or (PRIORITY_ORDER.get(rec["priority"], 5) < PRIORITY_ORDER.get(seen[key]["priority"], 5)):
            seen[key] = rec
    
    recommendations = list(seen.values())
    
    # Sort: CRITICAL first, then by confidence descending
    recommendations.sort(key=lambda x: (PRIORITY_ORDER.get(x["priority"], 5), -x["confidence"]))
    
    # Cap at 20 recommendations
    if not recommendations:
        return [{"priority": "LOW", "category": "System Status", "action": "All systems nominal — no immediate action required", 
                 "segment_id": "N/A", "confidence": 0.0, "reasoning": "No critical conditions detected in current data.", 
                 "triggered_by": [], "estimated_impact": "N/A", "time_window": "N/A"}]
    
    return recommendations


PRIORITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3}


def save_recommendations_to_db(recommendations: list, db_path: str):
    """Saves recommendations to SQLite history table with timestamp."""
    try:
        con = sqlite3.connect(db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS recommendation_history (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                generated_at     TEXT,
                priority         TEXT,
                category         TEXT,
                action           TEXT,
                segment_id       TEXT,
                confidence       REAL,
                reasoning        TEXT,
                triggered_by     TEXT,
                estimated_impact TEXT,
                time_window      TEXT
            )
        """)
        
        now = datetime.utcnow().isoformat()
        for rec in recommendations:
            if rec.get("priority") != "LOW" or rec.get("category") != "System Status":  # Skip "nominal" entries
                con.execute("""
                    INSERT INTO recommendation_history 
                    (generated_at, priority, category, action, segment_id, confidence, reasoning, triggered_by, estimated_impact, time_window)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    now,
                    rec.get("priority", ""),
                    rec.get("category", ""),
                    rec.get("action", ""),
                    rec.get("segment_id", ""),
                    rec.get("confidence", 0.0),
                    rec.get("reasoning", ""),
                    json.dumps(rec.get("triggered_by", [])),
                    rec.get("estimated_impact", ""),
                    rec.get("time_window", "")
                ))
        
        con.commit()
        con.close()
    except Exception as e:
        st.warning(f"Failed to save recommendations to history: {str(e)[:50]}")


def build_recommendation_explainability(recommendations: list, df: pd.DataFrame) -> None:
    """Renders explainability charts for top 3 CRITICAL/HIGH recommendations."""
    critical_high = [r for r in recommendations if r["priority"] in ["CRITICAL", "HIGH"]][:3]
    
    if not critical_high:
        return
    
    st.markdown("#### 🔍 Explainability — Feature Thresholds")
    
    for rec in critical_high:
        segment = rec["segment_id"]
        priority = rec["priority"]
        category = rec["category"]
        
        # Get row for this segment
        seg_data = df[df["segment_id"].astype(str) == segment]
        if seg_data.empty:
            continue
        
        row = seg_data.iloc[0]
        
        # Define thresholds and feature names for each category
        features_to_show = []
        
        if category == "Resource Deployment":
            features_to_show = [
                ("alert_prob", row.get("alert_prob", 0), 0.75 if priority == "CRITICAL" else 0.6),
                ("telemetry.speed", row.get("speed", 0), 90),
                ("telemetry.acceleration", row.get("acceleration", 0), 3),
            ]
        elif category == "Route Diversion":
            features_to_show = [
                ("traffic.congestion_level", row.get("congestion_level", 0), 0.8),
                ("traffic.avg_speed", row.get("avg_speed", 0), 5),
            ]
        elif category == "Maintenance":
            features_to_show = [
                ("road_events.severity", row.get("severity", 0), 0.7),
                ("road_events.active_events", row.get("active_events", 0), 1),
            ]
        elif category == "Weather Alert":
            features_to_show = [
                ("weather.rain", row.get("rain", 0), 5),
                ("weather.visibility", row.get("visibility", 1000), 500),
                ("weather.weather_risk", row.get("weather_risk", 0), 0.7),
            ]
        
        if features_to_show:
            with st.expander(f"Why {segment} is {priority} ({category})", expanded=False):
                fig = go.Figure()
                
                feature_names = [f[0] for f in features_to_show]
                actual_vals = [f[1] for f in features_to_show]
                threshold_vals = [f[2] for f in features_to_show]
                
                # Normalize for display
                colors = []
                for actual, thresh in zip(actual_vals, threshold_vals):
                    if actual > thresh * 1.2:
                        colors.append("#EF4444")  # Red
                    elif actual > thresh:
                        colors.append("#F97316")  # Orange
                    else:
                        colors.append("#10B981")  # Green
                
                fig.add_trace(go.Bar(
                    y=feature_names,
                    x=actual_vals,
                    orientation='h',
                    marker=dict(color=colors),
                    text=[f"{v:.2f}" for v in actual_vals],
                    textposition='outside',
                ))
                
                # Add threshold line
                fig.add_vline(x=threshold_vals[0], line_dash="dash", line_color="gray", annotation_text="Threshold")
                
                fig.update_layout(
                    title=f"Feature Values vs. Thresholds — {segment}",
                    xaxis_title="Value",
                    yaxis_title="Feature",
                    showlegend=False,
                    height=250,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#F8FAFC"),
                    margin=dict(l=150, r=50, t=50, b=30)
                )
                
                st.plotly_chart(fig, use_container_width=True)


# ── 4. Real-time DB Ingestion Health & Waiting Screen ───────────────────────
@st.cache_data(ttl=30)
def load_predictions(hours_back: int = 24) -> pd.DataFrame:
    """Reads predictions from SQLite with cache timeout."""
    if not os.path.exists(PREDICTIONS_DB):
        return pd.DataFrame()
    try:
        con = sqlite3.connect(PREDICTIONS_DB)
        cutoff = (datetime.utcnow() - timedelta(hours=hours_back)).isoformat()
        df = pd.read_sql(
            "SELECT * FROM predictions WHERE scored_at >= ? ORDER BY timestamp DESC",
            con, params=[cutoff],
        )
        con.close()
        if df.empty:
            return pd.DataFrame()
        df = ensure_schema(df)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_silver_layer_data_fast(hours_back: int = 24) -> dict:
    """Directly loads raw tables from the S3 Silver layer, optimized with single pass and fallback."""
    import boto3
    import io
    from config import S3_BUCKET, S3_SILVER_PREFIX, AWS_REGION, ROAD_EVENTS_FOLDER, TELEMETRY_FOLDER, TRAFFIC_FOLDER, WEATHER_FOLDER
    
    # Use shorter timeout for faster failure and retry
    s3 = boto3.client("s3", region_name=AWS_REGION, config=boto3.session.Config(connect_timeout=5, retries={'max_attempts': 2}))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    
    data = {}
    folders = {
        "telemetry": TELEMETRY_FOLDER,
        "traffic": TRAFFIC_FOLDER,
        "weather": WEATHER_FOLDER,
        "road_events": ROAD_EVENTS_FOLDER
    }
    
    for label, folder in folders.items():
        prefix = f"{S3_SILVER_PREFIX}{folder}/"
        try:
            with st.spinner(f"Loading {label}..."):
                paginator = s3.get_paginator("list_objects_v2")
                all_objs = []
                recent_objs = []
                
                # Single pass: collect all objects and filter recent ones
                try:
                    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, PaginationConfig={'PageSize': 100}):
                        for obj in page.get("Contents", []):
                            if obj["Key"].endswith(".parquet"):
                                all_objs.append(obj)
                                if obj["LastModified"] >= cutoff:
                                    recent_objs.append(obj)
                except Exception as list_err:
                    st.warning(f"Timeout listing {label}: {str(list_err)[:50]}")
                
                # Use recent files, or fallback to latest single file
                keys_to_read = []
                if recent_objs:
                    keys_to_read = [obj["Key"] for obj in recent_objs[:10]]  # Limit to 10 most recent
                elif all_objs:
                    latest_obj = max(all_objs, key=lambda x: x["LastModified"])
                    keys_to_read = [latest_obj["Key"]]
                
                frames = []
                for key in keys_to_read:
                    try:
                        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                        if not df.empty:
                            frames.append(df)
                    except Exception as read_err:
                        st.warning(f"Failed to read {key}: {str(read_err)[:50]}")
                        pass
                
                if frames:
                    data[label] = pd.concat(frames, ignore_index=True)
                else:
                    data[label] = pd.DataFrame()
        except Exception as e:
            st.warning(f"Error loading {label} from S3: {str(e)[:100]}")
            data[label] = pd.DataFrame()
            
    return data


# Mappings for cleaning Silver layer schemas
telemetry_mappings = {
    "speed": ["speed"],
    "acceleration": ["acceleration", "accel"],
    "heading": ["heading"],
    "vehicle_id": ["vehicle_id", "vehicle"],
    "segment_id": ["segment_id", "segment", "location"],
    "timestamp": ["timestamp", "time"]
}
traffic_mappings = {
    "congestion_level": ["congestion_level", "congestion_ratio", "congestion"],
    "vehicle_count": ["vehicle_count", "density", "count"],
    "avg_speed": ["avg_speed", "current_speed", "speed"],
    "segment_id": ["segment_id", "segment", "location"],
    "timestamp": ["timestamp", "time"]
}
weather_mappings = {
    "temperature": ["temp_c", "temperature", "temp"],
    "rain": ["rain", "precipitation"],
    "wind_speed": ["wind_kmh", "wind_speed", "wind"],
    "visibility": ["visibility_km", "visibility"],
    "weather_risk": ["weather_severity", "weather_risk", "risk"],
    "timestamp": ["event_time", "timestamp", "time"]
}
road_events_mappings = {
    "event_type": ["event_type", "type"],
    "severity": ["severity"],
    "segment_id": ["segment_id", "segment", "location"],
    "timestamp": ["timestamp", "time"]
}


# Check if the database contains any records before continuing
db_exists = os.path.exists(PREDICTIONS_DB)
is_db_empty = True
if db_exists:
    try:
        con = sqlite3.connect(PREDICTIONS_DB)
        c = con.cursor()
        c.execute("SELECT count(*) FROM predictions")
        row_count = c.fetchone()[0]
        con.close()
        if row_count > 0:
            is_db_empty = False
    except Exception:
        pass

if not db_exists or is_db_empty:
    st.markdown("""
    <div style="text-align: center; margin-top: 15%;">
        <div style="font-size: 3.5rem; font-weight: 800; color: #F8FAFC; margin-bottom: 20px;">
            Cairo Smart City Operations Center
        </div>
        <p style="color: #94A3B8; font-size: 1.25rem; margin-bottom: 30px;">
            Pipeline starting — waiting for first scoring run...
        </p>
    </div>
    """, unsafe_allow_html=True)
    with st.spinner("Connecting to live scoring database..."):
        time.sleep(10)
        st.rerun()


# ── 5. Sidebar & Configuration ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <h3 style="color: #F8FAFC; font-weight: 700; margin-bottom: 20px;">
        Control Panel
    </h3>
    """, unsafe_allow_html=True)
    hours = st.slider("Timeframe (Hours)", 1, 72, 24, help="Historical data lookup window")
    threshold = st.slider("Model Threshold", 0.1, 1.0, ALERT_THRESHOLD, 0.05, help="Alert firing probability threshold")
    
    st.markdown("---")
    if st.button("Force Clear Cache", use_container_width=True):
        st.cache_data.clear()


# Load predictions database data
df_raw = load_predictions(hours)
if df_raw.empty:
    st.warning("No database predictions found within the selected timeframe.")
    st.stop()

# Force sidebar threshold overwrite
df_raw["will_alert"] = (df_raw["alert_prob"] >= threshold).astype(int)

# Fetch S3 Silver datasets using the FAST list and download method
silver_data = load_silver_layer_data_fast(hours)

telemetry_df = clean_column_names(silver_data.get("telemetry", pd.DataFrame()), telemetry_mappings)
traffic_df = clean_column_names(silver_data.get("traffic", pd.DataFrame()), traffic_mappings)
weather_df = clean_column_names(silver_data.get("weather", pd.DataFrame()), weather_mappings)
road_events_df = clean_column_names(silver_data.get("road_events", pd.DataFrame()), road_events_mappings)

# Normalize schemas to prevent KeyErrors and parse data types
telemetry_df = normalize_dataframe(telemetry_df, ["speed", "acceleration", "heading", "vehicle_id", "segment_id", "timestamp"])
traffic_df = normalize_dataframe(traffic_df, ["congestion_level", "avg_speed", "vehicle_count", "segment_id", "timestamp"])
weather_df = normalize_dataframe(weather_df, ["temperature", "rain", "wind_speed", "visibility", "weather_risk", "timestamp"])
road_events_df = normalize_dataframe(road_events_df, ["event_type", "severity", "segment_id", "timestamp"])

# Add fillna defaults for weather_df to ensure plots and calculations do not crash or empty out
# Weather data removed - focus on predictive analysis

# Convert all timestamps in silver dataframes to UTC datetime
for rdf in [telemetry_df, traffic_df, weather_df, road_events_df]:
    if not rdf.empty and "timestamp" in rdf.columns:
        rdf["timestamp"] = pd.to_datetime(rdf["timestamp"], utc=True)

if not telemetry_df.empty and "speed" in telemetry_df.columns:
    telemetry_df["speed"] = pd.to_numeric(telemetry_df["speed"], errors="coerce")
    telemetry_df["acceleration"] = pd.to_numeric(telemetry_df["acceleration"], errors="coerce")
    telemetry_df["heading"] = pd.to_numeric(telemetry_df["heading"], errors="coerce")

if not traffic_df.empty and "congestion_level" in traffic_df.columns:
    traffic_df["congestion_level"] = pd.to_numeric(traffic_df["congestion_level"], errors="coerce")
    traffic_df["avg_speed"] = pd.to_numeric(traffic_df["avg_speed"], errors="coerce")
    traffic_df["vehicle_count"] = pd.to_numeric(traffic_df["vehicle_count"], errors="coerce")

if not weather_df.empty and "rain" in weather_df.columns:
    weather_df["rain"] = pd.to_numeric(weather_df["rain"], errors="coerce")
    weather_df["weather_risk"] = pd.to_numeric(weather_df["weather_risk"], errors="coerce")
    weather_df["temperature"] = pd.to_numeric(weather_df["temperature"], errors="coerce")
    weather_df["wind_speed"] = pd.to_numeric(weather_df["wind_speed"], errors="coerce")
    weather_df["visibility"] = pd.to_numeric(weather_df["visibility"], errors="coerce")

if not road_events_df.empty and "severity" in road_events_df.columns:
    road_events_df["severity"] = pd.to_numeric(road_events_df["severity"], errors="coerce")


# ── 6. Header Section (Gradient Title + LIVE Indicator + Timestamp) ──────────
current_time_utc_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
st.markdown(f"""
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; border-bottom: 1px solid rgba(255, 255, 255, 0.07); padding-bottom: 15px;">
    <div>
        <span class="pulsing-dot"></span>
        <span style="color: #10B981; font-weight: 700; font-size: 0.9rem; letter-spacing: 0.15em; vertical-align: middle; margin-right: 15px;">LIVE FEED</span>
        <h1 class="main-title">Cairo Operations command</h1>
    </div>
    <div style="text-align: right; color: #94A3B8; font-size: 1rem; font-weight: 600;">
        SYSTEM STATUS: <span style="color: #60A5FA; font-weight: 700; margin-right: 20px;">ONLINE</span>
        LAST UPDATED: <span style="color: #F8FAFC; font-weight: 800; background: rgba(255,255,255,0.05); padding: 4px 8px; border-radius: 6px;">{current_time_utc_str} UTC</span>
    </div>
</div>
""", unsafe_allow_html=True)


# Calculate sidebar ingestion latency
last_scored_dt = pd.to_datetime(df_raw["scored_at"].max())
if last_scored_dt.tzinfo is None:
    last_scored_dt = last_scored_dt.replace(tzinfo=timezone.utc)
now_utc = datetime.now(timezone.utc)
seconds_since = (now_utc - last_scored_dt).total_seconds()

with st.sidebar:
    st.markdown("### Ingestion Health")
    st.markdown(f"**Last Scored:** `{df_raw['scored_at'].max()}`")
    st.markdown(f"**Elapsed Latency:** `{int(seconds_since)}s` ago")


# ── 7. SECTION 3 — KPI Row (8 cards, 2 rows of 4) ───────────────────────────
kpi_container = st.empty()

# Calculate deltas against previous hour
max_ts = df_raw["timestamp"].max()
if pd.notna(max_ts):
    df_current_hour = df_raw[df_raw["timestamp"] >= max_ts - pd.Timedelta("1h")]
    df_prev_hour = df_raw[(df_raw["timestamp"] >= max_ts - pd.Timedelta("2h")) & (df_raw["timestamp"] < max_ts - pd.Timedelta("1h"))]
else:
    df_current_hour = df_raw
    df_prev_hour = pd.DataFrame()

# Helper to format delta values
def get_delta_html(curr, prev, is_pct=False, invert_color=False, format_str="{:.1f}"):
    if pd.isna(curr) or pd.isna(prev):
        return f'<span class="delta-neutral-gray">-- vs prev hour</span>'
    diff = curr - prev
    symbol = "+" if diff >= 0 else ""
    unit = "%" if is_pct else ""
    
    if diff == 0:
        d_class = "delta-neutral-gray"
    elif diff > 0:
        d_class = "delta-negative-red" if invert_color else "delta-positive-green"
    else:
        d_class = "delta-positive-green" if invert_color else "delta-negative-red"
        
    return f'<span class="kpi-delta {d_class}">{symbol}{format_str.format(diff)}{unit} vs prev hour</span>'

# Calculate actual values
tot_curr = len(df_raw)
tot_prev = len(df_raw[df_raw["timestamp"] < max_ts - pd.Timedelta("1h")]) if pd.notna(max_ts) else tot_curr
tot_delta = tot_curr - tot_prev
tot_delta_html = f'<span class="kpi-delta delta-positive-green">+{tot_delta:,} runs</span>' if tot_delta >= 0 else f'<span class="kpi-delta delta-negative-red">{tot_delta:,} runs</span>'

act_alerts_curr = df_raw["will_alert"].sum()
act_alerts_prev = df_prev_hour["will_alert"].sum() if not df_prev_hour.empty else 0
act_alerts_delta_html = get_delta_html(act_alerts_curr, act_alerts_prev, invert_color=True, format_str="{:+d}")

alert_rate_curr = (act_alerts_curr / tot_curr * 100) if tot_curr else 0.0
alert_rate_prev = (act_alerts_prev / len(df_prev_hour) * 100) if not df_prev_hour.empty else 0.0
alert_rate_delta_html = get_delta_html(alert_rate_curr, alert_rate_prev, is_pct=True, invert_color=True)

if alert_rate_curr > 30:
    rate_border = "#EF4444"
elif alert_rate_curr > 15:
    rate_border = "#F59E0B"
else:
    rate_border = "#10B981"

# High-Risk Segments KPI removed - focus on predictive analysis instead

# Speed KPI: pull from telemetry_df or predictions
if not telemetry_df.empty and telemetry_df["speed"].notna().any():
    avg_speed_curr = telemetry_df["speed"].mean()
    t_max = telemetry_df["timestamp"].max()
    t_prev_df = telemetry_df[(telemetry_df["timestamp"] >= t_max - pd.Timedelta("2h")) & (telemetry_df["timestamp"] < t_max - pd.Timedelta("1h"))] if pd.notna(t_max) else pd.DataFrame()
    avg_speed_prev = t_prev_df["speed"].mean() if not t_prev_df.empty else np.nan
else:
    avg_speed_curr = df_raw["speed"].mean()
    avg_speed_prev = df_prev_hour["speed"].mean() if not df_prev_hour.empty else np.nan
avg_speed_delta_html = get_delta_html(avg_speed_curr, avg_speed_prev, invert_color=False)

# Congestion KPI: pull from traffic_df or predictions
if not traffic_df.empty and traffic_df["congestion_level"].notna().any():
    avg_cong_curr = traffic_df["congestion_level"].mean()
    tr_max = traffic_df["timestamp"].max()
    tr_prev_df = traffic_df[(traffic_df["timestamp"] >= tr_max - pd.Timedelta("2h")) & (traffic_df["timestamp"] < tr_max - pd.Timedelta("1h"))] if pd.notna(tr_max) else pd.DataFrame()
    avg_cong_prev = tr_prev_df["congestion_level"].mean() if not tr_prev_df.empty else np.nan
else:
    avg_cong_curr = df_raw["congestion_level"].mean()
    avg_cong_prev = df_prev_hour["congestion_level"].mean() if not df_prev_hour.empty else np.nan
avg_cong_delta_html = get_delta_html(avg_cong_curr, avg_cong_prev, invert_color=True)

# Weather data removed - focus on predictive analysis

# Road Events (Active in last 30 minutes): pull from road_events_df or predictions
if not road_events_df.empty and "timestamp" in road_events_df.columns:
    re_max = road_events_df["timestamp"].max()
    events_curr = len(road_events_df[road_events_df["timestamp"] >= re_max - pd.Timedelta("30T")]) if pd.notna(re_max) else len(road_events_df)
    events_prev = len(road_events_df[(road_events_df["timestamp"] >= re_max - pd.Timedelta("60T")) & (road_events_df["timestamp"] < re_max - pd.Timedelta("30T"))]) if pd.notna(re_max) else 0
else:
    events_30m_df = df_raw[df_raw["timestamp"] >= max_ts - pd.Timedelta("30T")] if pd.notna(max_ts) else df_raw
    events_curr = events_30m_df["active_events"].sum()
    events_prev = df_raw[(df_raw["timestamp"] >= max_ts - pd.Timedelta("60T")) & (df_raw["timestamp"] < max_ts - pd.Timedelta("30T"))]["active_events"].sum() if pd.notna(max_ts) else 0
events_delta_html = get_delta_html(events_curr, events_prev, invert_color=True, format_str="{:+.0f}")

with kpi_container.container():
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="kpi-card-custom" style="border-left: 4px solid #3B82F6;">
            <div class="kpi-title">Predictions Scored</div>
            <div class="kpi-value">{tot_curr:,}</div>
            {tot_delta_html}
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="kpi-card-custom" style="border-left: 4px solid #3B82F6;">
            <div class="kpi-title">Active Alerts</div>
            <div class="kpi-value">{act_alerts_curr:,}</div>
            {act_alerts_delta_html}
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="kpi-card-custom" style="border-left: 4px solid {rate_border};">
            <div class="kpi-title">Alert Rate</div>
            <div class="kpi-value">{alert_rate_curr:.1f}%</div>
            {alert_rate_delta_html}
        </div>
        """, unsafe_allow_html=True)
        
    st.write("")
    
    col5, col6 = st.columns(2)
    with col5:
        speed_str = f"{avg_speed_curr:.1f} km/h" if pd.notna(avg_speed_curr) else "N/A"
        st.markdown(f"""
        <div class="kpi-card-custom" style="border-left: 4px solid #10B981;">
            <div class="kpi-title">Avg Vehicle Speed</div>
            <div class="kpi-value">{speed_str}</div>
            {avg_speed_delta_html}
        </div>
        """, unsafe_allow_html=True)

    with col6:
        st.markdown(f"""
        <div class="kpi-card-custom" style="border-left: 4px solid #EF4444;">
            <div class="kpi-title">Active Events (30m)</div>
            <div class="kpi-value">{int(events_curr)}</div>
            {events_delta_html}
        </div>
        """, unsafe_allow_html=True)


# ── 8. SECTION 4 — Next 2 Hours Predictive Forecast Box ─────────────────────
risk_lbl, risk_cls, proj_prob, exp_alerts, primary, vulnerable = predict_next_2_hours(df_current_hour, threshold)

vulnerable_html = ""
if not vulnerable.empty:
    for seg, p in vulnerable.items():
        vulnerable_html += f'<div style="margin-bottom: 8px;"><div style="display: flex; justify-content: space-between; font-size: 0.85rem; color: #E2E8F0; margin-bottom: 2px;"><span>{seg}</span><span style="font-weight: 700; color: #EF4444;">{p:.1%}</span></div><div style="width: 100%; background: #1E293B; border-radius: 4px; height: 6px; overflow: hidden;"><div style="width: {p * 100}%; background: #EF4444; height: 6px;"></div></div></div>'
else:
    vulnerable_html = f'<div style="color: #64748B; font-style: italic; font-size: 0.9rem; margin-top: 5px;">No segments currently exceeding {threshold*100:.0f}% incident risk.</div>'

st.markdown(f"""
<div class="glass-card" style="padding: 24px; margin-bottom: 25px;">
    <div style="display: flex; gap: 40px; flex-wrap: wrap;">
        <!-- Left Side -->
        <div style="flex: 1; min-width: 320px; border-right: 1px solid rgba(255,255,255,0.07); padding-right: 30px;">
            <h4 style="margin: 0 0 16px 0; color: #38BDF8; font-size: 1.25rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;">
                2-Hour Predictive Forecast
            </h4>
            <div style="display: flex; align-items: center; gap: 20px;">
                <span class="risk-badge {risk_cls}">{risk_lbl} RISK</span>
                <span style="font-size: 2.5rem; font-weight: 800; color: #F8FAFC; letter-spacing: -0.02em;">{proj_prob:.1%}</span>
            </div>
            <div style="margin-top: 18px; font-size: 0.95rem; color: #94A3B8;">
                Expected active alerts: <strong style="color: #F8FAFC; font-size: 1.15rem;">{exp_alerts}</strong> monitored segments
            </div>
        </div>
        <!-- Right Side -->
        <div style="flex: 2; min-width: 420px;">
            <h4 style="margin: 0 0 16px 0; color: #38BDF8; font-size: 1.25rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;">
                Attribution & Segment Vulnerabilities
            </h4>
            <div style="margin-bottom: 16px; font-size: 1rem; color: #E2E8F0;">
                Primary Incident Driver: <span style="font-weight: 700; color: #F87171;">{primary}</span>
            </div>
            <div>
                <div style="font-size: 0.85rem; color: #94A3B8; margin-bottom: 8px; text-transform: uppercase; font-weight: 600; letter-spacing: 0.03em;">
                    Priority Segments to Monitor (Mean Risk &gt; {threshold*100:.0f}%)
                </div>
                {vulnerable_html}
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── 9. SECTION 5 — Operational Recommendations Engine ─────────────────────
st.markdown("""
<h3 style="color: #F8FAFC; font-weight: 700; margin-bottom: 20px;">
    Operational Recommendations
</h3>
""", unsafe_allow_html=True)

# Generate recommendations
recommendations = generate_recommendations(df_raw)
recommendations = [r for r in recommendations if r.get("category") not in ["Resource Deployment", "System Status"]]
save_recommendations_to_db(recommendations, PREDICTIONS_DB)

# Debug: Show data diagnostics
with st.expander("Recommendation Diagnostics", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Segments Evaluated", len(df_raw))
        st.metric("Segments with will_alert=1", int(df_raw["will_alert"].sum()))
        st.metric("Segments with alert_prob > 0.3", len(df_raw[df_raw["alert_prob"] > 0.3]))
    with col2:
        st.metric("Recommendations Generated", len(recommendations))
        st.metric("Avg alert_prob in data", f"{df_raw['alert_prob'].mean():.3f}")
        st.metric("Max alert_prob in data", f"{df_raw['alert_prob'].max():.3f}")
    
    # Top recommendations section
    st.markdown("---")
    st.markdown("### Top Priority Recommendations")
    
    critical_recs = [r for r in recommendations if r["priority"] == "CRITICAL"]
    high_recs = [r for r in recommendations if r["priority"] == "HIGH"]
    
    top_recs = (critical_recs + high_recs)[:3]  # Top 3 recommendations
    
    if top_recs:
        for i, rec in enumerate(top_recs, 1):
            priority = rec["priority"]
            action = rec["action"]
            segment = rec["segment_id"]
            confidence = rec["confidence"]
            
            badge_colors = {
                "CRITICAL": "#EF4444",
                "HIGH": "#F97316",
                "MODERATE": "#F59E0B",
                "LOW": "#10B981"
            }
            badge_color = badge_colors.get(priority, "#10B981")
            
            st.markdown(f"""
            <div style="background: rgba(30, 41, 59, 0.5); border-left: 3px solid {badge_color}; border-radius: 8px; padding: 12px; margin-bottom: 12px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                    <span style="background: {badge_color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 700;">{priority}</span>
                    <span style="color: #94A3B8; font-size: 0.85rem;">Segment {segment}</span>
                    <span style="color: #F8FAFC; font-weight: 600;">{confidence:.0%}</span>
                </div>
                <div style="color: #F8FAFC; font-size: 0.95rem;">{action}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No critical or high priority recommendations at this time.")

# Summary ribbon with 4 mini cards
critical_count = sum(1 for r in recommendations if r["priority"] == "CRITICAL")
high_count = sum(1 for r in recommendations if r["priority"] == "HIGH")
moderate_count = sum(1 for r in recommendations if r["priority"] == "MODERATE")
low_count = sum(1 for r in recommendations if r["priority"] == "LOW")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div style="background: rgba(239, 68, 68, 0.15); border-left: 4px solid #EF4444; border-radius: 8px; padding: 12px; text-align: center;">
        <div style="color: #FCA5A5; font-size: 0.85rem; font-weight: 600;">CRITICAL</div>
        <div style="color: #EF4444; font-size: 2rem; font-weight: 800;">{critical_count}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="background: rgba(249, 115, 22, 0.15); border-left: 4px solid #F97316; border-radius: 8px; padding: 12px; text-align: center;">
        <div style="color: #FEDBA8; font-size: 0.85rem; font-weight: 600;">HIGH</div>
        <div style="color: #F97316; font-size: 2rem; font-weight: 800;">{high_count}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div style="background: rgba(245, 158, 11, 0.15); border-left: 4px solid #F59E0B; border-radius: 8px; padding: 12px; text-align: center;">
        <div style="color: #FCD34D; font-size: 0.85rem; font-weight: 600;">MODERATE</div>
        <div style="color: #F59E0B; font-size: 2rem; font-weight: 800;">{moderate_count}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div style="background: rgba(16, 185, 129, 0.15); border-left: 4px solid #10B981; border-radius: 8px; padding: 12px; text-align: center;">
        <div style="color: #A7F3D0; font-size: 0.85rem; font-weight: 600;">NOMINAL</div>
        <div style="color: #10B981; font-size: 2rem; font-weight: 800;">{low_count}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# Build category summaries and segment counts
category_summaries = {}
for rec in recommendations:
    cat = rec["category"]
    if cat not in category_summaries:
        category_summaries[cat] = {
            "segments": set(),
            "critical": 0,
            "high": 0,
            "moderate": 0,
            "low": 0,
            "total": 0
        }
    category_summaries[cat]["segments"].add(rec["segment_id"])
    category_summaries[cat][rec["priority"].lower()] = category_summaries[cat].get(rec["priority"].lower(), 0) + 1
    category_summaries[cat]["total"] += 1

# Convert segment sets to lists with counts
for cat in category_summaries:
    category_summaries[cat]["segment_count"] = len(category_summaries[cat]["segments"])
    category_summaries[cat]["segments"] = sorted(list(category_summaries[cat]["segments"]))

# Display category summaries with expandable sections
for category_name in ["Route Diversion", "Maintenance", "Weather Alert"]:
    if category_name in category_summaries:
        summary = category_summaries[category_name]
        total_recs = summary["total"]
        seg_count = summary["segment_count"]
        critical = summary["critical"]
        high = summary["high"]
        
        # Determine color based on severity
        if critical > 0:
            color = "#EF4444"
            severity = "CRITICAL"
        elif high > 0:
            color = "#F97316"
            severity = "HIGH"
        else:
            color = "#F59E0B"
            severity = "MODERATE"
        
        summary_title = f"{category_name} - {seg_count} Segments, {total_recs} Recommendations"
        
        with st.expander(f"{summary_title}", expanded=False):
            # Show priority breakdown
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Critical", summary["critical"], delta=None)
            with col2:
                st.metric("High", summary["high"], delta=None)
            with col3:
                st.metric("Moderate", summary["moderate"], delta=None)
            with col4:
                st.metric("Affected Segments", seg_count, delta=None)
            
            st.markdown("---")
            
            # Show all recommendations in this category
            filtered_recs = [r for r in recommendations if r["category"] == category_name]
            
            if not filtered_recs:
                st.info("No recommendations at this time.")
            else:
                # Render recommendation cards (no nested expanders)
                for idx_rec, rec in enumerate(filtered_recs):
                    priority = rec["priority"]
                    category = rec["category"]
                    action = rec["action"]
                    segment = rec["segment_id"]
                    confidence = rec["confidence"]
                    reasoning = rec["reasoning"]
                    triggered_by = rec["triggered_by"]
                    estimated_impact = rec["estimated_impact"]
                    time_window = rec["time_window"]
                    
                    # Priority badge color
                    badge_color_map = {
                        "CRITICAL": "#EF4444",
                        "HIGH": "#F97316",
                        "MODERATE": "#F59E0B",
                        "LOW": "#10B981"
                    }
                    badge_color = badge_color_map.get(priority, "#10B981")
                    
                    card_html = f"""<div style="background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.07); border-radius: 12px; padding: 18px; margin-bottom: 16px; box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
<div style="display: flex; gap: 8px; align-items: center;">
<span style="background: {badge_color}; color: white; padding: 4px 10px; border-radius: 6px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase;">{priority}</span>
<span style="color: #F8FAFC; font-weight: 600;">Segment {segment}</span>
</div>
<span style="color: #94A3B8; font-size: 0.85rem; font-style: italic;">{time_window}</span>
</div>
<div style="color: #F8FAFC; font-size: 1.1rem; font-weight: 600; margin-bottom: 12px; line-height: 1.5;">{action}</div>
<div style="margin-bottom: 12px;">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; font-size: 0.85rem;">
<span style="color: #94A3B8;">Confidence</span>
<span style="color: #F8FAFC; font-weight: 600;">{confidence:.0%}</span>
</div>
<div style="width: 100%; background: #1E293B; border-radius: 4px; height: 8px; overflow: hidden;">
<div style="width: {confidence * 100:.0f}%; background: {badge_color}; height: 8px; border-radius: 4px;"></div>
</div>
</div>
</div>"""
                    
                    st.markdown(card_html, unsafe_allow_html=True)
                    
                    # Display details inline under each card
                    st.markdown(f"**Reasoning:** {reasoning}")
                    st.markdown("**Contributing Factors:**")
                    for factor in triggered_by:
                        st.markdown(f"- `{factor}`")
                    st.markdown(f"**Estimated Impact:** {estimated_impact}")
                    st.divider()




# Recommendation history
with st.expander("Recommendation History (Last 24 Hours)", expanded=False):
    try:
        con = sqlite3.connect(PREDICTIONS_DB)
        cutoff_time = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        hist_df = pd.read_sql(
            "SELECT generated_at, priority, category, action, segment_id, confidence FROM recommendation_history WHERE generated_at >= ? ORDER BY generated_at DESC LIMIT 100",
            con, params=[cutoff_time]
        )
        con.close()
        
        if not hist_df.empty:
            hist_df["generated_at"] = pd.to_datetime(hist_df["generated_at"])
            hist_df["confidence"] = hist_df["confidence"].apply(lambda x: f"{x:.0%}")
            
            # Color priority column
            priority_colors = {
                "CRITICAL": "#EF4444",
                "HIGH": "#F97316",
                "MODERATE": "#F59E0B",
                "LOW": "#10B981"
            }
            
            st.dataframe(
                hist_df.rename(columns={
                    "generated_at": "Generated At",
                    "priority": "Priority",
                    "category": "Category",
                    "action": "Action",
                    "segment_id": "Segment",
                    "confidence": "Confidence"
                }),
                use_container_width=True,
                hide_index=True
            )
            
            st.caption(f"Total recommendations (last 24h): **{len(hist_df)}**")
        else:
            st.info("No recommendation history available yet.")
    except Exception as e:
        st.warning(f"Could not load recommendation history: {str(e)[:50]}")


st.markdown("<h3 class='section-header'>Alert & Congestion Trends</h3>", unsafe_allow_html=True)
if check_cols(df_raw, ["timestamp", "alert_prob", "congestion_level"]):
    df_sorted = df_raw.sort_values("timestamp")
    timeline = df_sorted.set_index("timestamp").resample("30min").agg({
        "alert_prob": ["mean", "max"],
        "congestion_level": "mean"
    }).reset_index()
    timeline.columns = ["timestamp", "avg_alert", "max_alert", "avg_cong"]
    
    c_min = timeline["avg_cong"].min()
    c_max = timeline["avg_cong"].max()
    if pd.notna(c_max) and c_max > c_min:
        timeline["avg_cong_norm"] = (timeline["avg_cong"] - c_min) / (c_max - c_min)
    else:
        timeline["avg_cong_norm"] = timeline["avg_cong"].fillna(0.0)
        
    fig_timeline = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig_timeline.add_trace(
        go.Scatter(
            x=timeline["timestamp"], y=timeline["avg_alert"],
            name="Avg Alert Probability", line=dict(color="#60A5FA", width=2.5)
        ),
        secondary_y=False
    )
    
    fig_timeline.add_trace(
        go.Scatter(
            x=timeline["timestamp"], y=timeline["max_alert"],
            name="Max Alert Probability", line=dict(color="#A78BFA", width=2, dash="dot")
        ),
        secondary_y=False
    )
    
    fig_timeline.add_trace(
        go.Scatter(
            x=timeline["timestamp"], y=timeline["avg_cong_norm"],
            name="Avg Congestion Level (Norm)", line=dict(color="#34D399", width=2)
        ),
        secondary_y=True
    )
    
    fig_timeline.add_hline(
        y=threshold, line_dash="dash", line_color="#F97316",
        annotation_text=f"Alert Threshold ({threshold:.2f})", annotation_position="top left",
        secondary_y=False
    )
    
    if not timeline.empty and timeline["max_alert"].notna().any():
        peak_idx = timeline["max_alert"].idxmax()
        peak_row = timeline.loc[peak_idx]
        fig_timeline.add_trace(
            go.Scatter(
                x=[peak_row["timestamp"]], y=[peak_row["max_alert"]],
                mode="markers+text", marker=dict(color="#EF4444", size=12, symbol="triangle-up"),
                text=["Peak Risk"], textposition="top center", name="Peak Risk Marker",
                showlegend=False
            ),
            secondary_y=False
        )
        
    fig_timeline.update_layout(
        xaxis_title="Timeline",
        yaxis_title="Probability",
        yaxis2_title="Congestion (Normalized)",
        hovermode="x unified",
        legend=dict(orientation="h", y=1.1, x=0.0)
    )
    apply_plotly_dark_theme(fig_timeline)
    st.plotly_chart(fig_timeline, use_container_width=True)
else:
    st.info("Timeline data not available yet")


st.markdown("<h3 class='section-header'>Operational Spatial Risk Map</h3>", unsafe_allow_html=True)
if check_cols(df_raw, ["segment_id"]):
    df_coords = df_raw.copy()
    # Deduplicate segments to show only the most recent prediction for each segment
    if "scored_at" in df_coords.columns:
        df_coords = df_coords.sort_values("scored_at").groupby("segment_id").last().reset_index()
    elif "timestamp" in df_coords.columns:
        df_coords = df_coords.sort_values("timestamp").groupby("segment_id").last().reset_index()
        
    coords = df_coords["segment_id"].apply(parse_coords)
    df_coords["latitude"] = [c[0] for c in coords]
    df_coords["longitude"] = [c[1] for c in coords]
    
    valid_rows = df_coords[df_coords["latitude"].notna() & df_coords["longitude"].notna()]
    valid_pct = len(valid_rows) / len(df_raw) if len(df_raw) > 0 else 0
    
    if valid_pct >= 0.1:
        # Prepare hover data with proper custom formatting
        valid_rows["hover_text"] = (
            "Segment: " + valid_rows["segment_id"].astype(str) + "<br>" +
            "Alert Probability: " + (valid_rows["alert_prob"] * 100).round(1).astype(str) + "%<br>" +
            "Congestion Level: " + valid_rows["congestion_level"].fillna(0).round(2).astype(str) + "<br>" +
            "Event Type: " + valid_rows["event_type"].fillna("None").astype(str)
        )
        
        # Color based on will_alert status - green if below threshold, red if above
        valid_rows["alert_status"] = valid_rows["will_alert"].astype(int)
        
        fig_map = px.scatter_mapbox(
            valid_rows,
            lat="latitude",
            lon="longitude",
            color="alert_status",
            size=valid_rows["alert_prob"].clip(lower=0.01),
            color_continuous_scale=[[0, "#10B981"], [1, "#EF4444"]],
            range_color=[0, 1],
            zoom=10,
            center=dict(lat=30.06, lon=31.24),
            mapbox_style="carto-positron",
            hover_data={"latitude": False, "longitude": False, "alert_status": False, "alert_prob": True, "hover_text": True},
            hover_name="hover_text"
        )
        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#F8FAFC", family="Outfit, sans-serif"),
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        df_treemap = df_raw.copy()
        df_treemap["alert_prob_val"] = df_treemap["alert_prob"].clip(lower=0.01)
        
        fig_tree = px.treemap(
            df_treemap,
            path=["segment_id"],
            values="alert_prob_val",
            color="alert_prob",
            color_continuous_scale="Reds",
            hover_data=["congestion_level", "event_type", "weather_risk", "vehicle_count"]
        )
        fig_tree.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#F8FAFC", family="Outfit, sans-serif"),
        )
        st.plotly_chart(fig_tree, use_container_width=True)
else:
    st.info("Segment data not available yet")


# ── 11. SECTION 7 — Vehicle Behavior Panel (2 charts) ───────────────────────
st.markdown("<h3 class='section-header'>Vehicle Behavior Analysis (S3 Telemetry)</h3>", unsafe_allow_html=True)
if not telemetry_df.empty and telemetry_df["speed"].notna().any() and telemetry_df["acceleration"].notna().any():
    # Ensure segment_id is consistently typed across all dataframes before merging
    if "segment_id" in telemetry_df.columns:
        telemetry_df["segment_id"] = telemetry_df["segment_id"].astype(str)
    if "segment_id" in df_raw.columns:
        df_raw["segment_id"] = df_raw["segment_id"].astype(str)
    if not traffic_df.empty and "segment_id" in traffic_df.columns:
        traffic_df["segment_id"] = traffic_df["segment_id"].astype(str)
    
    telemetry_df["window_start"] = telemetry_df["timestamp"].dt.floor("30T")
    df_raw["window_start"] = df_raw["timestamp"].dt.floor("30T")
    
    telemetry_merged = pd.merge(
        telemetry_df,
        df_raw[["segment_id", "window_start", "will_alert", "alert_prob"]].drop_duplicates(),
        on=["segment_id", "window_start"],
        how="left"
    )
    telemetry_merged["will_alert"] = telemetry_merged["will_alert"].fillna(0).astype(int)
    telemetry_merged["alert_prob"] = telemetry_merged["alert_prob"].fillna(0.0)
    
    if "congestion_level" not in telemetry_merged.columns:
        if not traffic_df.empty and traffic_df["congestion_level"].notna().any():
            traffic_df["window_start"] = traffic_df["timestamp"].dt.floor("30T")
            telemetry_merged = pd.merge(
                telemetry_merged,
                traffic_df[["segment_id", "window_start", "congestion_level"]].drop_duplicates(),
                on=["segment_id", "window_start"],
                how="left"
            )
        else:
            telemetry_merged = pd.merge(
                telemetry_merged,
                df_raw[["segment_id", "window_start", "congestion_level"]].drop_duplicates(),
                on=["segment_id", "window_start"],
                how="left"
            )
    telemetry_merged["congestion_level"] = telemetry_merged["congestion_level"].fillna(0.1)
    
    col1, col2 = st.columns(2)
    with col1:
        fig_speed_dist = px.histogram(
            telemetry_merged, x="speed", color="will_alert",
            title="Speed Distribution by Alert Status (Live Telemetry)",
            color_discrete_map={0: "#10B981", 1: "#EF4444"},
            labels={"speed": "Speed (km/h)", "will_alert": "Alert Active"}
        )
        apply_plotly_dark_theme(fig_speed_dist)
        st.plotly_chart(fig_speed_dist, use_container_width=True)
        
    with col2:
        fig_speed_accel = px.scatter(
            telemetry_merged, x="speed", y="acceleration", color="alert_prob",
            size=telemetry_merged["congestion_level"].fillna(0.01).clip(lower=0.01),
            color_continuous_scale="Reds",
            title="Speed vs Acceleration Risk Profile (Live Telemetry)",
            labels={"speed": "Speed (km/h)", "acceleration": "Acceleration (m/s²)", "alert_prob": "Alert Prob"}
        )
        apply_plotly_dark_theme(fig_speed_accel)
        st.plotly_chart(fig_speed_accel, use_container_width=True)
        
    X = telemetry_merged["speed"].quantile(0.80)
    Y = telemetry_merged["acceleration"].quantile(0.80)
    if pd.isna(X) or X == 0: X = 80.0
    if pd.isna(Y) or Y == 0: Y = 2.0
    
    high_behavior = telemetry_merged[(telemetry_merged["speed"] > X) & (telemetry_merged["acceleration"] > Y)]
    baseline_behavior = telemetry_merged[~((telemetry_merged["speed"] > X) & (telemetry_merged["acceleration"] > Y))]
    
    p_high = high_behavior["will_alert"].mean() if not high_behavior.empty else 0.0
    p_base = baseline_behavior["will_alert"].mean() if not baseline_behavior.empty else 0.0
    lift_ratio = p_high / p_base if p_base > 0 else 1.0
    
    st.markdown(f"""
    <div class="glass-card">
        <p style="margin: 0; color: #E2E8F0; font-size: 0.95rem;">
        <strong>BUSINESS METRIC:</strong> Vehicles exceeding <strong>{X:.1f} km/h</strong> with acceleration rates above <strong>{Y:.2f} m/s²</strong> exhibit a <strong>{lift_ratio:.1f}x</strong> higher risk of triggering traffic alert incidents compared to normal driving behavior.
        </p>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info("Vehicle telemetry data not available or lacks speed/acceleration values.")


# ── 12. SECTION 8 — Road Event Intelligence Panel (2 charts) ─────────────────
st.markdown("<h3 class='section-header'>Road Event & Incident Intelligence (S3 Road Events)</h3>", unsafe_allow_html=True)
if not road_events_df.empty and road_events_df["event_type"].notna().any():
    # Ensure segment_id is consistently typed before merging
    if "segment_id" in road_events_df.columns:
        road_events_df["segment_id"] = road_events_df["segment_id"].astype(str)
    if "segment_id" in df_raw.columns:
        df_raw["segment_id"] = df_raw["segment_id"].astype(str)
    
    road_events_df["window_start"] = road_events_df["timestamp"].dt.floor("30T")
    df_raw["window_start"] = df_raw["timestamp"].dt.floor("30T")
    
    events_merged = pd.merge(
        road_events_df,
        df_raw[["segment_id", "window_start", "alert_prob"]].drop_duplicates(),
        on=["segment_id", "window_start"],
        how="left"
    )
    events_merged["alert_prob"] = events_merged["alert_prob"].fillna(0.0)
    events_merged["event_type"] = events_merged["event_type"].fillna("None")
    
    col1, col2 = st.columns(2)
    with col1:
        ev_summary = events_merged.groupby("event_type").agg(
            count=("timestamp", "count"),
            avg_prob=("alert_prob", "mean")
        ).reset_index()
        fig_ev_types = px.bar(
            ev_summary, x="event_type", y="count", color="avg_prob",
            title="Incident Types Driving Alerts",
            color_continuous_scale="Reds",
            labels={"event_type": "Event Type", "count": "Frequency", "avg_prob": "Avg Risk"}
        )
        apply_plotly_dark_theme(fig_ev_types)
        st.plotly_chart(fig_ev_types, use_container_width=True)
        
    with col2:
        df_ev_sort = events_merged.sort_values("timestamp")
        ev_timeline = df_ev_sort.set_index("timestamp").resample("30min").agg({
            "event_type": "count",
            "severity": "mean"
        }).reset_index().rename(columns={"event_type": "active_events"}).fillna(0.0)
        
        fig_ev_time = make_subplots(specs=[[{"secondary_y": True}]])
        fig_ev_time.add_trace(
            go.Scatter(
                x=ev_timeline["timestamp"], y=ev_timeline["active_events"],
                name="Active Events", line=dict(color="#38BDF8", width=2.5)
            ),
            secondary_y=False
        )
        fig_ev_time.add_trace(
            go.Scatter(
                x=ev_timeline["timestamp"], y=ev_timeline["severity"],
                name="Mean Severity", line=dict(color="#F87171", width=2, dash="dash")
            ),
            secondary_y=True
        )
        fig_ev_time.update_layout(
            title_text="Road Event Activity & Severity Over Time",
            xaxis_title="Time",
            yaxis_title="Active Events",
            yaxis2_title="Mean Severity",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.1, x=0.0)
        )
        apply_plotly_dark_theme(fig_ev_time)
        st.plotly_chart(fig_ev_time, use_container_width=True)
        
    baseline_alert = df_raw["alert_prob"].mean()
    ev_means = events_merged.groupby("event_type")["alert_prob"].mean()
    valid_means = ev_means.drop("None", errors="ignore")
    if not valid_means.empty:
        top_evt = valid_means.idxmax()
        top_evt_prob = valid_means.max()
        lift_pct = (top_evt_prob - baseline_alert) * 100
    else:
        top_evt = "N/A"
        lift_pct = 0.0
        
    st.markdown(f"""
    <div class="glass-card">
        <p style="margin: 0; color: #E2E8F0; font-size: 0.95rem;">
            <strong>ANALYSIS:</strong> Road events classified as <strong>{top_evt}</strong> correlate with a <strong>{lift_pct:.1f}%</strong> increase in incident alert probability over Cairo's baseline rate.
        </p>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info("Road events data not available or empty.")




st.markdown("<h3 class='section-header'>Root Cause & Pipeline Health</h3>", unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    if check_cols(df_raw, ["speed", "congestion_level", "weather_risk", "active_events", "alert_prob"]):
        q80 = df_raw["alert_prob"].quantile(0.80)
        top_20_df = df_raw[df_raw["alert_prob"] >= q80]
        
        factors = ["speed", "congestion_level", "weather_risk", "active_events"]
        factor_colors = ["#F87171", "#FB923C", "#FBBF24", "#34D399"]
        
        norm_means = []
        lift = {}
        for f in factors:
            min_val = df_raw[f].min()
            max_val = df_raw[f].max()
            if max_val > min_val:
                norm_mean = (top_20_df[f].mean() - min_val) / (max_val - min_val)
            elif max_val > 0:
                norm_mean = top_20_df[f].mean() / max_val
            else:
                norm_mean = 0.0
            norm_means.append(norm_mean)
            
            baseline = df_raw[f].mean()
            top_mean = top_20_df[f].mean()
            lift[f] = (top_mean - baseline) / baseline * 100 if baseline > 0 else 0.0
            
        rc_df = pd.DataFrame({
            "Factor": [f.replace("_", " ").title() for f in factors],
            "Normalized Score": norm_means
        })
        fig_rc = px.bar(
            rc_df, x="Factor", y="Normalized Score", color="Factor",
            color_discrete_sequence=factor_colors,
            title="Root Cause Factors — Top Risk Segments"
        )
        apply_plotly_dark_theme(fig_rc)
        fig_rc.update_layout(showlegend=False)
        st.plotly_chart(fig_rc, use_container_width=True)
        
        sorted_lifts = sorted(lift.items(), key=lambda x: x[1], reverse=True)
        p_fact = sorted_lifts[0][0].replace("_", " ").title()
        p_lift = sorted_lifts[0][1]
        s_fact = sorted_lifts[1][0].replace("_", " ").title()
        s_lift = sorted_lifts[1][1]
        
        st.markdown(f"""
        <div class="glass-card">
            <p style="margin: 0; color: #E2E8F0; font-size: 0.9rem;">
                <strong>ROOT CAUSE ANALYSIS:</strong> Primary driver is <strong>{p_fact}</strong> at <strong>{p_lift:.1f}%</strong> above baseline. Secondary contributor is <strong>{s_fact}</strong> at <strong>{s_lift:.1f}%</strong> above baseline.
            </p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Root cause data not available yet")

with col2:
    if check_cols(df_raw, ["scored_at"]):
        df_health = df_raw.copy()
        df_health["scored_at_dt"] = pd.to_datetime(df_health["scored_at"])
        df_health["scored_at_min"] = df_health["scored_at_dt"].dt.floor("1T")
        ingest_health = df_health.groupby("scored_at_min").size().reset_index(name="record_count")
        
        fig_health = px.bar(
            ingest_health, x="scored_at_min", y="record_count",
            title="Pipeline Ingestion Health — Records per Scoring Run",
            color_discrete_sequence=["#38BDF8"],
            labels={"scored_at_min": "Time of Run", "record_count": "Records Scored"}
        )
        
        median_val = ingest_health["record_count"].median()
        fig_health.add_hline(
            y=median_val, line_dash="dash", line_color="#E2E8F0",
            annotation_text=f"Median ({median_val:.1f})", annotation_position="top left"
        )
        apply_plotly_dark_theme(fig_health)
        st.plotly_chart(fig_health, use_container_width=True)
        
        runs = sorted(pd.to_datetime(df_raw["scored_at"].unique()))
        has_delay = False
        delay_mins = 0
        if len(runs) >= 2:
            run_gap = (runs[-1] - runs[-2]).total_seconds() / 60
            if run_gap > 10:
                has_delay = True
                now_utc = pd.to_datetime(datetime.now(timezone.utc))
                last_scored = runs[-1]
                if last_scored.tzinfo is None:
                    last_scored = last_scored.replace(tzinfo=timezone.utc)
                if now_utc.tzinfo is None:
                    now_utc = now_utc.replace(tzinfo=timezone.utc)
                delay_mins = (now_utc - last_scored).total_seconds() / 60
                
        if has_delay:
            st.markdown(f"""
            <div class="warning-banner">
                Pipeline delay detected — last scoring run was {int(delay_mins)} minutes ago
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background-color: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 8px; padding: 12px; color: #A7F3D0; font-weight: 600; text-align: center;">
                Ingestion pipeline operating within normal latency constraints
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Ingestion health data not available yet")


# ── 15. SECTION 12 — Raw Alerts Table (Full Width) ─────────────────────────
st.markdown("<h3 class='section-header'>Incident Alerts (Detailed View)</h3>", unsafe_allow_html=True)
with st.expander("View Incident Alerts logs", expanded=False):
    if check_cols(df_raw, ["timestamp", "segment_id", "alert_prob", "speed", "congestion_level", "weather_risk", "scored_at"]):
        raw_alerts = df_raw[df_raw["will_alert"] == 1].sort_values("timestamp", ascending=False).head(50)
        
        if not raw_alerts.empty:
            cols_to_use = ["timestamp", "segment_id", "alert_prob", "speed", "congestion_level", "weather_risk", "scored_at"]
            for c in ["event_type", "severity"]:
                if c in df_raw.columns:
                    cols_to_use.append(c)
            raw_display = raw_alerts[cols_to_use].copy()
            
            raw_display["alert_prob"] = (raw_display["alert_prob"] * 100).round(1).astype(str) + "%"
            
            def style_prob(val):
                try:
                    num = float(val.replace("%", "")) / 100
                except Exception:
                    return ""
                if num >= 0.75:
                    return "color: #F87171; font-weight: 800;"
                elif num >= 0.5:
                    return "color: #FBBF24; font-weight: 700;"
                else:
                    return "color: #34D399;"
                    
            styled_df = raw_display.style.applymap(style_prob, subset=["alert_prob"])
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.info("No active alerts generated in this timeframe.")
    else:
        st.info("Alert logs data not available yet")


# ── 16. Real-Time Streaming loop (st.rerun() every 30s) ──────────────────────
time.sleep(30)
st.rerun()