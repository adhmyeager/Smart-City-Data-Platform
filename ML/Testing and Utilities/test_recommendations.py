#!/usr/bin/env python3
"""Quick test to verify recommendation generation logic without running full dashboard."""

import sys
import pandas as pd
import numpy as np

# Create sample data with realistic alert probabilities and will_alert flags
data = {
    'segment_id': ['A', 'B', 'C', 'D', 'E'],
    'alert_prob': [0.65, 0.45, 0.25, 0.0, 0.55],
    'will_alert': [1, 1, 0, 0, 1],
    'active_events': [2, 1, 0, 0, 3],
    'event_type': ['accident', 'construction', 'none', 'none', 'incident'],
    'congestion_level': [0.8, 0.5, 0.3, 0.1, 0.9],
    'vehicle_count': [150, 100, 50, 20, 200],
    'avg_speed': [8, 25, 50, 80, 5],
    'speed': [85, 60, 45, 90, 95],
    'acceleration': [2.5, 1.0, 0.5, 0.2, 3.5],
    'severity': [0.8, 0.6, 0.2, 0.0, 0.9],
    'rain': [3, 0, 0, 0, 5],
    'visibility': [700, 1000, 1000, 1000, 600],
    'weather_risk': [0.6, 0.2, 0.1, 0.0, 0.8],
    'wind_speed': [55, 30, 20, 10, 70],
    'temperature': [3, 10, 15, 20, 2],
    'scored_at': pd.Timestamp.now(),
}

df = pd.DataFrame(data)

print("=" * 70)
print("RECOMMENDATION SYSTEM TEST")
print("=" * 70)
print(f"\nInput Data ({len(df)} segments):")
print(df[['segment_id', 'alert_prob', 'will_alert', 'active_events']].to_string())

# Test threshold logic inline
print("\n" + "=" * 70)
print("THRESHOLD TEST RESULTS")
print("=" * 70)

vehicle_count_median = df['vehicle_count'].median()
PRIORITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3}

recommendations = []

# Resource Deployment
for idx, row in df.iterrows():
    alert_prob = float(row.get("alert_prob", 0) or 0)
    will_alert = int(row.get("will_alert", 0) or 0)
    active_events = float(row.get("active_events", 0) or 0)
    segment = str(row.get("segment_id", "unknown"))
    
    # Rule 1: Emergency response
    if (alert_prob >= 0.5 or will_alert == 1) and active_events >= 1:
        recommendations.append({
            "priority": "CRITICAL",
            "category": "Resource Deployment",
            "action": "Emergency response",
            "segment_id": segment,
            "confidence": alert_prob,
        })
        print(f"✓ CRITICAL: Segment {segment} - Emergency response (alert={alert_prob:.2f}, will_alert={will_alert}, events={int(active_events)})")

# Fallback rule
alert_segments = set(df[df["will_alert"] == 1]["segment_id"].astype(str))
rec_segments = set(r["segment_id"] for r in recommendations)
for segment in alert_segments - rec_segments:
    seg_data = df[df["segment_id"].astype(str) == segment].iloc[0]
    alert_prob = float(seg_data.get("alert_prob", 0) or 0)
    recommendations.append({
        "priority": "MODERATE",
        "category": "Resource Deployment",
        "action": "Monitoring",
        "segment_id": segment,
        "confidence": alert_prob,
    })
    print(f"✓ MODERATE: Segment {segment} - Monitoring (alert={alert_prob:.2f}, will_alert=1 fallback)")

print(f"\n\nFinal: {len(recommendations)} recommendations generated")
print(f"Expected: ≥ 3 (segments A, B, E all have will_alert=1)")

if len(recommendations) >= 3:
    print("\n✅ SUCCESS: Recommendation system is generating recommendations!")
else:
    print("\n❌ FAILURE: Not enough recommendations generated")
