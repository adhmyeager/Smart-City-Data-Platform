#!/usr/bin/env python3
"""Check predictions database."""
import sqlite3

conn = sqlite3.connect('data/predictions.db')
cursor = conn.cursor()

# Get total predictions
cursor.execute('SELECT COUNT(*) FROM predictions')
total = cursor.fetchone()[0]
print(f'Total predictions: {total}')

# Get recent predictions
cursor.execute('SELECT COUNT(*) FROM predictions WHERE scored_at >= datetime("now", "-24 hours")')
recent = cursor.fetchone()[0]
print(f'Recent (24h) predictions: {recent}')

# Get latest timestamp
cursor.execute('SELECT MAX(scored_at) FROM predictions')
latest = cursor.fetchone()[0]
print(f'Latest timestamp: {latest}')

# Get sample of data
cursor.execute('SELECT segment_id, alert_prob, will_alert, scored_at FROM predictions ORDER BY scored_at DESC LIMIT 5')
print('\nLatest 5 predictions:')
for row in cursor.fetchall():
    print(f'  {row}')

conn.close()
