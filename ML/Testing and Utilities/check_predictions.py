import sqlite3

con = sqlite3.connect(r'C:\Users\1bood\smart-city-platform\ml\data\predictions.db')
cur = con.cursor()

# Get summary stats
print("=== PREDICTIONS SUMMARY ===")
summary = cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN will_alert = 1 THEN 1 END) as alerts,
        MIN(alert_prob) as min_prob,
        MAX(alert_prob) as max_prob,
        AVG(alert_prob) as avg_prob
    FROM predictions
""").fetchall()

for row in summary:
    print(f"Total predictions: {row[0]}")
    print(f"Above threshold (will_alert): {row[1]}")
    print(f"Min probability: {row[2]:.4f}")
    print(f"Max probability: {row[3]:.4f}")
    print(f"Avg probability: {row[4]:.4f}")

print("\n=== FIRST 5 PREDICTIONS ===")
rows = cur.execute("""
    SELECT timestamp, segment, alert_prob, will_alert 
    FROM predictions 
    LIMIT 5
""").fetchall()

for row in rows:
    print(f"{row[0]:30} | {row[1]:20} | prob={row[2]:.4f} | alert={row[3]}")

con.close()
