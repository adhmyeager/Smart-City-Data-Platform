"""Inspect silver data schema."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.extract import load_silver_data

print("Loading silver data from S3...")
data = load_silver_data()

for key, df in data.items():
    if not df.empty:
        print(f"\n{key}:")
        print(f"  Shape: {df.shape[0]} rows × {df.shape[1]} cols")
        print(f"  Columns: {df.columns.tolist()}")
        print(f"  Data types:\n{df.dtypes}")
        print(f"  First row:\n{df.iloc[0]}")
    else:
        print(f"\n{key}: EMPTY")
