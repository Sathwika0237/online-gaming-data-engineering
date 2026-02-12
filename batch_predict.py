import joblib
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, UTC

# -----------------------------------
# Configuration
# -----------------------------------
PROJECT_ID = "online-gaming-487114"
DATASET_ID = "gaming_analytics"
SOURCE_TABLE = "player_features"
TARGET_TABLE = "batch_predictions"

# -----------------------------------
# Load model
# -----------------------------------
print("Loading model...")
with open("high_spender_model.pkl", "rb") as f:
    model = joblib.load(f)

# -----------------------------------
# Load data from BigQuery
# -----------------------------------
print("Loading features from BigQuery...")
client = bigquery.Client()

query = f"""
SELECT
  player_id,
  avg_score,
  avg_kills,
  avg_latency,
  avg_session_duration,
  active_hours
FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}`
"""

df = client.query(query).to_dataframe()

print(f"Total records in this batch: {len(df)}")

if df.empty:
    print("No records found. Exiting.")
    exit()

# -----------------------------------
# Handle missing values
# -----------------------------------
df = df.fillna(0)

# -----------------------------------
# Prepare features
# -----------------------------------
feature_columns = [
    "avg_score",
    "avg_kills",
    "avg_latency",
    "avg_session_duration",
    "active_hours"
]

X = df[feature_columns]

# -----------------------------------
# Make predictions
# -----------------------------------
predictions = model.predict(X)
probabilities = model.predict_proba(X)[:, 1]

df["predicted_high_spender"] = predictions
df["prediction_probability"] = probabilities
df["prediction_timestamp"] = datetime.now(UTC)

# -----------------------------------
# Print Batch Summary
# -----------------------------------
print("\n----- Batch Prediction Summary -----")
print(f"Total records processed: {len(df)}")
print(f"High spender predictions: {df['predicted_high_spender'].sum()}")
print(f"Average prediction probability: {df['prediction_probability'].mean():.4f}")
print("-----------------------------------\n")

# -----------------------------------
# Select ONLY columns matching table schema
# -----------------------------------
output_columns = [
    "player_id",
    "avg_score",
    "avg_kills",
    "avg_latency",
    "avg_session_duration",
    "active_hours",
    "predicted_high_spender",
    "prediction_probability",
    "prediction_timestamp"
]

df = df[output_columns]

# -----------------------------------
# Upload to BigQuery
# -----------------------------------
print("Uploading predictions to BigQuery...")

job = client.load_table_from_dataframe(
    df,
    f"{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}"
)

job.result()

print("Upload completed successfully.")
print("Batch prediction pipeline finished.")

