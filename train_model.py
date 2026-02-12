from google.cloud import bigquery
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    mean_squared_error
)

# -------------------------------
# Configuration
# -------------------------------

project_id = "online-gaming-487114"
dataset_id = "gaming_analytics"

client = bigquery.Client(project=project_id)

print("Loading data from BigQuery...")

query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.player_features`
"""

df = client.query(query).to_dataframe()

print("Total records loaded:", len(df))

# Drop missing values
df = df.fillna(0)

print("Records after filling Nan:", len(df))
print("Class distribution:")
print(df["high_spender"].value_counts())

# -------------------------------
# Feature Selection
# IMPORTANT: Remove avg_purchase to avoid leakage
# -------------------------------

X = df[[
    "avg_score",
    "avg_kills",
    "avg_latency",
    "avg_session_duration",
    "active_hours"
]]

y = df["high_spender"]

# -------------------------------
# Train-Test Split
# -------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

print("Training model...")

model = LogisticRegression(max_iter=1000)

model.fit(X_train, y_train)

print("Model training completed.")

# -------------------------------
# Predictions
# -------------------------------

y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

# -------------------------------
# Evaluation Metrics
# -------------------------------

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, zero_division=0)
recall = recall_score(y_test, y_pred, zero_division=0)
f1 = f1_score(y_test, y_pred, zero_division=0)

rmse = np.sqrt(mean_squared_error(y_test, y_prob))

print("\n--- Model Evaluation (High-Spender Prediction) ---")
print("Accuracy:", round(accuracy, 4))
print("Precision:", round(precision, 4))
print("Recall:", round(recall, 4))
print("F1 Score:", round(f1, 4))
print("RMSE:", round(rmse, 4))
print("---------------------------------------------------")



import joblib

joblib.dump(model, "high_spender_model.pkl")
print("Model saved as high_spender_model.pkl")
