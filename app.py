from flask_cors import CORS
from flask import Flask, request, jsonify
import joblib
import pandas as pd
from google.cloud import bigquery
import os

app = Flask(__name__)

# Load model ONCE at startup
model = joblib.load("high_spender_model.pkl")

client = bigquery.Client()
PROJECT_ID = "online-gaming-487114"
DATASET_ID = "gaming_analytics"

# -------------------------------
# Health Check
# -------------------------------
@app.route("/")
def home():
    return "High Spender Prediction API is running."

# -------------------------------
# Predict Endpoint
# -------------------------------
@app.route("/predict", methods=["POST"])
def predict():
    data = request.get_json()

    features = pd.DataFrame([{
        "avg_score": data["avg_score"],
        "avg_kills": data["avg_kills"],
        "avg_latency": data["avg_latency"],
        "avg_session_duration": data["avg_session_duration"],
        "active_hours": data["active_hours"]
    }])

    prediction = model.predict(features)[0]
    probability = model.predict_proba(features)[0][1]

    return jsonify({
        "predicted_high_spender": int(prediction),
        "prediction_probability": float(probability)
    })

# -------------------------------
# Latest Batch Summary Endpoint
# -------------------------------
@app.route("/latest_batch", methods=["GET"])
def latest_batch():
    query = f"""
    SELECT
        COUNT(*) as total_predictions,
        SUM(predicted_high_spender) as high_spenders,
        AVG(prediction_probability) as avg_probability,
        MAX(prediction_timestamp) as latest_timestamp
    FROM `{PROJECT_ID}.{DATASET_ID}.batch_predictions`
    """

    df = client.query(query).to_dataframe()

    return jsonify(df.to_dict(orient="records")[0])


# -------------------------------
# Run App
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

