from google.cloud import bigquery

project_id = "online-gaming-487114"
dataset_id = "gaming_analytics"

client = bigquery.Client()

print("Starting batch pipeline...")

# Step 1: Create hourly stats table
hourly_query = f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.player_hourly_stats` AS
SELECT
    player_id,
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour,
    COUNT(*) AS session_count,
    SUM(score) AS total_score,
    SUM(kills) AS total_kills,
    SUM(in_game_purchase) AS total_purchase,
    AVG(latency_ms) AS avg_latency,
    AVG(session_duration) AS avg_session_duration
FROM
    `{project_id}.{dataset_id}.raw_events`
WHERE
    score < 10000
GROUP BY
    player_id, hour;
"""

client.query(hourly_query).result()
print("Hourly stats table created.")

# Step 2: Create feature table
feature_query = f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.player_features` AS
SELECT
    player_id,
    AVG(total_score) AS avg_score,
    AVG(total_kills) AS avg_kills,
    AVG(total_purchase) AS avg_purchase,
    AVG(avg_latency) AS avg_latency,
    AVG(avg_session_duration) AS avg_session_duration,
    COUNT(*) AS active_hours,
    CASE 
        WHEN COUNT(*) < 3 THEN 1
        ELSE 0
    END AS high_spender
FROM
    `{project_id}.{dataset_id}.player_hourly_stats`
GROUP BY
    player_id;
"""

client.query(feature_query).result()
print("Feature table created.")

print("Batch pipeline completed successfully.")
