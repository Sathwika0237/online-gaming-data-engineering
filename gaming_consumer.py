import json
import time
import logging
from google.cloud import pubsub_v1

# ----------------------------
# Configuration
# ----------------------------
project_id = "online-gaming-487114"
subscription_id = "gaming-events-subscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# ----------------------------
# Logging Setup
# ----------------------------
logging.basicConfig(
    filename="consumer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

print("Starting streaming consumer...")
logging.info("Starting streaming consumer...")

# ----------------------------
# Micro-batch Processing Loop
# ----------------------------
while True:
    try:
        logging.info("Starting new micro-batch...")
        
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 100,
            },
            timeout=60,
        )

        total_messages = len(response.received_messages)
        valid_events = 0
        total_score = 0
        total_latency = 0
        high_latency_count = 0
        unique_players = set()

        for received_message in response.received_messages:
            try:
                message_data = received_message.message.data.decode("utf-8")
                data = json.loads(message_data)

                # Required fields validation
                required_fields = [
                    "player_id",
                    "score",
                    "latency_ms",
                    "session_duration",
                    "kills",
                    "in_game_purchase"
                ]

                if not all(field in data for field in required_fields):
                    logging.warning(f"Skipping invalid message: {data}")
                    continue

                # Valid event
                valid_events += 1
                total_score += data["score"]
                total_latency += data["latency_ms"]
                unique_players.add(data["player_id"])

                # Threshold detection (High latency)
                if data["latency_ms"] > 200:
                    high_latency_count += 1

                # Acknowledge message
                subscriber.acknowledge(
                    request={
                        "subscription": subscription_path,
                        "ack_ids": [received_message.ack_id],
                    }
                )

            except Exception as e:
                logging.error(f"Error processing message: {e}")

        # ----------------------------
        # Micro Batch Report
        # ----------------------------
        if total_messages > 0:
            avg_score = total_score / valid_events if valid_events > 0 else 0
            avg_latency = total_latency / valid_events if valid_events > 0 else 0

            print("\n---- Micro Batch Report ----")
            print(f"Total messages pulled: {total_messages}")
            print(f"Valid events processed: {valid_events}")
            print(f"Unique players: {len(unique_players)}")
            print(f"Average score: {round(avg_score, 2)}")
            print(f"Average latency: {round(avg_latency, 2)}")
            print(f"High latency events (>200ms): {high_latency_count}")
            print("----------------------------\n")

            logging.info(
                f"Batch processed | Total: {total_messages} | "
                f"Valid: {valid_events} | Avg Score: {avg_score:.2f} | "
                f"Avg Latency: {avg_latency:.2f} | "
                f"High Latency: {high_latency_count}"
            )

        # Wait 30 seconds before next micro-batch
        time.sleep(30)

    except Exception as e:
        logging.error(f"Streaming error: {e}")
        time.sleep(10)

