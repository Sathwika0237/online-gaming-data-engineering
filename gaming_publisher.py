import json
import random
import time
import uuid
from datetime import datetime
from google.cloud import pubsub_v1

project_id = "online-gaming-487114"
topic_id = "gaming-events-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def generate_event():
    player_id = f"player_{random.randint(1, 500)}"
    game_id = f"game_{random.randint(1, 5)}"

    # ----------------------------
    # Behavioral Segmentation
    # ----------------------------

    is_high_spender = random.random() < 0.5  # 50% high spenders

    if is_high_spender:
        score = random.randint(1000, 3000)
        kills = random.randint(15, 60)
        session_duration = random.uniform(120, 400)
        latency_ms = random.uniform(20, 120)
        purchase = random.uniform(60, 150)
    else:
        score = random.randint(0, 900)
        kills = random.randint(0, 15)
        session_duration = random.uniform(10, 120)
        latency_ms = random.uniform(120, 350)
        purchase = random.uniform(0, 50)

    event = {
        "event_id": str(uuid.uuid4()),
        "player_id": player_id,
        "game_id": game_id,
        "timestamp": datetime.utcnow().isoformat(),
        "session_duration": session_duration,
        "score": score,
        "kills": kills,
        "in_game_purchase": purchase,
        "region": random.choice(["EU", "US", "APAC"]),
        "device_type": random.choice(["mobile", "pc", "console"]),
        "latency_ms": latency_ms
    }

    return event


print("Starting gaming data publisher...")

while True:
    event = generate_event()
    data = json.dumps(event).encode("utf-8")
    publisher.publish(topic_path, data)
    print("Published event:", event["player_id"])
    time.sleep(1)

