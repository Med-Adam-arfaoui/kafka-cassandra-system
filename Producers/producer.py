import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer, errors
import socket
import uuid

# ---------- CONFIG ----------
import os
BROKER = os.getenv("BROKER", "kafka:9092")

TOPICS = ["positions", "trips"]
NUM_TAXIS = 20
EVENTS = 20
SLEEP_SEC = 0.2
NUM_PRODUCERS = 3

# ---------- HELPER FUNCTIONS ----------
def random_coord():
    lat = round(random.uniform(40.70, 40.83), 6)
    lon = round(random.uniform(-74.02, -73.93), 6)
    alt = round(random.uniform(0, 50), 2)  # optional altitude
    return lat, lon, alt

def generate_position_event(taxi_id):
    lat, lon, alt = random_coord()
    return {
        "taxi_id": taxi_id,
        "latitude": lat,
        "longitude": lon,
        "altitude": alt,
        "timestamp": datetime.utcnow().isoformat(),
        "available": random.choice([True, False])
    }

def generate_trip_event(taxi_id):
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(minutes=random.randint(5, 60))
    return {
        "trip_id": random.randint(100,10000000),
        "taxi_id": taxi_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "trip_distance": round(random.uniform(5.0, 25.0), 2),
        "amount": round(random.uniform(8.0, 35.0), 2)
    }

# ---------- WAIT FOR KAFKA ----------
def wait_for_kafka(broker, timeout=60):
    print(f"Waiting for Kafka at {broker}...")
    start = time.time()
    host, port = broker.split(":")
    port = int(port)
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f" Kafka is ready at {broker}")
                return True
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Could not connect to Kafka at {broker} within {timeout}s")

# ---------- CREATE PRODUCER ----------
def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# ---------- SEND EVENT ----------
def send_event(producer, topic, value):
    try:
        fut = producer.send(topic, value=value)
        record_metadata = fut.get(timeout=2)
        print(
            f"Delivered to {record_metadata.topic} "
            f"partition {record_metadata.partition} offset {record_metadata.offset} | data={value}"
        )
    except errors.KafkaTimeoutError:
        print(f"Timeout sending message to {topic}")
    except AssertionError as e:
        print(f"Partition error: {e}")

# ---------- MAIN ----------
if __name__ == "__main__":
    wait_for_kafka(BROKER)

    producers = [create_producer() for _ in range(NUM_PRODUCERS)]

    try:
        for i, producer in enumerate(producers):
            for j in range(EVENTS):
                taxi_id = random.randint(1, NUM_TAXIS)

                # Choose topic and generate matching event
                if random.choice([True, False]):
                    topic = "positions"
                    value = generate_position_event(taxi_id)
                else:
                    topic = "trips"
                    value = generate_trip_event(taxi_id)

                send_event(producer, topic, value)
                time.sleep(SLEEP_SEC)

            producer.flush()

        print("Done producing events to Kafka.")

    finally:
        for p in producers:
            p.close()
