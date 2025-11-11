from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import os
import time
import sys


sys.stdout.reconfigure(line_buffering=True)


# Environment variables
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "positions"

# Cassandra connection with retry
while True:
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect('taxi_tracking')  # make sure keyspace exists
        print("Connected to Cassandra!")
        break
    except Exception as e:
        print(f"[Cassandra] Not ready: {e}. Retrying in 5 seconds...", flush=True)
        time.sleep(5)


# Kafka connection with retry
print(f"Testing connection to Kafka broker: {KAFKA_BROKER} ...")
connected = False
while not connected:
    try:
        test_consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BROKER],
            request_timeout_ms=5000
        )
        metadata = test_consumer.topics()
        print(f"[Kafka] Connected. Topics: {metadata}", flush=True)
        test_consumer.close()
        connected = True
    except Exception as e:
        print(f"[Kafka] Not ready: {e}. Retrying in 5 seconds...", flush=True)
        time.sleep(5)


# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=f"test-group-{time.time()}",  # unique group id
    enable_auto_commit=True
)


print(f"Consuming topic '{TOPIC}'...")

while True:
    # Poll Kafka for new messages
    msg_pack = consumer.poll(timeout_ms=1000)  # wait max 1 second
    if not msg_pack:
        # No new messages
        print("no msg pack")
        time.sleep(5)
        continue
    for tp, messages in msg_pack.items():
        for message in messages:
            data = message.value
            # Print new message
            print(f"Received message: taxi_id={data.get('taxi_id')}, timestamp={data.get('timestamp')}")
            # Insert into Cassandra
            session.execute("""
                INSERT INTO positions (taxi_id, timestamp, latitude, longitude, altitude, available)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                data['taxi_id'],
                data['timestamp'],
                data['latitude'],
                data['longitude'],
                data['altitude'],
                data['available']
            ))
