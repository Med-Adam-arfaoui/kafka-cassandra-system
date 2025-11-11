from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import os
import time
import sys
import uuid

sys.stdout.reconfigure(line_buffering=True)

# Environment variables
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "trips"

# Connect to Cassandra with retry
while True:
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect('taxi_tracking')
        print("Connected to Cassandra!")
        break
    except Exception as e:
        print(f"[Cassandra] Not ready: {e}. Retrying in 5s...", flush=True)
        time.sleep(5)

# Connect to Kafka with retry
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
        print(f"[Kafka] Not ready: {e}. Retrying in 5s...", flush=True)
        time.sleep(5)

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=f"trips-group-{time.time()}",
    enable_auto_commit=True
)

print(f"Consuming topic '{TOPIC}'...")

def convert_trip_id(trip_id):
    """Convert UUID string to integer (not recommended for production)"""
    try:
        # This is a hack - converts UUID to int by taking first 8 characters
        # NOT recommended for production as it can cause collisions
        return int(trip_id.replace('-', '')[:8], 16)
    except:
        # Fallback: generate a random int
        import random
        return random.randint(1, 1000000)

while True:
    msg_pack = consumer.poll(timeout_ms=1000)
    if not msg_pack:
        print("no msg pack")
        time.sleep(5)
        continue
    for tp, messages in msg_pack.items():
        for message in messages:
            data = message.value
            print(f"Received trip: trip_id={data.get('trip_id')}, taxi_id={data.get('taxi_id')}")
            
            try:
                # Convert trip_id from UUID string to int if needed
                trip_id = data['trip_id']
                if isinstance(trip_id, str) and len(trip_id) == 36:  # UUID format
                    trip_id = convert_trip_id(trip_id)
                
                # Insert into Cassandra trips table
                session.execute("""
                    INSERT INTO trips (trip_id, taxi_id, start_time, end_time, trip_distance, amount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    trip_id,
                    data['taxi_id'],
                    data['start_time'],
                    data['end_time'],
                    data['trip_distance'],
                    data['amount']
                ))
                print(f"Successfully inserted trip: {trip_id}")
                
            except Exception as e:
                print(f"Error inserting trip: {e}")