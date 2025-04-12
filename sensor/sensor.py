import json
import random

import json
import time
import random
from confluent_kafka import Producer

BROKER_LIST = ['kafka-cluster-brokers.strimzi-kafka:9092']

# Kafka configuration
conf = {
    'bootstrap.servers': BROKER_LIST
}

# Kafka topic
TOPIC = 'sensor-data'

# Create Kafka Producer
producer = Producer(conf)

# Function to simulate sensor data
def get_sensor_data(sensor_id):
    return {
        'sensor_id': sensor_id,
        'temperature': round(random.uniform(20.0, 30.0), 2),
        'humidity': round(random.uniform(30.0, 60.0), 2),
        'timestamp': int(time.time())
    }

def acked(err, msg):
    """
    Acknowledgement callback triggered on message delivery.

    :param err: Error instance indicating delivery result (or None on success)
    :param msg: Delivered message or Message object
    """

    if err is not None:
        print(f"❌ Failed to deliver message: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# Main loop to publish data
def publish_sensor_data(sensor_id, interval=2):
    print("🚀 Starting sensor data publisher...")
    try:
        while True:
            data = get_sensor_data(sensor_id)
            producer.produce(TOPIC, key=str(sensor_id), value=json.dumps(data), callback=acked)
            producer.poll(1)  # triggers delivery callbacks
            time.sleep(interval)
    except KeyboardInterrupt:
        print("🛑 Stopping publisher.")
    finally:
        producer.flush()


