import json
import time
import random
import logging
import signal
import sys
import os
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

BROKER_LIST = os.getenv("KAFKA_BROKERS_LIST")
TOPIC = os.getenv("KAFKA_TOPIC")
# BROKER_LIST = 'kafka-cluster-kafka-brokers.strimzi-kafka:9092'
# TOPIC = 'sensor-data'

logging.info(f"🚀 Using Kafka brokers: {BROKER_LIST} and TOPIC: {TOPIC}")

# Kafka configuration
conf = {
    'bootstrap.servers': BROKER_LIST
}



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
    logging.info("🚀 Starting sensor data publisher...")
    logging.info(f"🚀 Publishing data for sensor: {sensor_id}")
    logging.info(f"🚀 Publishing data to topic {TOPIC} on broker list {BROKER_LIST}")
    try:
        # Create Kafka Producer
        producer = Producer(conf)
        while True:
            data = get_sensor_data(sensor_id)
            producer.produce(TOPIC, key=str(sensor_id), value=json.dumps(data), callback=acked)
            producer.poll(1)  # triggers delivery callbacks
            time.sleep(interval)
    except Exception as e:
        logging.exception("🛑 Stopping publisher.")
        logging.exception("🛑 Producer error occurred.")
        logging.exception(e)
    finally:
        producer.flush()


