import os
from sensor.sensor import KafkaCheckpointedConsumer

BROKER_LIST = os.getenv("KAFKA_BROKERS_LIST")
TOPIC = os.getenv("KAFKA_TOPIC")

def message_processor(message):
    """Example message processing function"""
    print(f"Processing message: {message}")
    # Add your message processing logic here

# Example usage
if __name__ == "__main__":

    
    # Configuration
    config = {
        'bootstrap_servers': BROKER_LIST,  # Change to your Kafka brokers
        'topic': TOPIC,
        'group_id': 'checkpointed_consumer_group',
        'checkpoint_interval': 30,  # Checkpoint every 30 seconds
        'checkpoint_file': 'kafka_consumer_checkpoint.json'
    }
    
    # Create and run consumer
    consumer = KafkaCheckpointedConsumer(**config)
    consumer.process_messages(message_processor)