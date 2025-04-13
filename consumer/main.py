import os
from sensor.sensor import ConfluentKafkaCheckpointedConsumer
from sensor.message_processor import message_processor

BROKER_LIST = os.getenv("KAFKA_BROKERS_LIST")
TOPIC = os.getenv("KAFKA_TOPIC")


def main ():
  """
  Main entry point for the consumer.

  Reads environment variables KAFKA_BROKERS_LIST and KAFKA_TOPIC to connect to a
  Kafka cluster and consume messages from a topic. The messages are processed using
  the message_processor function.

  The consumer is configured to checkpoint every 30 seconds and store the
  checkpoint data in a file named kafka_consumer_checkpoint.json.
  """
  config = {
      'bootstrap_servers': BROKER_LIST,  # Change to your Kafka brokers
      'topic': TOPIC,
      'group_id': 'checkpointed_consumer_group',
      'checkpoint_interval': 30,  # Checkpoint every 30 seconds
      'checkpoint_file': 'kafka_consumer_checkpoint.json'
  }
  
  # Create and run consumer
  consumer = ConfluentKafkaCheckpointedConsumer(**config)
  consumer.process_messages(message_processor)


# Example usage
if __name__ == "__main__":
    main()