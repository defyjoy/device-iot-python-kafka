from confluent_kafka import Consumer, KafkaException, KafkaError,TopicPartition
import json
import time
import os
from datetime import datetime

class ConfluentKafkaCheckpointedConsumer:
    def __init__(self, bootstrap_servers, topic, group_id, checkpoint_interval=60, checkpoint_file='kafka_checkpoint.json'):
        """
        Initialize the Kafka consumer with checkpointing capabilities using confluent_kafka.
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma separated string)
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            checkpoint_interval: Interval in seconds between checkpoint saves
            checkpoint_file: File to store/load checkpoint data
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start from earliest if no checkpoint
            'enable.auto.commit': False,  # We'll handle commits manually
            'enable.auto.offset.store': False,  # Disable auto offset store
            'on_commit': self._on_commit_callback
        }
        
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = checkpoint_file
        self.last_checkpoint_time = time.time()
        self.running = False
        
        # Subscribe to topic
        self.consumer.subscribe([topic], on_assign=self._on_assign)
        
    def _on_assign(self, consumer, partitions):
        """Callback when partitions are assigned."""
        # Load previous checkpoint if exists
        offsets = self._load_checkpoint()
        
        if offsets:
            # Seek to the checkpointed offsets
            for p in partitions:
                if p.partition in offsets:
                    p.offset = offsets[p.partition]
                    print(f"Resuming partition {p.partition} from offset {p.offset}")
            
            # Apply the offsets
            self.consumer.assign(partitions)
    
    def _on_commit_callback(self, err, partitions):
        """Callback for commit completion."""
        if err is not None:
            print(f"Failed to commit offsets: {err}")
        else:
            print(f"Successfully committed offsets for partitions: {partitions}")
    
    def _load_checkpoint(self):
        """Load offsets from checkpoint file if it exists."""
        if not os.path.exists(self.checkpoint_file):
            return {}
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                # Convert partition numbers from string to int (JSON keys are always strings)
                return {int(partition): offset for partition, offset in data.items()}
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return {}
    
    def _save_checkpoint(self, force=False):
        """Save current offsets to checkpoint file."""
        current_time = time.time()
        if not force and (current_time - self.last_checkpoint_time < self.checkpoint_interval):
            return False
        
        try:
            # Get committed offsets for all assigned partitions
            committed = self.consumer.committed(self.consumer.assignment())
            
            # Get current positions for all assigned partitions
            current_offsets = {}
            for partition in self.consumer.assignment():
                if partition.topic == self.topic:
                    current_offsets[partition.partition] = self.consumer.position(partition)
            
            # Save to file
            with open(self.checkpoint_file, 'w') as f:
                json.dump(current_offsets, f)
            
            self.last_checkpoint_time = current_time
            print(f"{datetime.now()} - Checkpoint saved for {len(current_offsets)} partitions")
            return True
        except Exception as e:
            print(f"Error saving checkpoint: {e}")
            return False
    
    def process_messages(self, process_func):
        """
        Process messages from Kafka, calling the provided function for each message.
        Periodically saves checkpoints.
        
        Args:
            process_func: Function to call for each message (message value will be passed)
        """
        self.running = True
        try:
            while self.running:
                msg = self.consumer.poll(1.0)  # Wait for message with timeout
                
                if msg is None:
                    # No message received within timeout period
                    self._save_checkpoint()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                    continue
                
                # Process the message
                try:
                    process_func(msg.value())
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
                
                tp = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                
                # Store offset for next commit
                self.consumer.store_offsets(offsets=[tp])
                
                # Periodically commit stored offsets
                self._save_checkpoint()
                
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            # Save final checkpoint before exiting
            self._save_checkpoint(force=True)
            self.consumer.close()
            self.running = False