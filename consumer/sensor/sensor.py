from kafka import KafkaConsumer
import json
import time
import os
from datetime import datetime

class KafkaCheckpointedConsumer:
    def __init__(self, bootstrap_servers, topic, group_id, checkpoint_interval=60, checkpoint_file='kafka_checkpoint.json'):
        """
        Initialize the Kafka consumer with checkpointing capabilities.
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma separated string or list)
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            checkpoint_interval: Interval in seconds between checkpoint saves
            checkpoint_file: File to store/load checkpoint data
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,  # We'll handle commits manually
            auto_offset_reset='earliest',  # Start from earliest if no checkpoint
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        
        self.topic = topic
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = checkpoint_file
        self.last_checkpoint_time = time.time()
        
        # Load previous checkpoint if exists
        self.offsets = self._load_checkpoint()
        
        # Seek to last checkpoint if available
        if self.offsets:
            for partition in self.consumer.assignment():
                if partition.topic == self.topic and partition.partition in self.offsets:
                    self.consumer.seek(partition, self.offsets[partition.partition])
                    print(f"Resumed from checkpoint: partition {partition.partition} offset {self.offsets[partition.partition]}")
    
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
    
    def _save_checkpoint(self):
        """Save current offsets to checkpoint file."""
        try:
            # Get current positions for all assigned partitions
            current_offsets = {}
            for partition in self.consumer.assignment():
                if partition.topic == self.topic:
                    current_offsets[partition.partition] = self.consumer.position(partition)
            
            # Save to file
            with open(self.checkpoint_file, 'w') as f:
                json.dump(current_offsets, f)
            
            self.last_checkpoint_time = time.time()
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
        try:
            for message in self.consumer:
                # Process the message
                process_func(message.value)
                
                # Check if it's time to checkpoint
                current_time = time.time()
                if current_time - self.last_checkpoint_time >= self.checkpoint_interval:
                    self._save_checkpoint()
                    
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            # Save final checkpoint before exiting
            self._save_checkpoint()
            self.consumer.close()


