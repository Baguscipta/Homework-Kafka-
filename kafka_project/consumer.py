"""Kafka consumer with event processing and consumer group support."""
import json
from typing import Callable
from collections import defaultdict

from kafka import KafkaConsumer

from .config import BOOTSTRAP_SERVERS, TOPIC, CONSUMER_GROUP


def create_consumer(topic: str = TOPIC, group_id: str = CONSUMER_GROUP, consumer_id: str = "1") -> KafkaConsumer:
    """Create a Kafka consumer with auto commit enabled."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )


def consume_forever(
    topic: str = TOPIC,
    group_id: str = CONSUMER_GROUP,
    consumer_id: str = "1",
    callback: Callable[[dict], None] = None
) -> None:
    """Consume messages forever with event processing.
    
    Counts events per user and displays partition information.
    """
    consumer = create_consumer(topic=topic, group_id=group_id, consumer_id=consumer_id)
    
    # Track statistics
    event_stats = defaultdict(int)
    user_stats = defaultdict(int)
    partition_stats = defaultdict(int)
    total_messages = 0
    
    try:
        print(f"\n[Consumer {consumer_id}] Started consuming from topic '{topic}' (group: '{group_id}')")
        print(f"[Consumer {consumer_id}] Assigned partitions: {consumer.assignment()}\n")
        
        for record in consumer:
            total_messages += 1
            
            # Extract message data
            value = record.value
            user_id = value.get('user_id', 'unknown')
            event_type = value.get('event_type', 'unknown')
            partition = record.partition
            offset = record.offset
            key = record.key.decode('utf-8') if record.key else 'no-key'
            
            # Update statistics
            event_stats[event_type] += 1
            user_stats[user_id] += 1
            partition_stats[partition] += 1
            
            # Display message info
            print(f"[Consumer {consumer_id}] Message #{total_messages}")
            print(f"  Key: {key} | Partition: {partition} | Offset: {offset}")
            print(f"  User: {user_id} | Event: {event_type}")
            print(f"  Timestamp: {record.timestamp}")
            print(f"  Stats: User {user_id} (#{user_stats[user_id]}), Event '{event_type}' (#{event_stats[event_type]})")
            print()
            
            # Call custom callback if provided
            if callback:
                callback(value)
    except KeyboardInterrupt:
        print(f"\n[Consumer {consumer_id}] Stopping...")
    finally:
        # Print final statistics
        print(f"\n[Consumer {consumer_id}] ===== FINAL STATISTICS =====")
        print(f"Total messages processed: {total_messages}")
        print(f"Events per type: {dict(event_stats)}")
        print(f"Events per user: {dict(user_stats)}")
        print(f"Messages per partition: {dict(partition_stats)}")
        print(f"Assigned partitions: {list(consumer.assignment())}")
        print("=" * 40 + "\n")
        
        consumer.close()


__all__ = ["create_consumer", "consume_forever"]
