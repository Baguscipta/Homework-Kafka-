#!/usr/bin/env python3
"""Script to run multiple consumers in the same consumer group.

This demonstrates consumer group partitioning where multiple consumers
share the partitions of a topic.
"""
import sys
import time
import threading
from kafka_project.consumer import consume_forever
from kafka_project.config import TOPIC, CONSUMER_GROUP


def run_consumer_thread(consumer_id: str, topic: str, group_id: str):
    """Run a consumer in a separate thread."""
    print(f"[Thread {consumer_id}] Starting consumer {consumer_id}...")
    try:
        consume_forever(topic=topic, group_id=group_id, consumer_id=consumer_id)
    except Exception as e:
        print(f"[Thread {consumer_id}] Error: {e}")


def main():
    """Run 2 consumers in the same consumer group."""
    num_consumers = 2
    topic = TOPIC
    group_id = CONSUMER_GROUP
    
    print("=" * 60)
    print("MULTIPLE CONSUMERS IN SAME CONSUMER GROUP DEMONSTRATION")
    print("=" * 60)
    print(f"Starting {num_consumers} consumers in group '{group_id}'")
    print(f"Topic: {topic}")
    print("=" * 60)
    print()
    
    threads = []
    
    for i in range(1, num_consumers + 1):
        consumer_id = f"consumer-{i}"
        thread = threading.Thread(
            target=run_consumer_thread,
            args=(consumer_id, topic, group_id),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        
        # Stagger the start times slightly
        time.sleep(1)
    
    # Keep main thread alive
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()
