"""Kafka producer with key-based partitioning."""
import json
import time
import random
from typing import List, Any

from .config import BOOTSTRAP_SERVERS, TOPIC, USER_KEYS, PRODUCER_INTERVAL_SECONDS


def _json_serializer(data: Any) -> bytes:
    return json.dumps(data).encode("utf-8")


def _string_serializer(data: str) -> bytes:
    return data.encode("utf-8")


def create_producer():
    # Import kafka client lazily so tests that don't need Kafka broker can run
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=_json_serializer,
        key_serializer=_string_serializer
    )


def send_messages(messages: List[Any], topic: str = TOPIC) -> None:
    """Send a list of JSON-serializable messages to `topic` with key-based partitioning.
    
    Messages are sent every PRODUCER_INTERVAL_SECONDS with random user keys.
    """
    producer = create_producer()
    try:
        for i, msg in enumerate(messages):
            # Select a random user key for partition determination
            key = random.choice(USER_KEYS)
            
            print(f"[Producer] Sending message {i+1}/{len(messages)} with key='{key}' to topic '{topic}'")
            producer.send(topic, key=key, value=msg)
            
            # Wait PRODUCER_INTERVAL_SECONDS before sending next message
            if i < len(messages) - 1:
                time.sleep(PRODUCER_INTERVAL_SECONDS)
        
        producer.flush()
        print("[Producer] All messages sent successfully")
    finally:
        producer.close()


def send_messages_forever(topic: str = TOPIC) -> None:
    """Send messages in an infinite loop, every PRODUCER_INTERVAL_SECONDS."""
    from .utils import load_messages
    
    producer = create_producer()
    message_counter = 0
    
    try:
        while True:
            # Load messages and cycle through them
            messages = load_messages('sample_data/messages.json')
            for msg in messages:
                key = random.choice(USER_KEYS)
                message_counter += 1
                
                print(f"[Producer] Message #{message_counter}: key='{key}' | user='{msg.get('user_id')}' | event='{msg.get('event_type')}'")
                producer.send(topic, key=key, value=msg)
                producer.flush()
                
                time.sleep(PRODUCER_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("[Producer] Stopped by user")
    finally:
        producer.close()


__all__ = ["send_messages", "send_messages_forever", "create_producer", "_json_serializer"]
