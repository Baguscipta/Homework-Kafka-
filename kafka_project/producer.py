"""Simple Kafka producer helpers."""
import json
from typing import List, Any

from .config import BOOTSTRAP_SERVERS, TOPIC


def _json_serializer(data: Any) -> bytes:
    return json.dumps(data).encode("utf-8")


def create_producer():
    # Import kafka client lazily so tests that don't need Kafka broker can run
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=_json_serializer)


def send_messages(messages: List[Any], topic: str = TOPIC) -> None:
    """Send a list of JSON-serializable messages to `topic`."""
    producer = create_producer()
    for msg in messages:
        producer.send(topic, msg)
    producer.flush()


__all__ = ["send_messages", "create_producer", "_json_serializer"]
