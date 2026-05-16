"""Simple Kafka consumer helpers."""
import json
from typing import Callable

from kafka import KafkaConsumer

from .config import BOOTSTRAP_SERVERS, TOPIC


def create_consumer(topic: str = TOPIC, group_id: str = "consumer-group") -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def consume_forever(callback: Callable[[dict], None] = lambda m: print(m)) -> None:
    """Consume messages forever and call `callback` for each parsed JSON message."""
    consumer = create_consumer()
    for record in consumer:
        callback(record.value)


__all__ = ["create_consumer", "consume_forever"]
