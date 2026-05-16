import os

# Kafka connection settings (can be overridden with environment variables)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events_topic")

# User keys for partitioning
USER_KEYS = ["user_1", "user_2", "user_3"]

# Producer settings
PRODUCER_INTERVAL_SECONDS = 5
CONSUMER_GROUP = "events-consumer-group"
