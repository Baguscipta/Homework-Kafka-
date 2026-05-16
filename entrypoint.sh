#!/bin/sh
set -e

if [ "${RUN}" = "producer" ]; then
  echo "Starting Kafka Producer..."
  python - <<'PY'
from kafka_project.utils import load_messages
from kafka_project.producer import send_messages
import sys

try:
    msgs = load_messages('sample_data/messages.json')
    print(f"Loaded {len(msgs)} messages from sample_data/messages.json\n")
    send_messages(msgs)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
PY
elif [ "${RUN}" = "consumer-multiple" ]; then
  echo "Starting Multiple Consumers (2 in 1 group)..."
  python - <<'PY'
from kafka_project.consumer import consume_forever
from kafka_project.config import TOPIC, CONSUMER_GROUP
import threading
import sys

def run_consumer(consumer_id):
    try:
        consume_forever(topic=TOPIC, group_id=CONSUMER_GROUP, consumer_id=consumer_id)
    except Exception as e:
        print(f"Consumer {consumer_id} error: {e}", file=sys.stderr)

threads = []
for i in range(1, 3):
    t = threading.Thread(target=run_consumer, args=(f"consumer-{i}",), daemon=True)
    threads.append(t)
    t.start()

try:
    for t in threads:
        t.join()
except KeyboardInterrupt:
    print("\nShutting down...")
    sys.exit(0)
PY
else
  echo "Starting Single Consumer..."
  python - <<'PY'
from kafka_project.consumer import consume_forever
import sys

try:
    consume_forever()
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
PY
fi
