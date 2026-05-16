#!/bin/sh
set -e

if [ "${RUN}" = "producer" ]; then
  python - <<'PY'
from kafka_project.utils import load_messages
from kafka_project.producer import send_messages
msgs = load_messages('sample_data/messages.json')
send_messages(msgs)
PY
else
  python - <<'PY'
from kafka_project.consumer import consume_forever
consume_forever()
PY
fi
