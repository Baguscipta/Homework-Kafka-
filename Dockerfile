FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_project/ ./kafka_project/
COPY sample_data/ ./sample_data/
COPY entrypoint.sh ./

RUN chmod +x ./entrypoint.sh

ENV PYTHONPATH=/app

ENTRYPOINT ["/app/entrypoint.sh"]
