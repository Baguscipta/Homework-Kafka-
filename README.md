# Homework-Kafka-

Project Kafka sederhana dengan Python yang mengimplementasikan requirements assignment:
1. **Producer dengan Key-Based Partitioning** - mengirim message dengan keys (user_1, user_2, user_3) setiap 5 detik
2. **Consumer dengan Event Processing** - menerima dan memproses events, menghitung statistics per user
3. **Consumer Group** - menjalankan 2+ consumers dalam 1 group untuk demonstrasi distribution

## Struktur File Penting

```
kafka_project/
  ├── config.py          - Kafka configuration & constants (TOPIC, USER_KEYS, CONSUMER_GROUP)
  ├── producer.py        - Producer dengan key-based partitioning
  ├── consumer.py        - Consumer dengan event processing
  └── utils.py           - Utility functions
sample_data/
  └── messages.json      - Sample messages dengan user_id dan event_type
run_multiple_consumers.py - Script untuk menjalankan 2 consumers dalam 1 group
REPORT_PARTITIONING.md   - Report tentang key-based partitioning
REPORT_CONSUMER_GROUP.md - Report tentang consumer group behavior
```

## Quick Start

### 1. Jalankan Kafka Stack + Single Producer

```bash
docker compose up --build
```

Output akan menunjukkan producer mengirim messages dengan key-based partitioning setiap 5 detik.

### 2. Jalankan Consumer (di terminal terpisah)

```bash
docker compose run app sh -c "export RUN=consumer && /app/entrypoint.sh"
```

Consumer akan:
- Menampilkan messages yang diterima dengan partition & offset info
- Menghitung events per user dan per event type
- Menampilkan statistics saat dihentikan

### 3. Jalankan Multiple Consumers dalam 1 Consumer Group

**Terminal 1 - Producer:**
```bash
docker compose run app sh -c "export RUN=producer && /app/entrypoint.sh"
```

**Terminal 2 - Multiple Consumers (di machine lain atau terminal terpisah):**
```bash
docker compose run app python run_multiple_consumers.py
```

Expected Output:
- Consumer-1 dan Consumer-2 akan join group `events-consumer-group`
- Kafka akan melakukan rebalancing dan assign partitions
- Messages akan didistribusikan antar consumers
- Setiap messages diproses exactly-once (no duplicates)

## Menjalankan Lokal Tanpa Docker

### Setup

```bash
# Create virtualenv
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt
```

### Jalankan Producer

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=events_topic \
python -c "from kafka_project.utils import load_messages; from kafka_project.producer import send_messages; send_messages(load_messages('sample_data/messages.json'))"
```

### Jalankan Single Consumer

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=events_topic \
python -c "from kafka_project.consumer import consume_forever; consume_forever()"
```

### Jalankan Multiple Consumers

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=events_topic \
python run_multiple_consumers.py
```

## Understanding Key-Based Partitioning

Producer menggunakan 3 keys: `user_1`, `user_2`, `user_3`

```python
key = random.choice(USER_KEYS)  # "user_1", "user_2", atau "user_3"
producer.send(topic, key=key, value=msg)
```

Kafka menentukan partition dengan formula:
```
partition = hash(key) % number_of_partitions
```

**Hasil:**
- Semua message dengan key=`user_1` → always same partition
- Semua message dengan key=`user_2` → always same partition
- Semua message dengan key=`user_3` → always same partition

Ini memastikan **ordering** dan **co-location** data per user.

Lihat `REPORT_PARTITIONING.md` untuk penjelasan detail.

## Understanding Consumer Group

Ketika 2+ consumers dalam satu group:

1. **Rebalancing**: Kafka assign partitions exclusively ke each consumer
2. **Parallel Processing**: Consumers process messages secara parallel
3. **No Duplicates**: Setiap message diproses exactly-once
4. **Fault Tolerance**: Jika consumer crash, consumer lain ambil alih

Contoh dengan 3 partitions + 2 consumers:
```
Consumer-1: partition_0, partition_1
Consumer-2: partition_2
```

Output akan menunjukkan:
- Consumer-1 menerima ~67% messages
- Consumer-2 menerima ~33% messages
- Total 100% messages processed (no loss, no duplication)

Lihat `REPORT_CONSUMER_GROUP.md` untuk penjelasan detail dan observasi.

## Menjalankan Tests

```bash
pytest -v
```

## Docker Compose Services

- **zookeeper**: Coordination service untuk Kafka
- **kafka**: Kafka broker di port 9092
- **app**: Python application (producer/consumer)

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string (default: "localhost:9092")
- `KAFKA_TOPIC`: Topic name (default: "events_topic")
- `RUN`: Mode eksekusi ("producer" atau "consumer", default: "consumer")

## Implementation Details

### Producer (producer.py)

- Membaca messages dari `sample_data/messages.json`
- Setiap message dikirim dengan random key dari [user_1, user_2, user_3]
- Messages dikirim setiap 5 detik (PRODUCER_INTERVAL_SECONDS)
- Key digunakan untuk key-based partitioning

### Consumer (consumer.py)

- Joins consumer group: `events-consumer-group`
- Auto offset reset: `earliest` (start dari beginning)
- Tracks statistics:
  - Total messages processed
  - Events per type
  - Events per user
  - Messages per partition
  - Assigned partitions untuk consumer instance ini

### Konfigurasi (config.py)

```python
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events_topic"
USER_KEYS = ["user_1", "user_2", "user_3"]
PRODUCER_INTERVAL_SECONDS = 5
CONSUMER_GROUP = "events-consumer-group"
```

## Troubleshooting

### Error: "Connection refused"
- Pastikan Kafka running: `docker compose ps`
- Jika offline, jalankan: `docker compose up -d`

### Messages tidak diterima consumer?
- Check topic name matches: `events_topic` (di config.py)
- Check consumer group: `events-consumer-group`
- Consumer automatically seeks to earliest offset jika group baru

### Duplicate messages?
- Normal untuk multiple consumers yang different groups
- Untuk 1 group: Kafka menjamin no duplicates (exactly-once per partition)

## Pertanyaan dan Pengembangan

Untuk pertanyaan atau request fitur (multi-topic, schema registry, security), silakan buat issue.


