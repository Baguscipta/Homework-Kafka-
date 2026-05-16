# Homework-Kafka-

Project lengkap contoh aplikasi Kafka sederhana menggunakan Python.

Ringkasan:
- Kode producer/consumer minimal di `kafka_project/`
- Contoh data di `sample_data/messages.json`
- Dockerfile dan `docker-compose.yml` untuk menjalankan Kafka + app
- Unit tests dengan `pytest` di `tests/`
- CI GitHub Actions di `.github/workflows/ci.yml`

Lihat bagian "Quick start" untuk perintah cepat menjalankan aplikasi.

Quick start
-----------

1. Jalankan stack Kafka (Zookeeper + Kafka) dan app via Docker Compose:

```bash
docker compose up --build
```

2. Untuk menjalankan consumer (di terminal terpisah):

```bash
docker compose run --service-ports app sh -c "export RUN=consumer && /app/entrypoint.sh"
```

3. Untuk menjalankan producer yang mengirim sample messages ke topik default:

```bash
docker compose run app
```

Menjalankan lokal tanpa Docker
-----------------------------

1. Buat virtualenv dan instal dependensi:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

2. Jalankan producer (pastikan Kafka reachable di `KAFKA_BOOTSTRAP_SERVERS` env):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RUN=producer .venv/bin/python entrypoint.sh
```

3. Jalankan consumer:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 RUN=consumer .venv/bin/python entrypoint.sh
```

Menjalankan tes
---------------

```bash
.venv/bin/python -m pytest -q
```

Struktur file penting
---------------------
- `kafka_project/` : kode sumber utama (`producer.py`, `consumer.py`, `utils.py`, `config.py`)
- `sample_data/messages.json` : contoh pesan JSON
- `Dockerfile` : image runtime untuk app
- `docker-compose.yml` : stack untuk menjalankan Kafka + app
- `tests/` : tes unit dasar
- `.github/workflows/ci.yml` : workflow CI untuk menjalankan tes

Pertanyaan
---------
Jika ingin saya tambahkan fitur tertentu (topik multi, skema Avro/Schema Registry, atau integrasi ACL/SECURE), beri tahu saya dan saya akan tambahkan.

