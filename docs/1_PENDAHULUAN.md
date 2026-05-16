# Pendahuluan - Kafka Assignment Report

## Tujuan Project
Project ini mengimplementasikan kafka assignment dengan fokus pada:

1. **Producer dengan Key-Based Partitioning**
   - Mengirim data ke topic `events_topic`
   - Menggunakan key untuk menentukan partition
   - Keys yang digunakan: `user_1`, `user_2`, `user_3`
   - Interval pengiriman: setiap 5 detik

2. **Consumer dengan Event Processing**
   - Menerima events dari topic `events_topic`
   - Melakukan processing sederhana (menghitung jumlah data per user)
   - Menampilkan statistik real-time

3. **Consumer Group**
   - Menjalankan minimal 2 consumers dalam 1 consumer group
   - Mengobservasi behavior rebalancing
   - Menganalisis data distribution

---

## Arsitektur System

### Stack Technology:
- **Kafka**: Message broker (dalam Docker)
- **Zookeeper**: Coordinator untuk Kafka
- **Python**: Producer dan Consumer
- **Docker Compose**: Orchestration

### Komponen Utama:

```
┌─────────────────────────────────────────────┐
│           Producer                          │
│  (Sends 5 messages setiap 5 detik)         │
│  Keys: user_1, user_2, user_3              │
└────────────────┬────────────────────────────┘
                 │
                 ▼
        ┌────────────────┐
        │ Kafka Broker   │
        │ events_topic   │
        │ (1 partition)  │
        └────────────────┘
        /      |        \
       /       |         \
      ▼        ▼          ▼
   Consumer Consumer  Multiple
      1        2      Consumers
   (Single) (Single)  (in group)
```

---

## Data Model

### Sample Messages (messages.json):
```json
{
  "id": 1,
  "user_id": "user_1",           // Key untuk partitioning
  "event_type": "user.signup",   // Tipe event
  "details": {"name": "Alice"}   // Detail tambahan
}
```

### Field Penting:
- `user_id`: Identifier untuk tracking per user
- `event_type`: Kategori event (user.signup, user.login, order.created, dll)
- `details`: Info tambahan yang relevan

---

## Configuration

### Kafka Configuration (config.py):
```python
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events_topic"
USER_KEYS = ["user_1", "user_2", "user_3"]
PRODUCER_INTERVAL_SECONDS = 5
CONSUMER_GROUP = "events-consumer-group"
```

### Docker Compose Setup:
- **Zookeeper**: Port 2181 (internal)
- **Kafka**: Port 9092 (exposed)
- **App**: Python container untuk producer/consumer

---

## Dokumen Lanjutan

Laporan ini terdiri dari:

1. **[Pendahuluan](1_PENDAHULUAN.md)** (dokumen ini)
   - Overview project, arsitektur, data model
   
2. **[Key-Based Partitioning](2_KEY_BASED_PARTITIONING.md)**
   - Penjelasan cara Kafka menentukan partition dari key
   - Formula hash dan contoh scenario
   - Implementasi di project
   
3. **[Consumer Group & Rebalancing](3_CONSUMER_GROUP.md)**
   - Behavior dengan 2+ consumers dalam 1 group
   - Partition assignment strategy
   - Data distribution & load balancing
   
4. **[Testing & Observation](4_TESTING_OBSERVATION.md)**
   - Hasil testing
   - Output dan analysis
   - Kesimpulan

---

## Quick Summary

| Aspek | Status | Keterangan |
|-------|--------|-----------|
| Producer | ✅ | Mengirim 5 messages setiap 5 detik dengan key-based partitioning |
| Topic | ✅ | `events_topic` dengan 1 partition |
| Keys | ✅ | user_1, user_2, user_3 (random assignment per message) |
| Consumer | ✅ | Menerima messages dan menghitung statistics per user |
| Consumer Group | ✅ | 2 consumers dalam group yang sama, auto partition assignment |
| Processing | ✅ | Count per user, count per event type, partition tracking |

---

## Cara Menjalankan

### 1. Single Consumer:
```bash
docker compose run app sh -c "export RUN=consumer && /app/entrypoint.sh"
```

### 2. Multiple Consumers:
```bash
docker compose run app python run_multiple_consumers.py
```

### 3. Producer:
```bash
docker compose up --build
```

---

**Next:** Baca [2_KEY_BASED_PARTITIONING.md](2_KEY_BASED_PARTITIONING.md) untuk penjelasan detail tentang partitioning mechanism.
