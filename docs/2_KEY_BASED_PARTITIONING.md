# 2. Key-Based Partitioning - Mekanisme dan Implementasi

## Pertanyaan dari Assignment:
> "Bagaimana Kafka menentukan ke partition mana sebuah event dikirim berdasarkan key?"

---

## 2.1 Konsep Dasar

### Apa itu Key?
**Key** adalah identifier tambahan yang dikirim bersama message ke Kafka. Key digunakan untuk:

1. **Menentukan Partition** - Message dengan key yang sama akan selalu ke partition yang sama
2. **Ordering** - Menjamin urutan processing untuk messages dengan key yang sama
3. **Co-location** - Data dari user/entity yang sama berkumpul di partition yang sama

### Tanpa Key:
```
Message 1 → Partition 0
Message 2 → Partition 1
Message 3 → Partition 2
(Random distribution)
```

### Dengan Key:
```
Message 1 (key=user_1) → Partition 0 (always)
Message 2 (key=user_1) → Partition 0 (always)
Message 3 (key=user_2) → Partition 1 (always)
Message 4 (key=user_2) → Partition 1 (always)
(Consistent, deterministic)
```

---

## 2.2 Formula Partition Determination

### Algoritma:

$$\text{partition} = \text{hash(key)} \bmod \text{number\_of\_partitions}$$

Dimana:
- `hash(key)`: Fungsi hash untuk mengkonversi key menjadi integer
- `number_of_partitions`: Total partition dalam topic
- `mod`: Operasi modulo untuk mapping ke range partition

### Contoh Calculation:

Misalkan topic `events_topic` memiliki 3 partitions (0, 1, 2):

```
hash("user_1") = 42156
partition = 42156 % 3 = 0  → Partition 0

hash("user_2") = 78923
partition = 78923 % 3 = 1  → Partition 1

hash("user_3") = 15634
partition = 15634 % 3 = 1  → Partition 1

hash("user_1") = 42156  (konsisten)
partition = 42156 % 3 = 0  → Partition 0 (selalu sama)
```

### Konsistensi Hash:
- **Key yang sama → Partition yang sama** (selalu konsisten)
- **Hash function deterministic** → Hasil hash selalu sama untuk key yang sama
- **Modulo operation** → Mapping konsisten ke partition range

---

## 2.3 Implementasi di Project

### Producer Code:

```python
# File: kafka_project/producer.py

def send_messages(messages: List[Any], topic: str = TOPIC) -> None:
    """Send messages dengan key-based partitioning."""
    producer = create_producer()
    try:
        for i, msg in enumerate(messages):
            # Step 1: Select random key
            key = random.choice(USER_KEYS)  # user_1, user_2, atau user_3
            
            # Step 2: Send dengan key
            producer.send(topic, key=key, value=msg)
            
            # Step 3: Wait sebelum send next message
            time.sleep(PRODUCER_INTERVAL_SECONDS)
    finally:
        producer.close()
```

### Key Serialization:

```python
def _string_serializer(data: str) -> bytes:
    """Serialize key sebagai string."""
    return data.encode("utf-8")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=_string_serializer,      # ← Key serializer
        value_serializer=_json_serializer
    )
```

### Configuration:

```python
# File: kafka_project/config.py

USER_KEYS = ["user_1", "user_2", "user_3"]
TOPIC = "events_topic"
PRODUCER_INTERVAL_SECONDS = 5
```

---

## 2.4 Scenario & Example

### Setup:
- Topic: `events_topic`
- Partitions: 1 (hanya untuk demo, biasanya lebih banyak)
- Keys: user_1, user_2, user_3
- Producer: mengirim 5 messages setiap 5 detik

### Message Flow:

| Waktu | Message | Key | Hash | Partition | Dikirim ke |
|-------|---------|-----|------|-----------|-----------|
| T+0s  | msg_1   | user_1 | 42156 | 42156%1=0 | Partition 0 |
| T+5s  | msg_2   | user_3 | 15634 | 15634%1=0 | Partition 0 |
| T+10s | msg_3   | user_3 | 15634 | 15634%1=0 | Partition 0 |
| T+15s | msg_4   | user_2 | 78923 | 78923%1=0 | Partition 0 |
| T+20s | msg_5   | user_1 | 42156 | 42156%1=0 | Partition 0 |

**Observasi:**
- Semua messages ke partition 0 (karena hanya 1 partition)
- Key "user_1" → hash 42156 → selalu partition 0
- Key "user_2" → hash 78923 → selalu partition 0
- Key "user_3" → hash 15634 → selalu partition 0
- **Konsistensi:** Messages dengan key yang sama selalu dari partition yang sama

### Dengan 3 Partitions (Theoretical):

| Key | Hash | Partition (mod 3) |
|-----|------|------------------|
| user_1 | 42156 | 42156 % 3 = 0 |
| user_2 | 78923 | 78923 % 3 = 1 |
| user_3 | 15634 | 15634 % 3 = 1 |

Hasilnya:
```
Partition 0: user_1 messages (all user_1)
Partition 1: user_2 + user_3 messages
Partition 2: (empty)
```

---

## 2.5 Keuntungan Key-Based Partitioning

### 1. **Ordering Terjamin**
```
Consumer menerima:
msg_1 (user_1) → Offset 0
msg_2 (user_1) → Offset 1
msg_3 (user_1) → Offset 2

Order selalu sama karena dari partition yang sama
```

### 2. **Co-location of Data**
```
Semua data user_1 → Partition yang sama
Efisien untuk processing yang memerlukan user history
```

### 3. **Efficient State Management**
```
Jika ada stateful processing:
- Consumer A track user_1 state
- Consumer B track user_2 state
- Tidak perlu koordinasi lintas partition
```

### 4. **No Data Duplication**
```
Message dengan key tertentu hanya di 1 partition
Tidak ada duplikasi untuk processing yang sama
```

---

## 2.6 Actual Testing Output

### Terminal Output dari Testing:

```
[Producer] Sending message 1/5 with key='user_1' to topic 'events_topic'
[Producer] Sending message 2/5 with key='user_3' to topic 'events_topic'
[Producer] Sending message 3/5 with key='user_3' to topic 'events_topic'
[Producer] Sending message 4/5 with key='user_2' to topic 'events_topic'
[Producer] Sending message 5/5 with key='user_1' to topic 'events_topic'
[Producer] All messages sent successfully
```

### Consumer Output:

```
[Consumer 1] Message #1
  Key: user_1 | Partition: 0 | Offset: 0
  User: user_1 | Event: user.signup

[Consumer 1] Message #2
  Key: user_3 | Partition: 0 | Offset: 1
  User: user_2 | Event: user.login

[Consumer 1] Message #3
  Key: user_3 | Partition: 0 | Offset: 2
  User: user_3 | Event: order.created
```

**Observasi:**
- ✅ Key visible dalam message
- ✅ Partition 0 untuk semua (karena 1 partition)
- ✅ Offset sequential (0, 1, 2, ...)
- ✅ Messages dengan key yang sama in-order

---

## 2.7 Partitioning Strategy

### Default Partitioner (Kafka):
```
DefaultPartitioner:
  - Jika key != null → hash(key) % partitions
  - Jika key == null → round-robin antar partitions
```

### Custom Partitioner (optional):
```python
class CustomPartitioner:
    def __call__(self, key, all_partitions, available_partitions):
        if key == "high_priority":
            return all_partitions[0]  # Always first partition
        else:
            return random.choice(available_partitions)
```

---

## 2.8 Kesimpulan

### Key-Based Partitioning Mechanism:

1. **Deterministic:** Key yang sama → Partition yang sama (konsisten)
2. **Formula:** `partition = hash(key) % num_partitions`
3. **Benefits:** Ordering, co-location, efficient processing
4. **Implementation:** Kafka handles otomatis via hash function
5. **Project:** Menggunakan keys (user_1, user_2, user_3) untuk demo

### Pertanyaan PDF Terjawab:
> "Bagaimana Kafka menentukan ke partition mana sebuah event dikirim berdasarkan key?"

**Jawab:** 
Kafka menggunakan hash function pada key, kemudian melakukan modulo dengan jumlah partitions. Hasilnya menentukan partition mana yang menerima message. Proses ini konsisten: key yang sama selalu hash ke partition yang sama.

---

**Next:** Baca [3_CONSUMER_GROUP.md](3_CONSUMER_GROUP.md) untuk penjelasan tentang consumer group behavior.
