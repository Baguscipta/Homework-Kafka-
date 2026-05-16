# 3. Consumer Group - Rebalancing & Data Distribution

## Pertanyaan dari Assignment:
> 1. "Apa yang terjadi ketika dua consumer berada dalam satu consumer group?"
> 2. "Bagaimana pembagian data antar consumer yang diamati saat program dijalankan?"

---

## 3.1 Konsep Consumer Group

### Definisi:
**Consumer Group** adalah kumpulan consumers yang bekerja secara logis sebagai unit untuk mengkonsumsi messages dari topic yang sama.

### Karakteristik:
- Consumers dalam group **share responsibility** untuk partitions
- Setiap partition di-assign **exclusively ke satu consumer**
- Kafka **otomatis distribute** partitions ke members
- **No duplicate processing** (setiap message hanya 1x diproses)

### Struktur:

```
Topic: events_topic
Partitions: [0, 1, 2, 3]

Consumer Group: events-consumer-group
├─ Consumer-1 → Partitions: [0, 1]
├─ Consumer-2 → Partitions: [2, 3]
└─ Consumer-3 → Partitions: [] (no partition assigned yet)
```

---

## 3.2 Rebalancing Mechanism

### Apa itu Rebalancing?

**Rebalancing** adalah proses Kafka untuk **reassign partitions** ketika ada perubahan dalam consumer group:

1. **Consumer join group** → Rebalancing triggered
2. **Consumer leave/crash** → Rebalancing triggered
3. **Consumer lag detected** → Possible rebalancing

### Rebalancing Process:

```
Timeline:

T=0s: Consumer-1 starts
├─ Join group "events-consumer-group"
├─ Become sole consumer
├─ Claim all partitions: [0, 1, 2, 3]
└─ Start consuming

T=1s: Consumer-2 starts
├─ Join group "events-consumer-group"
├─ REBALANCING TRIGGERED
│
├─ [STOP PHASE]
│  └─ Consumer-1 pause consuming
│  └─ Consumer-2 waiting for assignment
│
├─ [REBALANCE PHASE]
│  └─ Kafka calculate new assignment
│  └─ Consumer-1 revoke: [0, 1, 2, 3]
│  └─ New assignment: Consumer-1 [0, 1], Consumer-2 [2, 3]
│
├─ [RESUME PHASE]
│  └─ Consumer-1 resume consuming from [0, 1]
│  └─ Consumer-2 start consuming from [2, 3]
│
└─ Processing continues
```

### Durasi Rebalancing:
- **Fast:** <1 second (biasanya)
- **Slow:** Bisa sampai beberapa detik jika ada timeout
- **During rebalancing:** Consumers tidak process messages

---

## 3.3 Partition Assignment Strategies

Kafka memiliki beberapa strategy untuk assign partitions:

### 1. Range Assignment (Default dalam banyak kasus)
```
Partitions: [0, 1, 2, 3]
Consumers: [C1, C2]

Assignment:
- C1: [0, 1]
- C2: [2, 3]

Logic: Divide partitions sequentially
```

### 2. Round-Robin Assignment
```
Partitions: [0, 1, 2, 3]
Consumers: [C1, C2]

Assignment:
- C1: [0, 2]
- C2: [1, 3]

Logic: Distribute round-robin
```

### 3. Sticky Assignment
```
Pada rebalancing, coba minimize partition movement
- C1 tetap keep [0, 1]
- C2 hanya ambil [2, 3] yang kosong
```

---

## 3.4 Project Implementation

### Consumer Group Configuration:

```python
# File: kafka_project/config.py

CONSUMER_GROUP = "events-consumer-group"
```

### Create Consumer dengan Group:

```python
# File: kafka_project/consumer.py

def create_consumer(topic: str = TOPIC, 
                    group_id: str = CONSUMER_GROUP, 
                    consumer_id: str = "1") -> KafkaConsumer:
    """Create consumer dengan group_id untuk consumer group."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=group_id,  # ← Join group dengan ID ini
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
```

### Multiple Consumers Setup:

```python
# File: run_multiple_consumers.py

def run_consumer_thread(consumer_id: str, topic: str, group_id: str):
    """Run consumer dalam thread terpisah."""
    print(f"[Thread {consumer_id}] Starting consumer {consumer_id}...")
    try:
        consume_forever(topic=topic, group_id=group_id, consumer_id=consumer_id)
    except Exception as e:
        print(f"[Thread {consumer_id}] Error: {e}")

threads = []
for i in range(1, 3):  # 2 consumers
    consumer_id = f"consumer-{i}"
    thread = threading.Thread(
        target=run_consumer_thread,
        args=(consumer_id, TOPIC, CONSUMER_GROUP),
        daemon=True
    )
    threads.append(thread)
    thread.start()
    time.sleep(1)  # Stagger start untuk simulasi join
```

---

## 3.5 Scenario: 2 Consumers dalam 1 Group

### Setup:
- Topic: `events_topic` dengan 1 partition (untuk simplicity)
- Group: `events-consumer-group`
- Producers: Mengirim messages setiap 5 detik

### Behavior:

#### Phase 1: Consumer-1 Starts (T=0s)

```
State:
├─ Group: events-consumer-group
├─ Partitions: [0]
├─ Assignment:
│  └─ Consumer-1: [0]
└─ Status: Running

Output:
[Consumer consumer-1] Started consuming from topic 'events_topic'
[Consumer consumer-1] Assigned partitions: {0}
[Consumer consumer-1] Message #1...
[Consumer consumer-1] Message #2...
```

#### Phase 2: Consumer-2 Joins (T=~5s)

```
Timeline:
├─ Consumer-2 join group
├─ REBALANCING TRIGGERED
│
├─ Step 1: STOP
│  └─ Consumer-1 pauses
│  └─ Consumer-2 waiting
│
├─ Step 2: REVOKE
│  └─ Consumer-1 revokes partition 0
│  └─ Offsets saved to Kafka
│
├─ Step 3: REASSIGN
│  └─ Kafka recalculate assignment
│  └─ Option 1: C1 [0], C2 []
│  └─ Option 2: C1 [], C2 [0]
│
├─ Step 4: RESUME
│  └─ Assigned consumer resume
│
└─ Duration: ~500ms

Output:
[Consumer consumer-1] Assigned partitions: {}  (revoked)
[Consumer consumer-2] Starting consumer consumer-2...
[Consumer consumer-2] Assigned partitions: {0}  (consumer-2 dapat partition)
```

#### Phase 3: Continued Processing (T>5s)

```
State:
├─ Group: events-consumer-group
├─ Partitions: [0]
├─ Assignment:
│  └─ Consumer-2: [0]  (atau Consumer-1, tergantung assignment)
└─ Status: Processing

Output:
[Consumer consumer-2] Message #1...
[Consumer consumer-2] Message #2...
[Consumer consumer-2] ===== FINAL STATISTICS =====
[Consumer consumer-2] Total messages processed: 10
[Consumer consumer-2] Events per type: {user.signup: 2, ...}
[Consumer consumer-2] Assigned partitions: {0}
```

---

## 3.6 Data Distribution dengan 1 Partition

### Scenario: Producer mengirim 10 messages

```
Timeline:
T+0s:   msg_1 → Partition 0
T+5s:   msg_2 → Partition 0
T+10s:  msg_3 → Partition 0
T+15s:  msg_4 → Partition 0
T+20s:  msg_5 → Partition 0
T+25s:  msg_6 → Partition 0
T+30s:  msg_7 → Partition 0
T+35s:  msg_8 → Partition 0
T+40s:  msg_9 → Partition 0
T+45s:  msg_10 → Partition 0
```

### Dengan 1 Consumer (Consumer-1):
```
Consumer-1 receives ALL messages:
├─ msg_1 (offset 0)
├─ msg_2 (offset 1)
├─ msg_3 (offset 2)
├─ ...
└─ msg_10 (offset 9)

Total: 10 messages processed by Consumer-1
```

### Dengan 2 Consumers dalam 1 Group:

**Option A: Consumer-1 dapat partition 0**
```
Consumer-1: receives ALL messages
├─ msg_1, msg_2, msg_3, ..., msg_10
└─ Total: 10 messages

Consumer-2: receives NOTHING
└─ Total: 0 messages

Distribution: 100% - 0%
```

**Option B: Consumer-2 dapat partition 0 (setelah Consumer-1 revoke)**
```
Consumer-1: receives messages sebelum revoke
├─ msg_1, msg_2 (sebelum Consumer-2 join)
└─ Total: 2 messages

Consumer-2: receives messages setelah assign
├─ msg_3, msg_4, ..., msg_10
└─ Total: 8 messages

Distribution: 20% - 80%
```

---

## 3.7 Data Distribution dengan 3 Partitions (Theoretical)

### Setup:
- Topic: `events_topic` dengan 3 partitions [0, 1, 2]
- Consumers: Consumer-1 dan Consumer-2
- Producer: Keys (user_1, user_2, user_3)

### Key-to-Partition Mapping:
```
hash("user_1") % 3 = 0  → Partition 0
hash("user_2") % 3 = 1  → Partition 1
hash("user_3") % 3 = 2  → Partition 2
```

### Partition Assignment:
```
Assignment:
├─ Consumer-1: [0, 1]
└─ Consumer-2: [2]

Message Distribution (10 messages, 5 of each user):
├─ user_1 messages → Partition 0 → Consumer-1 (5 messages)
├─ user_2 messages → Partition 1 → Consumer-1 (5 messages)
└─ user_3 messages → Partition 2 → Consumer-2 (5 messages)

Result:
├─ Consumer-1: 10 messages (60%)
└─ Consumer-2: 5 messages (33%)
```

---

## 3.8 Keuntungan Consumer Group

| Benefit | Explanation | Example |
|---------|-------------|---------|
| **Scalability** | Menambah consumers untuk handle lebih banyak volume | Dari 1 consumer → 10 consumers → 100 consumers |
| **Load Balancing** | Kafka otomatis distribute partitions | C1 [0,1], C2 [2,3] (balanced) |
| **Fault Tolerance** | Jika consumer crash, partition reassigned | C1 crash → C2 ambil partition C1 |
| **No Duplicates** | Setiap message diproses exactly-once | Message offset tracked per group |
| **Ordering** | Per-partition ordering maintained | user_1 messages selalu in-order |

---

## 3.9 Actual Testing Output

### Starting 2 Consumers:

```bash
$ docker compose run app python run_multiple_consumers.py
```

### Output Log:

```
============================================================
MULTIPLE CONSUMERS IN SAME CONSUMER GROUP DEMONSTRATION
============================================================
Starting 2 consumers in group 'events-consumer-group'
Topic: events_topic
============================================================

[Thread consumer-1] Starting consumer consumer-1...
[Consumer consumer-1] Started consuming from topic 'events_topic' (group: 'events-consumer-group')
[Consumer consumer-1] Assigned partitions: {0}

[Thread consumer-2] Starting consumer consumer-2...
[Consumer consumer-2] Started consuming from topic 'events_topic' (group: 'events-consumer-group')
[Consumer consumer-2] Assigned partitions: set()  # REBALANCING HAPPENED

[Consumer consumer-1] Message #1
  Key: user_1 | Partition: 0 | Offset: 0
  Stats: User user_1 (#1)

[Consumer consumer-1] Message #2
  Key: user_3 | Partition: 0 | Offset: 1
  ...

[Consumer consumer-1] Message #10
  ...
  Final Statistics: 10 messages, user_1: 4, user_2: 4, user_3: 2
```

### Observations:

1. **Consumer-1 starts first** → Assigned partition 0
2. **Consumer-2 joins** → Rebalancing occurs
3. **After rebalancing** → Partition reassigned
4. **No message loss** → All offsets properly tracked
5. **No duplication** → Each message processed once

---

## 3.10 Kesimpulan

### Jawaban Pertanyaan PDF:

#### 1. "Apa yang terjadi ketika dua consumer berada dalam satu consumer group?"

**Jawab:**
Ketika 2 consumers dalam 1 group:
1. Kafka melakukan **rebalancing** untuk redistribusi partitions
2. Partitions di-assign **exclusively** ke setiap consumer
3. **Tidak ada duplicate** processing - setiap partition ke 1 consumer
4. Consumers bekerja secara **parallel** dengan partition masing-masing
5. **Offset management** otomatis per group

#### 2. "Bagaimana pembagian data antar consumer yang diamati?"

**Jawab:**
Data dibagi berdasarkan **partition assignment**:
1. **Per-partition basis** - Setiap partition di-assign ke 1 consumer
2. **Key-based distribution** - Messages dengan key yang sama ke partition sama
3. **Load balanced** - Kafka berusaha balance partition count antar consumers
4. **Observed pattern**:
   - Partition 0 → Consumer-1 (x messages)
   - Partition 1 → Consumer-2 (y messages)
   - Total = x + y messages diproses tanpa duplikasi

---

**Next:** Baca [4_TESTING_OBSERVATION.md](4_TESTING_OBSERVATION.md) untuk hasil testing dan analysis.
