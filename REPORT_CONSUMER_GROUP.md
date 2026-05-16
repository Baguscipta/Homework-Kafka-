# REPORT: Consumer Group dan Distribution Data

## Penjelasan Consumer Group di Kafka

### 1. Konsep Consumer Group

Consumer Group adalah sekelompok consumer yang bertindak sebagai logical unit untuk mengkonsumsi message dari topic. Kafka akan mendistribusikan partitions antar members dalam group.

### 2. Mekanisme Partition Assignment:

Ketika multiple consumer dalam satu group:

```
Consumer Group = {consumer_1, consumer_2, ...}
Topic partitions = {partition_0, partition_1, partition_2, ...}

Kafka Assignment:
- consumer_1 → partition_0
- consumer_2 → partition_1
- consumer_3 → partition_2
(dsb)
```

### 3. Behavior dengan 2 Consumers dalam 1 Group:

#### Skenario Setup:
- Topic: `events_topic` dengan 3 partitions (partition_0, partition_1, partition_2)
- Consumer Group: `events-consumer-group`
- Jumlah Consumers: 2

#### Outcome:
```
Partition Assignment:
- Consumer-1: partition_0, partition_1
- Consumer-2: partition_2

atau

- Consumer-1: partition_0
- Consumer-2: partition_1, partition_2

(Tergantung Kafka assignment strategy)
```

### 4. Apa yang Terjadi:

#### A. Rebalancing:
Ketika consumer baru join atau consumer lama leave, Kafka melakukan **rebalancing**:
1. Pause semua consumer
2. Revoke partitions dari consumer yang ada
3. Reassign partitions ke semua consumers
4. Resume consuming

#### B. Pembagian Kerja:

Dengan 3 partitions dan 2 consumers:
- Consumer-1 akan memproses ~50% message (dari assigned partitions)
- Consumer-2 akan memproses ~50% message (dari assigned partitions)
- Tidak ada message yang diproses 2x (exclusive partition ownership)

#### C. Parallel Processing:

```
Producer sends to events_topic with keys (user_1, user_2, user_3)

Distribution:
- user_1 messages → partition_0 → Consumer-1
- user_2 messages → partition_1 → Consumer-1 atau Consumer-2 (depends on assignment)
- user_3 messages → partition_2 → Consumer-2

Result: Consumer-1 dan Consumer-2 process messages in parallel
```

### 5. Keuntungan Consumer Group:

1. **Scalability**: Menambah consumer untuk meningkatkan throughput
2. **Fault Tolerance**: Jika satu consumer crash, consumer lain ambil alih partitions-nya
3. **Load Balancing**: Kafka otomatis mendistribusikan load antar consumers
4. **No Duplicate**: Setiap message hanya diproses sekali (exclusive partition ownership)

### 6. Statefulness dalam Consumer Group:

Setiap consumer group memiliki state tersendiri:
```
Consumer Group: events-consumer-group
  partition_0: offset 100
  partition_1: offset 95
  partition_2: offset 98
```

- Offset disimpan di Kafka (default: `__consumer_offsets` topic)
- Ketika consumer bergerak, partitions tetap track offsetnya
- Memungkinkan recovery: restart consumer dari last position

### 7. Observasi dari Experiment:

Ketika menjalankan `run_multiple_consumers.py`:

1. **Consumer-1 mulai terlebih dahulu**:
   - Claim semua 3 partitions
   - Mulai consuming dari partition_0, partition_1, partition_2

2. **Consumer-2 join** (setelah 1 detik):
   - **Rebalancing terjadi**:
     - Consumer-1 revoke partitions
     - Kafka reassign partitions
     - Misal: Consumer-1 → partition_0, Consumer-2 → partition_1, partition_2
   - Consumer-2 mulai consuming dari assigned partitions
   - Log akan menunjukkan: "Assigned partitions: {partition_0}" untuk Consumer-1

3. **Message Distribution**:
   - Jika Producer mengirim 6 messages (2 per key) setiap round
   - Consumer-1 menerima messages dari partition yang di-assign-nya
   - Consumer-2 menerima messages dari partition yang di-assign-nya
   - Total 6 messages diproses (tidak ada duplicate)

### 8. Kode yang Demonstrasikan:

File: `run_multiple_consumers.py`
```python
for i in range(1, num_consumers + 1):
    consumer_id = f"consumer-{i}"
    thread = threading.Thread(
        target=run_consumer_thread,
        args=(consumer_id, topic, group_id),
        daemon=True
    )
```

Setiap consumer akan:
1. Join group `events-consumer-group`
2. Trigger rebalancing
3. Assign partitions sesuai strategy
4. Consume dan print statistics

### 9. Cara Menjalankan Experiment:

```bash
# Terminal 1: Start producer (sends 5 messages, setiap 5 detik)
python -m kafka_project.producer

# Terminal 2: Run 2 consumers dalam 1 group
python run_multiple_consumers.py
```

Expected Output:
- Consumer-1 dan Consumer-2 masing-masing menampilkan assigned partitions
- Messages didistribusikan antara mereka
- Statistics menunjukkan partition assignment dan events per consumer

### 10. Kesimpulan:

Consumer group adalah fitur kunci Kafka untuk:
- **Scalability**: Menangani high-volume events dengan multiple consumers
- **Reliability**: Auto-failover jika consumer gagal
- **Efficiency**: Parallel processing dengan no data loss atau duplication
- **Coordination**: Kafka handles partition management otomatis

Dengan 2 consumers dalam 1 group:
- Data didistribusikan secara ekslusif (no overlaps)
- Processing terjadi parallel
- Jika consumer crash, consumer lain ambil alih
- System bisa scale up dengan menambah lebih banyak consumers
