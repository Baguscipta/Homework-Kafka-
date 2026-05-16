# SUMMARY OF CHANGES

Perubahan yang dilakukan untuk menyesuaikan project dengan requirements PDF:

## 1. Configuration (config.py)
✅ **CHANGED:**
- Topic diubah dari "test-topic" → "events_topic"
- Ditambah USER_KEYS = ["user_1", "user_2", "user_3"]
- Ditambah PRODUCER_INTERVAL_SECONDS = 5
- Ditambah CONSUMER_GROUP = "events-consumer-group"

## 2. Producer (producer.py)
✅ **CHANGED:**
- Added key-based partitioning (menggunakan key untuk partition determination)
- Added 5-second interval between messages (sleep PRODUCER_INTERVAL_SECONDS)
- Added key_serializer untuk serialize key sebagai string
- Message dikirim dengan: `producer.send(topic, key=key, value=msg)`
- Added random key selection dari USER_KEYS
- Added detailed logging untuk show key, partition info
- Added new function: `send_messages_forever()` untuk infinite message sending

## 3. Consumer (consumer.py)
✅ **CHANGED:**
- Added parameter "consumer_id" untuk distinguish multiple consumers
- Added event processing dengan statistics tracking:
  - event_stats: count events per type
  - user_stats: count events per user
  - partition_stats: count messages per partition
- Added detailed message display dengan partition & offset info
- Display statistics saat consumer dihentikan
- Using CONSUMER_GROUP dari config

## 4. Sample Data (sample_data/messages.json)
✅ **CHANGED:**
- Reformatted untuk include required fields:
  - user_id: untuk track events per user (user_1, user_2, user_3)
  - event_type: untuk categorize events
  - details: tambahan event information
- Total 5 sample messages (mix dari different users)

## 5. Entry Point (entrypoint.sh)
✅ **CHANGED:**
- Added support untuk RUN=producer (dengan improved error handling)
- Added support untuk RUN=consumer-multiple (run 2 consumers dalam 1 group)
- Default mode: single consumer
- Added better error handling dan messages

## 6. Multiple Consumers Script (run_multiple_consumers.py)
✅ **NEW FILE CREATED:**
- Script untuk menjalankan 2 consumers dalam satu consumer group
- Uses threading untuk run multiple consumers secara parallel
- Demonstrates partition assignment dan rebalancing
- Each consumer shows assigned partitions, offset, statistics

## 7. Partitioning Report (REPORT_PARTITIONING.md)
✅ **NEW FILE CREATED:**
- Penjelasan mekanisme key-based partitioning
- Formula: partition = hash(key) % number_of_partitions
- Keuntungan: ordering, co-location, efficient processing
- Contoh scenario dengan 3 keys dan 3 partitions
- Kode implementation
- Observasi dan kesimpulan

## 8. Consumer Group Report (REPORT_CONSUMER_GROUP.md)
✅ **NEW FILE CREATED:**
- Penjelasan consumer group concept
- Partition assignment dan rebalancing mechanism
- Behavior dengan 2 consumers dalam 1 group
- Keuntungan: scalability, fault tolerance, load balancing
- Statefulness dan offset management
- Detailed observation dari experiment
- Cara menjalankan experiment
- Kesimpulan tentang scalability & reliability

## 9. README.md
✅ **CHANGED:**
- Updated untuk reflect new implementation
- Added clear instructions untuk menjalankan producer
- Added clear instructions untuk menjalankan single consumer
- Added clear instructions untuk menjalankan multiple consumers dalam 1 group
- Added explanation tentang key-based partitioning
- Added explanation tentang consumer group behavior
- Reference ke report files
- Updated environment variables section
- Added troubleshooting guide

## Key Implementation Details:

### Producer Flow:
```
1. Load sample_data/messages.json
2. Untuk setiap message:
   - Select random key dari [user_1, user_2, user_3]
   - Send dengan key → Kafka determine partition
   - Wait 5 seconds before next message
   - Log: partition assignment untuk setiap key
3. Close producer
```

### Consumer Flow:
```
1. Join consumer group "events-consumer-group"
2. Kafka assign partitions exclusively ke consumer ini
3. Untuk setiap message:
   - Extract user_id, event_type dari value
   - Extract key, partition, offset dari record metadata
   - Update statistics
   - Display detailed info
4. Saat dihentikan:
   - Print final statistics
   - Show partition assignment
   - Close consumer
```

### Multiple Consumers Flow:
```
1. Consumer-1 start terlebih dahulu
   - Claim semua partitions
   - Mulai consuming
2. Consumer-2 join setelah 1 detik
   - Kafka trigger REBALANCING
   - Reassign partitions antar consumer-1 & consumer-2
   - Consumer-2 mulai consuming dari assigned partitions
3. Messages didistribusikan antar consumers
   - No message duplication
   - Parallel processing
```

## Assignment Requirements Coverage:

✅ **Requirement 1: Producer dengan Key-Based Partitioning**
- Topic: events_topic ✓
- Use key untuk partition ✓
- Keys: user_1, user_2, user_3 ✓
- Send every 5 detik ✓
- JSON payload ✓
- Report tentang partitioning ✓ (REPORT_PARTITIONING.md)

✅ **Requirement 2: Consumer**
- Receive dari events_topic ✓
- Processing events (count per user, per event type) ✓
- Report tentang partitioning ✓ (included di REPORT_PARTITIONING.md)

✅ **Requirement 3: Consumer Group**
- Minimal 2 consumers dalam 1 group ✓ (run_multiple_consumers.py)
- Observasi tentang behavior ✓ (REPORT_CONSUMER_GROUP.md)
- Report tentang data distribution ✓ (REPORT_CONSUMER_GROUP.md)

## Testing:

Untuk memverify implementation:

1. **Test Producer dengan Partitioning:**
   ```bash
   docker compose up --build
   # Output akan show: "key='user_1'" atau "key='user_2'" dll
   ```

2. **Test Consumer Processing:**
   ```bash
   docker compose run app sh -c "export RUN=consumer && /app/entrypoint.sh"
   # Output akan show partition info, events per user statistics
   ```

3. **Test Consumer Group:**
   ```bash
   # Terminal 1:
   docker compose run app sh -c "export RUN=producer && /app/entrypoint.sh"
   
   # Terminal 2:
   docker compose run app python run_multiple_consumers.py
   # Output akan show 2 consumers dengan assigned partitions berbeda
   ```

## Files Modified/Created:

Modified:
- kafka_project/config.py
- kafka_project/producer.py
- kafka_project/consumer.py
- sample_data/messages.json
- entrypoint.sh
- README.md

Created:
- run_multiple_consumers.py
- REPORT_PARTITIONING.md
- REPORT_CONSUMER_GROUP.md
- SUMMARY_OF_CHANGES.md (this file)
