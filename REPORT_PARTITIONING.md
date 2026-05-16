# REPORT: Kafka Key-Based Partitioning

## Penjelasan Mekanisme Partitioning Berdasarkan Key

### 1. Konsep Dasar
Kafka menggunakan key untuk menentukan ke partition mana sebuah message akan dikirim. Key ini sangat penting untuk memastikan ordering message dan distribution yang konsisten.

### 2. Algoritma Partitioning
Ketika producer mengirim message dengan key, Kafka menggunakan hash function untuk menentukan partition:

```
partition = hash(key) % number_of_partitions
```

Dimana:
- `hash(key)`: Fungsi hash untuk mengkonversi key menjadi integer
- `number_of_partitions`: Total partition dalam topic

### 3. Implementasi di Project Ini

#### Keys yang Digunakan:
- `user_1`
- `user_2`
- `user_3`

#### Proses:
1. Producer mengirim message dengan salah satu dari 3 keys tersebut
2. Kafka menghitung hash dari key
3. Hash tersebut di-modulo dengan jumlah partition
4. Message dikirim ke partition yang sesuai

### 4. Contoh Scenario:

Misalkan topic `events_topic` memiliki 3 partitions (0, 1, 2):

```
hash("user_1") % 3 → partition 0 (selalu untuk user_1)
hash("user_2") % 3 → partition 1 (selalu untuk user_2)
hash("user_3") % 3 → partition 2 (selalu untuk user_3)
```

### 5. Keuntungan Key-Based Partitioning:

1. **Ordering Terjamin**: Semua message dengan key yang sama akan masuk ke partition yang sama, sehingga ordering terjaga
2. **Co-location Data**: Data dari user yang sama akan berada di partition yang sama
3. **Efficient Processing**: Consumer dapat memproses data per user tanpa interaksi lintas partition

### 6. Kode Producer:

```python
key = random.choice(USER_KEYS)  # "user_1", "user_2", atau "user_3"
producer.send(topic, key=key, value=msg)
```

Ketika message dikirim dengan key, Kafka:
1. Melakukan hash pada key
2. Menghitung partition = hash % num_partitions
3. Mengirim message ke partition tersebut

### 7. Observasi:

Jika kita menjalankan consumer dan melihat output, kita akan melihat:
- Message dengan key "user_1" selalu dari partition yang sama
- Message dengan key "user_2" selalu dari partition yang sama
- Message dengan key "user_3" selalu dari partition yang sama

Ini membuktikan bahwa Kafka menggunakan hash konsisten untuk menentukan partition berdasarkan key.

### 8. Kesimpulan:

Key-based partitioning adalah mekanisme penting dalam Kafka untuk:
- Memastikan ordering message
- Mendistribusikan data secara merata antar partition
- Memungkinkan efficient processing dengan consumer group
