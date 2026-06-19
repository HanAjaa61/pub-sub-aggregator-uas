# Pub-Sub Log Aggregator Terdistribusi

**UAS Sistem Terdistribusi | Muhammad Rayhan Saputra | 11231061 | Institut Teknologi Kalimantan**

Sistem Pub-Sub log aggregator multi-service dengan idempotent consumer, strong deduplication, dan transaction/concurrency control — berjalan sepenuhnya via Docker Compose.

---

## Arsitektur

```
Publisher ──(HTTP batch)──▶ Aggregator (FastAPI) ──(RPUSH)──▶ Redis (broker)
                                                                      │
                                              4 async workers (BLPOP)─┘
                                                      │
                                              PostgreSQL (dedup store + stats + audit)
```

### Service

| Service | Image/Base | Port | Keterangan |
|---|---|---|---|
| `aggregator` | python:3.11-slim | 8080 (host) | API + consumer workers |
| `publisher` | python:3.11-slim | — | Event generator, run-once |
| `broker` | redis:7-alpine | internal only | Message queue |
| `storage` | postgres:16-alpine | internal only | Dedup store + stats |

---

## Quickstart

### Prasyarat

- Docker Desktop / Docker Engine ≥ 24
- Docker Compose v2

### Build dan Jalankan

```bash
# Clone / masuk ke direktori proyek
cd pubsub-aggregator

# Build semua image dan jalankan stack
docker compose up --build

# Akses aggregator (terminal lain)
curl http://localhost:8080/health
curl http://localhost:8080/stats
```

Publisher akan otomatis berjalan dan mengirim **20.000 event (35% duplikat)** setelah aggregator siap.

### Jalankan Ulang Publisher

```bash
docker compose run --rm publisher
```

### Stop dan Hapus Container (data tetap aman)

```bash
docker compose down     # container dihapus, volume tetap
docker compose down -v  # hapus juga volume (reset total)
```

---

## Endpoints API

### POST /publish

Terima single atau batch event.

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "auth.login",
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "timestamp": "2025-01-15T10:30:00Z",
      "source": "service-a",
      "payload": {"user_id": 42}
    }]
  }'
# → {"queued": 1, "status": "accepted"}
```

### GET /events

```bash
# Semua event
curl http://localhost:8080/events

# Filter by topic
curl "http://localhost:8080/events?topic=auth.login"

# Dengan pagination
curl "http://localhost:8080/events?limit=50&offset=0"
```

### GET /stats

```bash
curl http://localhost:8080/stats
# → {"received": 20000, "unique_processed": 13000, "duplicate_dropped": 7000, ...}
```

### GET /health

```bash
curl http://localhost:8080/health
# → {"status": "ok", "db": "connected"}
```

### GET /audit

```bash
curl "http://localhost:8080/audit?action=duplicate_dropped&limit=20"
```

---

## Cara Menjalankan Tests

```bash
# Install test dependencies
pip install -r tests/requirements.txt

# Pastikan stack sedang berjalan, lalu:
cd tests
BASE_URL=http://localhost:8080 pytest test_aggregator.py -v

# Jalankan test spesifik
BASE_URL=http://localhost:8080 pytest test_aggregator.py::test_13_concurrent_dedup_no_double_processing -v
```

### 15 Tests yang Dicakup

| No | Test | Skenario |
|---|---|---|
| 01 | Health check | DB dan service terhubung |
| 02 | Single publish | 202 Accepted |
| 03 | Batch publish | Count benar |
| 04 | Validasi: missing field | 422 |
| 05 | Validasi: invalid timestamp | 422 |
| 06 | Validasi: empty event_id | 422 |
| 07 | Dedup: single duplicate | 1 processed, 1 dropped |
| 08 | Dedup: 10x duplicate | 1 processed, 9 dropped |
| 09 | GET /events returns data | Event muncul setelah diproses |
| 10 | GET /events topic filter | Filter bekerja benar |
| 11 | Stats structure | Semua field ada |
| 12 | Stats consistency | received = unique + duplicate |
| 13 | **Concurrency** | 8 worker paralel → tidak ada double-processing |
| 14 | Empty batch rejected | 400 |
| 15 | **Stress test** | 500 events < 30 detik, hitungan akurat |

---

## Konfigurasi Environment

### Aggregator

| Variable | Default | Keterangan |
|---|---|---|
| `DATABASE_URL` | `postgresql://user:pass@storage:5432/db` | PostgreSQL connection |
| `BROKER_URL` | `redis://broker:6379` | Redis connection |
| `NUM_WORKERS` | `4` | Jumlah consumer worker |

### Publisher

| Variable | Default | Keterangan |
|---|---|---|
| `TARGET_URL` | `http://aggregator:8080/publish` | Aggregator endpoint |
| `TOTAL_EVENTS` | `20000` | Total event yang dikirim |
| `DUPLICATE_RATE` | `0.35` | Proporsi duplikat (0.0–1.0) |
| `BATCH_SIZE` | `50` | Event per batch |
| `CONCURRENCY` | `8` | Concurrent batch publishers |

---

## Persistensi Data

| Volume | Mount | Isi |
|---|---|---|
| `pubsub_pg_data` | `/var/lib/postgresql/data` | Semua data PostgreSQL |
| `pubsub_broker_data` | `/data` | AOF Redis |

Data **aman melewati `docker compose down`**. Hanya hilang saat `docker compose down -v`.

---

## Keputusan Desain Utama

- **Dedup**: `INSERT ON CONFLICT DO NOTHING` dengan unique constraint `(topic, event_id)` — atomik, tidak butuh distributed lock.
- **Transaksi**: READ COMMITTED — cukup untuk constraint-based dedup, lebih ringan dari SERIALIZABLE.
- **Stats counter**: `UPDATE stats SET value = value + 1` — atomic increment, tidak ada lost-update.
- **Broker**: Redis dengan AOF — latensi rendah, cukup durable untuk use case ini.
- **Security**: Non-root containers, broker/storage hanya di jaringan internal.

---

## Struktur File

```
pubsub-aggregator/
├── aggregator/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── publisher/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── tests/
│   ├── test_aggregator.py   # 15 tests
│   ├── requirements.txt
│   └── pytest.ini
├── docker-compose.yml
├── README.md
└── report.md
```
