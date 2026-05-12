# Mini Project 3 — Real-Time Movie Recommendation System

**Domain**: Movies (MovieLens 1M dataset)
**Focus**: Real-Time Intelligence — trending detection, rating spikes, streaming impact
**Team**: Student A (this repo) + Student B (see `student_b/STUDENT_B_TASKS.md`)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  BATCH LAYER (Student A)                                        │
│                                                                 │
│  [MovieLens 1M]  ──►  [1_preprocess.py]  ──►  data/processed/  │
│                                                      │          │
│                              [2_train_als.py]  ◄─────┘          │
│                                      │                          │
│                              models/als_model/                  │
└──────────────────────────────────────┬──────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────┐
│  STREAMING LAYER (Student A + Student B)                        │
│                                                                 │
│  [3_kafka_producer.py]                                          │
│        │  {user_id, item_id, rating, timestamp}                 │
│        ▼                                                        │
│  Kafka: movie-ratings  (2 partitions, user_id % 2)              │
│        │                                                        │
│        ▼                                                        │
│  [4_spark_streaming.py]                                         │
│    ├── Window 30s/10s → avg_rating per item    (console)        │
│    ├── Window 30s/10s → interactions per user  (console)        │
│    ├── Raw valid events                        (Parquet)        │
│    │                                                            │
│    │   ── Student B adds ──────────────────────────────────     │
│    ├── Watermark (1 min late-data handling)                     │
│    ├── Trending score  (avg_rating × log(1+count))              │
│    ├── Alert system    (item avg > 4.5 / user spike)            │
│    └── ALS recommendations per user  (foreachBatch)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Docker Desktop | ≥ 24 | Must be running |
| Python | ≥ 3.10 | For producer + data download |
| Internet | — | First run downloads dataset & Maven JARs |

---

## Step-by-Step Run Instructions

### 0. Install Python dependencies (host machine)

```bash
cd mini_proj3
pip install -r requirements.txt
```

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts:
- `zookeeper` — Kafka coordination
- `kafka` — broker on `localhost:9092`
- `kafka-init` — creates the `movie-ratings` topic (2 partitions) and exits
- `spark` — Spark 3.5.1 container (stays running, used for all Spark jobs)

Wait ~30 seconds for Kafka to be healthy, then verify:

```bash
docker compose ps
# kafka and zookeeper should show "healthy"
```

### 2. Download the dataset

```bash
python data/download_data.py
# Downloads ~6 MB zip, extracts to data/ml-1m/
# 1,000,209 ratings  |  6,040 users  |  3,706 movies
```

### 3. Preprocess data

```bash
docker exec spark spark-submit /workspace/student_a/1_preprocess.py
```

Expected output:
```
Raw lines: 1,000,209
Rows dropped (null): 0
Rows dropped (out-of-range): 0
Final record count: 1,000,209  (≥ 500K ✓)
Unique users: 6,040
Unique items: 3,706
Matrix sparsity: 95.5295%  → justifies distributed ALS
```

Preprocessed data written to `data/processed/` (Parquet, 4 partitions).

### 4. Train ALS model

```bash
docker exec spark spark-submit /workspace/student_a/2_train_als.py
```

- 80/20 split, seed=42
- Initial: rank=10, regParam=0.1, maxIter=10
- If RMSE > 1.5 the script auto-tunes (tries 4 additional configurations)
- Model saved to `models/als_model/`
- RMSE saved to `models/rmse.txt`

Expected RMSE for MovieLens 1M with default params: **~0.87** (well under 1.5).

### 5. Start the streaming pipeline (Terminal 1)

```bash
docker exec -it spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /workspace/student_a/4_spark_streaming.py
```

> First run downloads the Kafka connector JAR (~20 MB, cached in a Docker volume).
> Spark Web UI available at **http://localhost:4040**.

### 6. Start the Kafka producer (Terminal 2 — on host machine)

```bash
# Default: 5 events/sec, runs forever, spikes items 120 356 589 1197
python student_a/3_kafka_producer.py

# Faster with explicit spike items
python student_a/3_kafka_producer.py --rate 10 --spike-items 120 356

# Fixed duration (useful for testing)
python student_a/3_kafka_producer.py --rate 5 --duration 300
```

Within ~30 seconds the streaming terminal will print windowed aggregations:

```
-------------------------------------------
Batch: 1  [avg_rating_per_item]
-------------------------------------------
+-------------------+-------------------+-------+----------+-----------------+
|window_start       |window_end         |item_id|avg_rating|interaction_count|
+-------------------+-------------------+-------+----------+-----------------+
|2024-01-01 12:00:00|2024-01-01 12:00:30|  120  |   4.750  |        6        |
...
```

### 7. Stop everything

```bash
# Ctrl+C in Terminal 1 and Terminal 2, then:
docker compose down
```

---

## Project Structure

```
mini_proj3/
├── docker-compose.yml          Zookeeper + Kafka + Spark
├── requirements.txt            kafka-python (host-side)
├── README.md
│
├── data/
│   ├── download_data.py        Downloads MovieLens 1M
│   ├── ml-1m/                  Raw dataset (created by download)
│   └── processed/              Cleaned Parquet (created by step 3)
│
├── models/
│   ├── als_model/              Trained ALS model (created by step 4)
│   └── rmse.txt                Final RMSE value
│
├── output/
│   ├── raw_events/             Valid streaming events (Parquet)
│   └── checkpoints/            Spark streaming checkpoints
│
├── student_a/
│   ├── 1_preprocess.py         Data cleaning & validation
│   ├── 2_train_als.py          ALS training, RMSE, tuning
│   ├── 3_kafka_producer.py     Streaming event generator
│   └── 4_spark_streaming.py   Window analytics pipeline
│
└── student_b/
    └── STUDENT_B_TASKS.md      Handoff: watermark, alerts, recs, report
```

---

## Dataset Justification

| Property | Value | Why it matters |
|----------|-------|----------------|
| Size | 1,000,209 ratings | Exceeds 500K requirement; needs distributed processing |
| Format | (user_id, item_id, rating, timestamp) | Exact match for spec |
| Users | 6,040 | Enough for meaningful collaborative filtering |
| Items | 3,706 | Enough for recommendation diversity |
| Sparsity | ~95.5% | Classical sparse matrix problem — ALS is designed for this |
| Distributed need | 6040 × 3706 = 22M possible pairs | Single-machine matrix ops impractical |

---

## Kafka Partitioning Strategy

Topic `movie-ratings` uses **2 partitions**.  
Assignment: `partition = user_id % 2`

**Rationale**: All events for a given user land on the same partition, preserving arrival order per user and making user-level aggregations (interaction counts, activity spikes) efficient — the consumer only needs one partition's state to compute a complete user view.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `NoBrokersAvailable` in producer | Wait 30s after `docker compose up`, or check `docker compose ps` |
| Kafka JAR download hangs | First run downloads ~20 MB; give it 2–3 minutes |
| `WARN TaskSchedulerImpl` spam | Normal in local mode with 2 cores; safe to ignore |
| `data/ml-1m not found` | Run `python data/download_data.py` first |
| Port 4040 already in use | Another Spark session is running; stop it first |
