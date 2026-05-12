# Student B — Task Handoff

> Domain: **Movies (MovieLens 1M)**  |  Focus: **Real-Time Intelligence**

---

## ✅ Student A Verified Output Log (2026-05-13)

All steps ran and passed end-to-end. Copy these numbers into the report.

### Step 1 — Preprocessing (`1_preprocess.py`)

```text
Raw lines          : 1,000,209
Null rows dropped  : 0
Invalid ratings    : 0
Duplicate pairs    : 0
Final record count : 1,000,209  (>=500K check passed)
Unique users       : 6,040
Unique items       : 3,706
Matrix sparsity    : 95.53%   -> justifies distributed Spark processing

Rating distribution:
  1.0 -> 56,174   2.0 -> 107,557   3.0 -> 261,197
  4.0 -> 348,971  5.0 -> 226,310
  Mean: 3.58  |  Std: 1.12
```

### Step 2 — ALS Training (`2_train_als.py`)

```text
Train set : 799,826 ratings  (80%)
Test set  : 200,383 ratings  (20%)
Parameters: rank=10, regParam=0.1, maxIter=10, coldStartStrategy=drop
RMSE      : 0.8694  <- well under 1.5 threshold, no tuning required
Model saved -> models/als_model/
```

Sample top-5 recommendations produced (excerpt):

```text
user_id  item_id  predicted_rating
12       572      5.005
12       858      4.592
13       572      4.678
14       583      4.781
18       2562     5.430
38       572      5.203
```

### Step 3 — Kafka Producer (`3_kafka_producer.py`)

```text
Events produced : 701 in 90 seconds
Throughput      : 7.8 events/sec
Errors          : 0
Partitions      : 2  (partition = user_id % 2)
Spike items     : 120, 356, 589  (forced rating >= 4.5)
```

### Step 4 — Spark Structured Streaming (`4_spark_streaming.py`)

```text
Window    : 30 seconds  |  Slide: 10 seconds
Kafka JAR : org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1  (downloaded + cached)
Batches   : 5+ processed without errors
Raw events written to output/raw_events/ (Parquet, checkpointed)
```

Streaming output excerpt (Batch 4 — avg_rating_per_item):

```text
window_start         window_end           item_id  avg_rating  interaction_count
2026-05-12 22:14:10  2026-05-12 22:14:40  589      4.8         10   <- SPIKE (will trigger alert)
2026-05-12 22:14:30  2026-05-12 22:15:00  3599     4.5         1
2026-05-12 22:14:30  2026-05-12 22:15:00  545      4.5         1
```

Streaming output excerpt (Batch 4 — interactions_per_user):

```text
window_start         window_end           user_id  interaction_count  avg_rating_given
2026-05-12 22:14:20  2026-05-12 22:14:50  5606     2                  4.25
2026-05-12 22:14:20  2026-05-12 22:14:50  5584     1                  4.5
2026-05-12 22:14:30  2026-05-12 22:15:00  5370     1                  5.0
```

### Artifacts on disk

| Path | Contents |
| --- | --- |
| `data/processed/` | 4 Parquet files, 1,000,209 cleaned ratings |
| `models/als_model/` | Trained ALSModel (PySpark MLlib) |
| `models/rmse.txt` | `RMSE: 0.8694` |
| `output/raw_events/` | Streaming valid events (Parquet, checkpointed) |

---

## What Student A already built

| File | What it does |
|------|-------------|
| `docker-compose.yml` | Starts Zookeeper, Kafka (2-partition topic `movie-ratings`), and the Spark container |
| `data/download_data.py` | Downloads MovieLens 1M (1 M ratings) |
| `student_a/1_preprocess.py` | PySpark cleaning → `data/processed/` (Parquet) |
| `student_a/2_train_als.py` | Trains ALS; saves model to `models/als_model/`, RMSE to `models/rmse.txt` |
| `student_a/3_kafka_producer.py` | Produces `{user_id, item_id, rating, timestamp}` events; injects rating spikes |
| `student_a/4_spark_streaming.py` | Consumes Kafka, parses JSON, window analytics (avg rating/item + interactions/user), raw events → Parquet |

**Before you start**: make sure all of Student A's steps have been run (data downloaded, model trained, streaming pipeline tested with the producer running).

---

## Your Tasks (in recommended order)

### Task B-1 — Late Data Handling & Watermarking

**File to modify**: `student_a/4_spark_streaming.py`

In `main()`, find the comment `# TODO_B-1` and **uncomment** the watermark line:

```python
valid_stream = valid_stream.withWatermark("event_time", "1 minute")
```

**What this does**: Spark drops events that arrive more than 1 minute late.
Any event whose `event_time < (max_event_time_seen − 1 minute)` is discarded.
This bounds the state size and allows append-mode sinks for windowed aggregations.

**After adding watermark**, you can change the parquet sinks for aggregated results from `outputMode("update")` to `outputMode("append")`.  Example:

```python
avg_rating_per_item.writeStream
    .outputMode("append")          # was: update (console-only, no file output)
    .format("parquet")
    .option("path", os.path.join(OUTPUT_DIR, "avg_rating_per_item"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "avg_rating"))
    .trigger(processingTime="30 seconds")
    .start()
```

**In your report**: explain that events arriving after the watermark are silently dropped (not re-processed), so the system trades completeness for bounded state.

---

### Task B-2 — Custom Streaming Metric: Trending Score

**File to modify**: `student_a/4_spark_streaming.py`

Find `# TODO_B-2` and add the trending score calculation right after `avg_rating_per_item` is defined:

```python
from pyspark.sql import functions as F

trending = avg_rating_per_item.withColumn(
    "trending_score",
    # Score = avg_rating × log(1 + interactions)
    # High rating AND high volume → high score
    F.round(F.col("avg_rating") * F.log1p(F.col("interaction_count").cast("double")), 3)
).orderBy(F.col("trending_score").desc())
```

Add a console sink for it:

```python
q_trending = (
    trending.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "5")
    .queryName("trending_items")
    .trigger(processingTime="10 seconds")
    .start()
)
```

**In your report**: explain the formula — items with both high ratings AND high interaction volume rank highest, so a single 5-star review doesn't beat an item with 50 solid 4-star reviews.

---

### Task B-3 — Alert System

**File to modify**: `student_a/4_spark_streaming.py`

Find `# TODO_B-3` and add after the trending block:

```python
ALERT_RATING_THRESHOLD    = 4.5
ALERT_ACTIVITY_THRESHOLD  = 10   # interactions per user per window

# Alert: item average crosses threshold
item_alerts = avg_rating_per_item.filter(
    (F.col("avg_rating") >= ALERT_RATING_THRESHOLD)
    & (F.col("interaction_count") >= 3)   # ignore single-vote flukes
).withColumn(
    "alert_msg",
    F.concat_ws(" ",
        F.lit("ALERT: Item"),
        F.col("item_id").cast("string"),
        F.lit("is trending — avg_rating ="),
        F.col("avg_rating").cast("string"),
        F.lit(f"(threshold ≥ {ALERT_RATING_THRESHOLD})"),
    )
)

# Alert: user activity spike
user_alerts = interactions_per_user.filter(
    F.col("interaction_count") >= ALERT_ACTIVITY_THRESHOLD
).withColumn(
    "alert_msg",
    F.concat_ws(" ",
        F.lit("ALERT: User"),
        F.col("user_id").cast("string"),
        F.lit("spike —"),
        F.col("interaction_count").cast("string"),
        F.lit("interactions in window"),
    )
)

# Print alerts to console
q_item_alerts = (
    item_alerts.select("window_start", "window_end", "item_id", "avg_rating", "alert_msg")
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .queryName("item_alerts")
    .trigger(processingTime="10 seconds")
    .start()
)

q_user_alerts = (
    user_alerts.select("window_start", "window_end", "user_id", "interaction_count", "alert_msg")
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .queryName("user_alerts")
    .trigger(processingTime="10 seconds")
    .start()
)
```

**To trigger a test alert manually**: run the producer with `--spike-items 120 --rate 20` — the injected 4.5/5.0 ratings will push item 120's average above the threshold.

---

### Task B-4 — ML + Streaming Integration (Recommendations)

**File to modify**: `student_a/4_spark_streaming.py`

Find `# TODO_B-4`.  Create a **new helper** (add this outside `main()`):

```python
from pyspark.ml.recommendation import ALSModel

def load_als_model(spark, model_path):
    return ALSModel.load(model_path)

def generate_recommendations(micro_batch_df, epoch_id, als_model, top_n=5):
    """
    Called by foreachBatch — generates top-N recs for every unique user
    in the micro-batch and prints them.
    Latency target: < 5 seconds per batch (see spec bonus).
    """
    import time
    t0 = time.time()

    users = micro_batch_df.select("user_id").distinct()
    recs = als_model.recommendForUserSubset(users, top_n)

    from pyspark.sql import functions as F
    (
        recs
        .select("user_id", F.explode("recommendations").alias("r"))
        .select(
            "user_id",
            F.col("r.item_id").alias("item_id"),
            F.round("r.rating", 2).alias("predicted_rating"),
        )
        .orderBy("user_id", F.col("predicted_rating").desc())
        .show(top_n * 3, truncate=False)
    )

    latency = time.time() - t0
    print(f"[Recommendation latency: {latency:.3f}s]")
```

Then in `main()`, after loading the model:

```python
MODEL_PATH = os.path.join(BASE_DIR, "models", "als_model")
als_model  = load_als_model(spark, MODEL_PATH)

q_recs = (
    valid_stream.writeStream
    .foreachBatch(lambda df, eid: generate_recommendations(df, eid, als_model))
    .trigger(processingTime="10 seconds")
    .queryName("recommendations")
    .start()
)
```

**In your report**: measure the latency printed for each batch and compare against the 5-second bonus target.  Note that cold-start (users not in training data) are handled by `coldStartStrategy="drop"` in the ALS model — they simply receive no recommendations.

---

## Report Sections (Student B owns)

1. **System Architecture Diagram** — draw the full pipeline:
   ```
   [MovieLens 1M]
        ↓
   [1_preprocess.py] → data/processed/ (Parquet)
        ↓
   [2_train_als.py]  → models/als_model/
                                          ↘
   [3_kafka_producer.py]                  [4_spark_streaming.py + your additions]
        ↓                                          ↓                ↓          ↓
   Kafka: movie-ratings              Window Analytics     Alerts   Recs    Trending
   (2 partitions, user_id % 2)
   ```

2. **ML Component** — describe ALS: matrix factorization, latent factors, cold-start handling, final RMSE (`models/rmse.txt`).

3. **Streaming Component** — describe the Kafka setup (2-partition strategy), window (30s/10s), watermark (1 minute).

4. **Integration Explanation** — how the batch model feeds into the streaming pipeline via `foreachBatch`.

5. **Results** — include console screenshots of recommendations, alerts, trending scores.

6. **Latency Measurement** — record the `[Recommendation latency: X.XXXs]` output.

7. **Custom Metric Explanation** — trending score formula and why it was chosen.

8. **Challenges & Lessons Learned** — watermark trade-offs, cold-start problem, state management.

---

## Quick-Start Commands (after Student A has run everything)

```bash
# Start infrastructure (if not already running)
docker compose up -d

# Terminal 1 — producer (from project root)
python student_a/3_kafka_producer.py --rate 10 --spike-items 120 356

# Terminal 2 — streaming (inside Docker Spark container)
docker exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /workspace/student_a/4_spark_streaming.py
```
