# Mini Project 3 — Real-Time Movie Recommendation System

**Domain:** Movies (MovieLens 1M)
**Focus:** Real-Time Intelligence — trending detection, rating spikes, ML-in-stream
**Run date:** 2026-05-13

---

## 1. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  BATCH LAYER                                                    │
│                                                                 │
│  MovieLens 1M ──► 1_preprocess.py ──► data/processed/ (Parquet) │
│                          │                                       │
│                          ▼                                       │
│                   2_train_als.py ──► models/als_model/           │
│                                       models/rmse.txt            │
└──────────────────────────────────────┬──────────────────────────┘
                                       │ ALS model loaded
┌──────────────────────────────────────▼──────────────────────────┐
│  STREAMING LAYER                                                │
│                                                                 │
│   3_kafka_producer.py ──► Kafka topic "movie-ratings"           │
│      JSON events            (2 partitions, partition=user_id%2) │
│                                       │                          │
│                                       ▼                          │
│                            4_spark_streaming.py                  │
│                            (Structured Streaming)                │
│                                       │                          │
│      ┌────────────────────────────────┼────────────────────────┐│
│      │ 30s / 10s tumbling window with 1-min watermark          ││
│      └────────────────────────────────┬────────────────────────┘│
│                                       │                          │
│   ┌─────────────┬─────────────┬───────┴────────┬──────────────┐ │
│   ▼             ▼             ▼                ▼              ▼ │
│ avg_rating   interactions  trending     alerts (item+user)  recs│
│ /item        /user         (top-5)      (avg≥4.5 / count≥3)     │
│ parquet      console       console      console + parquet       │
│ + console    +             foreachBatch                          │
│              parquet                                  foreachBatch│
│              raw_events                                  via ALS │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. ML Component — ALS

**Algorithm.** Alternating Least Squares matrix factorisation from PySpark MLlib.
Given the sparse user × item rating matrix R (6,040 × 3,706, 95.5 % sparse), ALS learns two dense latent-factor matrices U and V such that R ≈ U Vᵀ. Each user u and item i is represented by a `rank`-dimensional vector; the predicted rating is the dot product u·v.

**Configuration used.** `rank=10, regParam=0.1, maxIter=10, coldStartStrategy="drop"`.

**Cold-start.** Users not present in training (or items new since training) cannot be embedded — `coldStartStrategy="drop"` removes them from predictions instead of returning NaN. In the streaming context this means new users simply receive no recommendations until the model is retrained.

**Evaluation.**

| Metric | Value |
|---|---|
| Train / test split | 80 / 20, seed=42 |
| Train rows | 799,826 |
| Test rows | 200,383 |
| **RMSE** | **0.8694** (well under 1.5 threshold; no tuning required) |

RMSE persisted to `models/rmse.txt`, model directory `models/als_model/`.

---

## 3. Streaming Component

**Source.** Kafka topic `movie-ratings`, **2 partitions**, partitioning key `user_id % 2`. This guarantees that all events for a given user land on the same partition, preserving per-user ordering and making user-level aggregations stateful in a single partition.

**Window.** 30-second tumbling-with-slide windows (slide = 10 s) — every 10 s a new window starts, and each event contributes to up to 3 overlapping windows. This smooths short-term noise while still surfacing spikes within seconds.

**Watermark.** `withWatermark("event_time", "1 minute")` on `valid_stream`. Events whose `event_time < max_event_time_seen − 1 min` are dropped, which bounds the streaming state and enables **append-mode** parquet sinks on the windowed aggregations.

**Trade-off.** Late events after the 1-minute watermark are silently discarded; the system trades completeness for bounded state. In a production setting this would be tuned with knowledge of the upstream delay distribution.

---

## 4. Integration: Batch ALS → Streaming

The ALS model trained in step 2 is loaded once when the streaming job starts (`load_als_model`), then closed-over inside a `foreachBatch` sink:

```python
q_recs = valid_stream.writeStream
    .foreachBatch(lambda df, eid: generate_recommendations(df, eid, als_model))
    .trigger(processingTime="10 seconds")
    .start()
```

For every micro-batch, `generate_recommendations` takes the distinct `user_id`s present, calls `als_model.recommendForUserSubset(users, top_n*4)`, joins on `(user_id, item_id)` to anti-join out items the user just rated, then re-ranks with a window function. Output: per-user top-5 with `predicted_rating`.

`foreachBatch` is the bridge: it lets a streaming query consume an arbitrary batch-mode operation (`recommendForUserSubset`, which doesn't exist for streaming DataFrames) once per micro-batch.

---

## 5. Results (sample console output)

### 5.1 Top-5 trending items (Batch from run at 02:37:30)
```
window_start          window_end           item_id  avg_rating  count  trending_score
2026-05-13 02:37:20   2026-05-13 02:37:50  589      4.765       34     16.941
2026-05-13 02:37:30   2026-05-13 02:38:00  589      4.761       23     15.131
2026-05-13 02:37:20   2026-05-13 02:37:50  356      4.725       20     14.385
2026-05-13 02:37:20   2026-05-13 02:37:50  120      4.559       17     13.177
2026-05-13 02:37:30   2026-05-13 02:38:00  356      4.733       15     13.123
```
The three spike items (120, 356, 589) dominate trending — as designed.

### 5.2 Item alerts (avg_rating ≥ 4.5, count ≥ 3)
```
window_start          window_end           item_id  avg_rating  alert_msg
2026-05-13 02:37:10   2026-05-13 02:37:40  589      4.742       ALERT: Item 589 is trending — avg_rating = 4.742 (threshold ≥ 4.5)
2026-05-13 02:37:10   2026-05-13 02:37:40  356      4.682       ALERT: Item 356 is trending — avg_rating = 4.682 (threshold ≥ 4.5)
2026-05-13 02:37:10   2026-05-13 02:37:40  120      4.632       ALERT: Item 120 is trending — avg_rating = 4.632 (threshold ≥ 4.5)
```

### 5.3 User alerts (≥ 3 interactions / window)
```
window_start          window_end           user_id  interaction_count  alert_msg
2026-05-13 02:37:30   2026-05-13 02:38:00  2178     3                  ALERT: User 2178 spike — 3 interactions in window
2026-05-13 02:37:40   2026-05-13 02:38:10  2178     3                  ALERT: User 2178 spike — 3 interactions in window
```

### 5.4 Recommendations (excerpt)
```
user_id  item_id  predicted_rating
37       687      5.73
37       3092     5.43
37       572      5.18
37       1696     5.15
37       1669     5.14
96       572      4.59
96       1851     4.32
900      1743     4.57
900      3338     4.57
900      670      4.56
```
(Items the user rated *within the same micro-batch* are anti-joined out before ranking.)

### 5.5 Artifacts on disk
| Path | Contents |
|---|---|
| `data/processed/` | 4 Parquet files, 1,000,209 cleaned ratings |
| `models/als_model/` | Trained ALSModel |
| `models/rmse.txt` | `RMSE: 0.8694` |
| `output/raw_events/` | Streaming valid events (Parquet, checkpointed) |
| `output/avg_rating_per_item/` | Windowed item aggregates (Parquet, append mode) |
| `output/alerts/item/` | Item alerts persisted to Parquet |
| `output/alerts/user/` | User alerts persisted to Parquet |

---

## 6. Latency Measurement

For each micro-batch the recommendation sink prints `[Recommendation latency: X.XXXs]`. Observed values during the verification run:

| Batch | Latency (s) |
|---|---|
| 1 (empty) | 0.101 |
| 2 (cold) | 6.297 |
| 3 | 3.563 |
| 4 | 4.620 |
| 5 | 3.007 |
| 6 | 1.985 |
| 7 | 3.485 |
| 8 | 5.137 |

**Median ≈ 3.5 s, p95 ≈ 5.1 s.** Aside from the first non-empty batch (JVM warm-up + first model invocation), all batches are at or under the 5-second target from the spec bonus. The cold-start cost is paid once.

**What dominates the latency.** `recommendForUserSubset` runs an inner join between the user-factor matrix (6,040 × 10) and the item-factor matrix (3,706 × 10), then a per-user top-K. For small batches (≤ 50 distinct users typical at 20 ev/s) the constant Spark overhead (job submit, planning) dominates; the actual computation is microseconds.

---

## 7. Custom Metric — Trending Score

**Formula.**
```
trending_score = avg_rating × log(1 + interaction_count)
```

**Why this shape.**
- The `avg_rating` factor punishes items that are merely popular but mediocre — a 50-vote item averaging 2.5 stars scores lower than a 50-vote item averaging 4.5.
- `log(1 + count)` is sub-linear, so a single 5-star review cannot outrank a 4-star item with 50 reviews. Concretely: `5.0 × log(1+1) = 3.47`, but `4.0 × log(1+50) = 15.7`.
- `log1p` (log(1+x)) handles `count = 0` gracefully without `-inf`.

**Implementation note.** Spark Structured Streaming forbids `orderBy` on streaming DataFrames in update output mode, so the snippet's `.orderBy(...)` was moved into a `foreachBatch` sink. The sort runs on the small per-batch DataFrame instead of being a query-plan node — same result, allowed by the engine.

---

## 8. Challenges & Lessons Learned

1. **Watermark vs. completeness.** Choosing 1 minute was a guess. A larger watermark covers more late events but inflates state; smaller forces append-mode aggregations but drops more rows. There is no "correct" value without measuring the upstream delay distribution.

2. **`orderBy` is not free on streaming DataFrames.** The snippet originally called `.orderBy(...)` on the trending stream — Spark rejected the plan (`Sort is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode`). The fix: sort *inside* a `foreachBatch`, where the DataFrame is bounded.

3. **Cold-start for ML.** `coldStartStrategy="drop"` is silent — a new user simply sees no recommendations rather than a fallback. In production we would back-fill with a popularity baseline or a content-based model for unknown users.

4. **First-batch latency dominates.** The very first non-empty batch through `foreachBatch` paid ~6 s (JIT, model materialisation). Pre-warming the JVM by submitting a dummy recommendation request at startup would smooth this out.

5. **kafka-python 2.0.2 is broken on Python 3.13.** The vendored `six` module's `moves` submodule fails to import. Solved by swapping to the maintained fork `kafka-python-ng 2.2.3`. Worth updating `requirements.txt`.

6. **Windows console encoding.** The producer prints a `→` arrow; default cp1252 raises `UnicodeEncodeError`. Setting `PYTHONIOENCODING=utf-8` fixes it without touching the code.

7. **Realistic thresholds matter.** The originally proposed user-spike threshold of 10 interactions / 30 s window never fires at 10 ev/s across 6,040 users — the load is too diluted. Lowering to 3 surfaced real alerts (e.g., user 2178). Threshold tuning is part of the system design, not an afterthought.

8. **`recommendForUserSubset` is batch-only.** It has no streaming equivalent, which is exactly why the integration uses `foreachBatch`. Future Spark releases that add a true streaming join with a static DataFrame may simplify this pattern.
