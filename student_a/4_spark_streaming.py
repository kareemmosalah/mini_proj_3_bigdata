"""
Student A — Task 4: Spark Structured Streaming Pipeline
Source  : Kafka topic  'movie-ratings'  (2 partitions)
Output  : Console (windowed aggregations) + Parquet (raw valid events)

Window analytics (per spec):
  Window size  : 30 seconds
  Slide        : 10 seconds
  Metrics      :
    • avg_rating_per_item   — avg rating & interaction count per item per window
    • interactions_per_user — count & avg rating given per user per window

Malformed record handling:
  from_json() returns nulls for unparseable JSON.
  All rows with null user_id / item_id / rating are filtered BEFORE aggregation.

Integration points for Student B (marked TODO_B):
  • TODO_B-1 : Add watermark for late data handling  (section 8)
  • TODO_B-2 : Add custom metric (trending score)    (section 6)
  • TODO_B-3 : Add alert system                      (section 8)
  • TODO_B-4 : Wire in ALS model for recommendations (section 7)

Kafka addresses:
  Running inside Docker (spark container) : kafka:29092   [set by KAFKA_BOOTSTRAP_SERVERS env]
  Running locally (pyspark)               : localhost:9092 [default]

Spark-Kafka package version:
  Inside Docker (Spark 3.5.1): org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
  Locally (pyspark 3.5.3)    : org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
  Override via SPARK_KAFKA_PKG env variable.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType,
)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = "movie-ratings"
KAFKA_PKG     = os.environ.get(
    "SPARK_KAFKA_PKG",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "output", "checkpoints")

WINDOW_DURATION = "30 seconds"
SLIDE_DURATION  = "10 seconds"

# ── JSON schema for incoming events ───────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("user_id",   IntegerType(), True),
    StructField("item_id",   IntegerType(), True),
    StructField("rating",    FloatType(),   True),
    StructField("timestamp", StringType(),  True),
])


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MovieRatings_Streaming")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR,     exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("Spark Structured Streaming — Movie Ratings")
    print(f"  Kafka : {KAFKA_SERVERS}  |  Topic : {KAFKA_TOPIC}")
    print(f"  Window: {WINDOW_DURATION}  |  Slide : {SLIDE_DURATION}")
    print("=" * 60)

    # ── 1. Read from Kafka ────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        # Read both partitions in parallel
        .option("minPartitions", str(2))
        .load()
    )

    # ── 2. Parse JSON — malformed records become rows with null fields ────
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("d"),
            F.col("kafka_ts"),
        )
        .select(
            F.col("d.user_id"),
            F.col("d.item_id"),
            F.col("d.rating"),
            # Parse event timestamp; fall back to Kafka ingestion time if absent/malformed
            F.coalesce(
                F.to_timestamp(F.col("d.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
                F.to_timestamp(F.col("d.timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                F.to_timestamp(F.col("d.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                F.to_timestamp(F.col("d.timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.col("kafka_ts"),
            ).alias("event_time"),
        )
    )

    # ── 3. Drop malformed / invalid records ──────────────────────────────
    valid_stream = parsed.filter(
        F.col("user_id").isNotNull()
        & F.col("item_id").isNotNull()
        & F.col("rating").isNotNull()
        & F.col("rating").between(0.5, 5.0)
        & F.col("event_time").isNotNull()
    )

    # TODO_B-1 ── Watermark for late data handling (Student B adds this line):
    # valid_stream = valid_stream.withWatermark("event_time", "1 minute")
    # Once added, change parquet sinks below to outputMode("append").

    # ── 4a. Window: average rating per item ──────────────────────────────
    avg_rating_per_item = (
        valid_stream
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("item_id"),
        )
        .agg(
            F.round(F.avg("rating"), 3).alias("avg_rating"),
            F.count("*").alias("interaction_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "item_id",
            "avg_rating",
            "interaction_count",
        )
    )

    # ── 4b. Window: interactions per user ────────────────────────────────
    interactions_per_user = (
        valid_stream
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("interaction_count"),
            F.round(F.avg("rating"), 3).alias("avg_rating_given"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id",
            "interaction_count",
            "avg_rating_given",
        )
    )

    # TODO_B-2 ── Trending score (Student B adds after avg_rating_per_item):
    # trending = avg_rating_per_item.withColumn(
    #     "trending_score",
    #     F.round(F.col("avg_rating") * F.log1p(F.col("interaction_count")), 3)
    # )

    # TODO_B-3 ── Alert system (Student B adds):
    # alerts = avg_rating_per_item.filter(F.col("avg_rating") > 4.5)

    # TODO_B-4 ── ML integration (Student B adds after loading ALS model):
    # recs_stream = valid_stream.transform(lambda df: generate_recommendations(df, als_model))

    # ── 5. Sinks ──────────────────────────────────────────────────────────

    # Console: avg rating per item  (update mode works without watermark)
    q_items = (
        avg_rating_per_item.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "10")
        .queryName("avg_rating_per_item")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Console: interactions per user
    q_users = (
        interactions_per_user.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "10")
        .queryName("interactions_per_user")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Parquet: raw valid events (append, no aggregation — no watermark needed)
    # Student B adds aggregated parquet sinks once watermark is in place.
    q_raw = (
        valid_stream.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", os.path.join(OUTPUT_DIR, "raw_events"))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw_events"))
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("\nStreaming started. Spark UI → http://localhost:4040")
    print("Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nShutting down streams...")
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("Streaming stopped.")


if __name__ == "__main__":
    main()
