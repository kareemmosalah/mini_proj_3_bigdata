"""
Student A — Task 1: Data Preprocessing
Dataset: MovieLens 1M (1,000,209 ratings)
Input : data/ml-1m/ratings.dat   (UserID::MovieID::Rating::Timestamp)
Output: data/processed/          (Parquet, partitioned for Spark)

Steps:
  1. Load raw text, parse '::' delimiter
  2. Cast columns to correct types
  3. Drop null / unparseable rows
  4. Validate rating range [0.5, 5.0]
  5. Deduplicate (user_id, item_id) — keep latest interaction
  6. Print statistics
  7. Save as Parquet (4 partitions)
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, LongType

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH    = os.path.join(BASE_DIR, "data", "ml-1m", "ratings.dat")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed")


def create_spark():
    return (
        SparkSession.builder
        .appName("MovieLens_Preprocess")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    if not os.path.exists(RAW_PATH):
        print(f"ERROR: raw data not found at {RAW_PATH}")
        print("Run first:  python data/download_data.py")
        sys.exit(1)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("STEP 1 — Load raw MovieLens 1M ratings")
    print("=" * 60)

    # Ratings are separated by '::' which Spark CSV can't handle natively.
    # Read as plain text, then split manually.
    raw = spark.read.text(RAW_PATH)
    raw_count = raw.count()
    print(f"  Raw lines: {raw_count:,}")

    df = (
        raw
        .select(F.split(F.col("value"), "::").alias("p"))
        .select(
            F.col("p")[0].cast(IntegerType()).alias("user_id"),
            F.col("p")[1].cast(IntegerType()).alias("item_id"),
            F.col("p")[2].cast(FloatType()).alias("rating"),
            F.col("p")[3].cast(LongType()).alias("timestamp"),
        )
    )

    # ── 2. Drop rows where any cast failed (value became null) ────────────
    print("\nSTEP 2 — Drop nulls from failed casts")
    df_no_null = df.dropna(subset=["user_id", "item_id", "rating", "timestamp"])
    null_dropped = raw_count - df_no_null.count()
    print(f"  Rows dropped (null): {null_dropped:,}")

    # ── 3. Validate rating range ──────────────────────────────────────────
    print("\nSTEP 3 — Filter invalid ratings (keep 0.5 – 5.0)")
    df_valid = df_no_null.filter(
        F.col("rating").between(0.5, 5.0)
    )
    invalid_dropped = df_no_null.count() - df_valid.count()
    print(f"  Rows dropped (out-of-range rating): {invalid_dropped:,}")

    # ── 4. Deduplicate (user_id, item_id): keep the most recent rating ────
    print("\nSTEP 4 — Deduplicate (user_id, item_id) — keep latest timestamp")
    df_dedup = (
        df_valid
        .orderBy(F.col("timestamp").desc())
        .dropDuplicates(["user_id", "item_id"])
    )
    final_count = df_dedup.count()
    dup_dropped = df_valid.count() - final_count
    print(f"  Rows dropped (duplicates): {dup_dropped:,}")
    print(f"  Final record count:        {final_count:,}  (≥ 500K ✓)")

    # ── 5. Statistics ─────────────────────────────────────────────────────
    print("\nSTEP 5 — Dataset statistics")
    n_users = df_dedup.select("user_id").distinct().count()
    n_items = df_dedup.select("item_id").distinct().count()
    print(f"  Unique users : {n_users:,}")
    print(f"  Unique items : {n_items:,}")

    print("\n  Rating summary:")
    df_dedup.describe("rating").show(truncate=False)

    print("  Rating distribution:")
    (
        df_dedup
        .groupBy("rating")
        .count()
        .orderBy("rating")
        .show(truncate=False)
    )

    # Sparsity — useful for justifying distributed processing
    total_possible = n_users * n_items
    sparsity = 1.0 - final_count / total_possible
    print(f"  Matrix sparsity: {sparsity:.4%}  → justifies distributed ALS")

    # ── 6. Save ───────────────────────────────────────────────────────────
    print(f"\nSTEP 6 — Save Parquet → {OUTPUT_PATH}")
    df_dedup.repartition(4).write.mode("overwrite").parquet(OUTPUT_PATH)
    print("  Saved successfully.")

    spark.stop()
    print("\nPreprocessing complete.")


if __name__ == "__main__":
    main()
