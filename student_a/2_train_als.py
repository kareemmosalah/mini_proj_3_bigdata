"""
Student A — Task 2: ALS Model Training & Evaluation
Algorithm : Collaborative Filtering via Spark MLlib ALS
Input     : data/processed/  (Parquet from 1_preprocess.py)
Output    : models/als_model/   (saved PipelineModel)
            models/rmse.txt     (final RMSE for Student B reference)

Steps:
  1. Load preprocessed ratings
  2. 80 / 20 train-test split (seed=42)
  3. Train initial ALS (rank=10, regParam=0.1, maxIter=10)
  4. Evaluate RMSE on test set
  5. If RMSE > 1.5 → tune (grid over rank / regParam / maxIter)
  6. Save best model
  7. Print sample top-5 recommendations
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH   = os.path.join(BASE_DIR, "data", "processed")
MODEL_PATH  = os.path.join(BASE_DIR, "models", "als_model")
RMSE_FILE   = os.path.join(BASE_DIR, "models", "rmse.txt")

RMSE_THRESHOLD = 1.5

# Tuning grid searched when RMSE > threshold
TUNING_GRID = [
    {"rank": 20, "reg_param": 0.05, "max_iter": 15},
    {"rank": 15, "reg_param": 0.01, "max_iter": 20},
    {"rank": 10, "reg_param": 0.30, "max_iter": 20},
    {"rank": 25, "reg_param": 0.05, "max_iter": 10},
]


def create_spark():
    return (
        SparkSession.builder
        .appName("ALS_Training")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def build_als(rank=10, reg_param=0.1, max_iter=10):
    return ALS(
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        coldStartStrategy="drop",   # avoids NaN predictions for unseen users/items
        implicitPrefs=False,
        seed=42,
    )


def train_eval(train_df, test_df, **kwargs):
    """Return (model, rmse) for a given ALS configuration."""
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    )
    t0 = time.time()
    model = build_als(**kwargs).fit(train_df)
    preds = model.transform(test_df)
    rmse  = evaluator.evaluate(preds)
    elapsed = time.time() - t0
    return model, rmse, elapsed


def main():
    os.makedirs(os.path.join(BASE_DIR, "models"), exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ALS Collaborative Filtering — MovieLens 1M")
    print("=" * 60)

    # ── 1. Load ───────────────────────────────────────────────────────────
    df = spark.read.parquet(DATA_PATH)
    total = df.count()
    print(f"\nLoaded {total:,} ratings")

    # ── 2. Split ──────────────────────────────────────────────────────────
    print("\nSTEP 1 — Train / Test split  (80% / 20%, seed=42)")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()
    print(f"  Train: {train_df.count():,}  |  Test: {test_df.count():,}")

    # ── 3. Initial training ───────────────────────────────────────────────
    print("\nSTEP 2 — Initial ALS training")
    print("  rank=10  |  regParam=0.10  |  maxIter=10")
    model, rmse, elapsed = train_eval(
        train_df, test_df, rank=10, reg_param=0.1, max_iter=10
    )
    print(f"  Initial RMSE : {rmse:.4f}   ({elapsed:.1f}s)")

    # ── 4. Tune if needed ─────────────────────────────────────────────────
    best_model, best_rmse = model, rmse

    if rmse > RMSE_THRESHOLD:
        print(f"\n  RMSE {rmse:.4f} > {RMSE_THRESHOLD} → hyperparameter tuning")
        print(f"  {'rank':>6}  {'regParam':>9}  {'maxIter':>8}  {'RMSE':>8}  {'time':>6}")
        print("  " + "-" * 48)

        for cfg in TUNING_GRID:
            m, r, t = train_eval(train_df, test_df, **cfg)
            marker = " ◄ best" if r < best_rmse else ""
            print(
                f"  {cfg['rank']:>6}  {cfg['reg_param']:>9.2f}  "
                f"{cfg['max_iter']:>8}  {r:>8.4f}  {t:>5.1f}s{marker}"
            )
            if r < best_rmse:
                best_rmse  = r
                best_model = m

        print(f"\n  Best RMSE after tuning: {best_rmse:.4f}")
    else:
        print(f"\n  RMSE {rmse:.4f} ≤ {RMSE_THRESHOLD} — no tuning required.")

    # ── 5. Sample recommendations ─────────────────────────────────────────
    print("\nSTEP 3 — Sample top-5 recommendations (5 random users)")
    sample_users = train_df.select("user_id").distinct().limit(5)
    recs = best_model.recommendForUserSubset(sample_users, 5)

    (
        recs
        .select("user_id", F.explode("recommendations").alias("rec"))
        .select(
            "user_id",
            F.col("rec.item_id").alias("item_id"),
            F.round("rec.rating", 3).alias("predicted_rating"),
        )
        .orderBy("user_id", F.col("predicted_rating").desc())
        .show(25, truncate=False)
    )

    # ── 6. Save ───────────────────────────────────────────────────────────
    print(f"\nSTEP 4 — Save model → {MODEL_PATH}")
    best_model.write().overwrite().save(MODEL_PATH)
    print("  Model saved.")

    # Write RMSE so Student B can reference it without re-running
    with open(RMSE_FILE, "w") as fh:
        fh.write(f"RMSE: {best_rmse:.4f}\n")
    print(f"  RMSE written → {RMSE_FILE}")

    spark.stop()
    print(f"\nFinal RMSE : {best_rmse:.4f}")
    print("ALS training complete.")


if __name__ == "__main__":
    main()
