"""
Student A — Task 3: Kafka Producer
Topic   : movie-ratings  (2 partitions)
Format  : {"user_id": int, "item_id": int, "rating": float, "timestamp": str}

Partitioning strategy:
  Partition = user_id % 2
  Rationale: keeps all events for a given user on the same partition,
  enabling per-user ordered processing and efficient user-level aggregations
  in the downstream Spark consumer.

Spike injection:
  Every ~10 events a designated "trending" item receives a high rating (≥ 4.5).
  This lets the alert system (Student B) detect items that cross the 4.5 threshold.

Usage:
  python 3_kafka_producer.py                   # defaults: 5 ev/s, forever
  python 3_kafka_producer.py --rate 10 --duration 120
  python 3_kafka_producer.py --spike-items 120 356 589
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ─────────────────────────────────────────────────────────────
# Override via env var when running inside Docker: KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = "movie-ratings"
NUM_PARTITIONS    = 2

# MovieLens 1M user/item ranges
USER_ID_MIN, USER_ID_MAX = 1, 6040
ITEM_ID_MIN, ITEM_ID_MAX = 1, 3952

# MovieLens uses half-star ratings
VALID_RATINGS = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
HIGH_RATINGS  = [4.5, 5.0]

# Default items to spike (simulate trending / alert trigger)
DEFAULT_SPIKE_ITEMS = [120, 356, 589, 1197]
SPIKE_PROBABILITY   = 0.10


# ── Producer helpers ──────────────────────────────────────────────────────────

def create_producer(retries: int = 5, retry_delay: float = 3.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",          # wait for broker acknowledgment
                retries=3,
                linger_ms=5,         # small batching window for throughput
            )
            # Trigger a real connection by fetching metadata
            producer.bootstrap_connected()
            print(f"  Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            if attempt < retries:
                print(f"  Kafka not ready (attempt {attempt}/{retries}) — "
                      f"retrying in {retry_delay}s ...")
                time.sleep(retry_delay)
            else:
                print("ERROR: Cannot reach Kafka. Is docker compose up?")
                sys.exit(1)


def make_event(user_id: Optional[int] = None,
               item_id: Optional[int] = None,
               rating: Optional[float] = None) -> dict:
    uid = user_id or random.randint(USER_ID_MIN, USER_ID_MAX)
    iid = item_id or random.randint(ITEM_ID_MIN, ITEM_ID_MAX)
    r   = rating  or random.choice(VALID_RATINGS)
    return {
        "user_id":   uid,
        "item_id":   iid,
        "rating":    r,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def partition_for(user_id: int) -> int:
    """Route all events for the same user to the same partition."""
    return user_id % NUM_PARTITIONS


# ── Main loop ─────────────────────────────────────────────────────────────────

def produce(rate_per_sec: int, duration_sec: Optional[int], spike_items: List[int]):
    delay = 1.0 / max(rate_per_sec, 1)

    print("=" * 60)
    print(f"Kafka Producer — topic: {TOPIC}")
    print(f"  Rate      : {rate_per_sec} events/sec")
    print(f"  Duration  : {'∞' if duration_sec is None else f'{duration_sec}s'}")
    print(f"  Partitions: {NUM_PARTITIONS}  (user_id % {NUM_PARTITIONS})")
    print(f"  Spike items (simulated trending): {spike_items}")
    print("=" * 60)

    producer = create_producer()
    sent = 0
    errors = 0
    start = time.time()

    def on_error(exc):
        nonlocal errors
        errors += 1
        print(f"\n  SEND ERROR: {exc}")

    try:
        while True:
            # 10% chance to inject a trending spike on a designated item
            if spike_items and random.random() < SPIKE_PROBABILITY:
                event = make_event(
                    item_id=random.choice(spike_items),
                    rating=random.choice(HIGH_RATINGS),
                )
            else:
                event = make_event()

            partition = partition_for(event["user_id"])

            (
                producer
                .send(TOPIC, key=event["user_id"], value=event, partition=partition)
                .add_errback(on_error)
            )

            sent += 1
            if sent % 100 == 0:
                elapsed = time.time() - start
                throughput = sent / elapsed
                print(
                    f"  [{elapsed:6.1f}s]  sent={sent:,}  errors={errors}"
                    f"  throughput={throughput:.1f} ev/s"
                    f"  last → user={event['user_id']} "
                    f"item={event['item_id']} rating={event['rating']}"
                )

            # Flush every 200 events to bound memory
            if sent % 200 == 0:
                producer.flush()

            if duration_sec and (time.time() - start) >= duration_sec:
                break

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nInterrupted by user.")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start
        print(f"\nDone — sent {sent:,} events in {elapsed:.1f}s  ({sent/elapsed:.1f} ev/s)")
        print(f"       errors: {errors}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Movie-ratings Kafka producer for mini_proj3"
    )
    parser.add_argument(
        "--rate", type=int, default=5,
        help="Events per second (default: 5)"
    )
    parser.add_argument(
        "--duration", type=int, default=None,
        help="Stop after N seconds (default: run until Ctrl+C)"
    )
    parser.add_argument(
        "--spike-items", type=int, nargs="+", default=DEFAULT_SPIKE_ITEMS,
        help=f"Item IDs for spike injection (default: {DEFAULT_SPIKE_ITEMS})"
    )
    args = parser.parse_args()
    produce(args.rate, args.duration, args.spike_items)


if __name__ == "__main__":
    main()
