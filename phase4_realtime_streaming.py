"""
Phase 4: Real-time analytics with Spark Structured Streaming.

Use case:
- Simulate streaming sales events.
- Process events with Spark Structured Streaming.
- Detect sudden spikes in product demand.

Run producer in one terminal:
    python phase4_realtime_streaming.py produce

Run streaming detector in another terminal:
    python phase4_realtime_streaming.py consume
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql.functions import col, from_json, sum as spark_sum, window

from retailx_config import (
    DEFAULT_CHECKPOINT_DIR,
    DEFAULT_STREAM_DIR,
    default_spark_builder,
    ensure_safe_child_path,
)
from retailx_schemas import SALES_SCHEMA


STREAM_EVENTS = [
    ("C001", "P001", "S001", 1, 54999.0),
    ("C002", "P002", "S002", 4, 120.0),
    ("C003", "P001", "S003", 2, 52999.0),
    ("C004", "P001", "S004", 5, 51999.0),
    ("C005", "P004", "S005", 1, 2499.0),
]


def produce_events(stream_dir: Path, delay_seconds: float = 1.0) -> None:
    """Write JSON sales events into a directory watched by Structured Streaming."""

    safe_stream_dir = ensure_safe_child_path(stream_dir)
    safe_stream_dir.mkdir(parents=True, exist_ok=True)

    for customer_id, product_id, store_id, quantity, price in STREAM_EVENTS:
        event = {
            "transaction_id": f"RT-{uuid4().hex[:10]}",
            "customer_id": customer_id,
            "product_id": product_id,
            "store_id": store_id,
            "quantity": quantity,
            "price": price,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        event_path = safe_stream_dir / f"{event['transaction_id']}.json"
        event_path.write_text(json.dumps(event, ensure_ascii=True), encoding="utf-8")
        print(f"Wrote {event_path.name}")
        time.sleep(delay_seconds)


def consume_events(
    stream_dir: Path,
    checkpoint_dir: Path,
    demand_threshold_units: int = 5,
    processing_time: str = "10 seconds",
) -> None:
    """Run a streaming query that detects product demand spikes."""

    safe_stream_dir = ensure_safe_child_path(stream_dir)
    safe_checkpoint_dir = ensure_safe_child_path(checkpoint_dir)
    safe_stream_dir.mkdir(parents=True, exist_ok=True)
    safe_checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = default_spark_builder("RetailX_Phase4_Streaming").getOrCreate()
    try:
        raw_stream = spark.readStream.text(str(safe_stream_dir))
        parsed_stream = raw_stream.select(from_json(col("value"), SALES_SCHEMA).alias("event")).select(
            "event.*"
        )
        demand_spikes = (
            parsed_stream.withColumn("event_time", col("timestamp").cast("timestamp"))
            .withWatermark("event_time", "1 minute")
            .groupBy(window(col("event_time"), "1 minute"), col("product_id"))
            .agg(spark_sum("quantity").alias("units_sold"))
            .filter(col("units_sold") >= demand_threshold_units)
        )

        query = (
            demand_spikes.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", str(safe_checkpoint_dir / "demand_spikes"))
            .trigger(processingTime=processing_time)
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RetailX real-time streaming simulation.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    producer = subparsers.add_parser("produce")
    producer.add_argument("--stream-dir", type=Path, default=DEFAULT_STREAM_DIR)
    producer.add_argument("--delay-seconds", type=float, default=1.0)

    consumer = subparsers.add_parser("consume")
    consumer.add_argument("--stream-dir", type=Path, default=DEFAULT_STREAM_DIR)
    consumer.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    consumer.add_argument("--threshold", type=int, default=5)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.command == "produce":
        produce_events(args.stream_dir, args.delay_seconds)
    elif args.command == "consume":
        consume_events(args.stream_dir, args.checkpoint_dir, args.threshold)

