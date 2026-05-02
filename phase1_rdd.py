"""
Phase 1: PySpark Basics and RDD processing.

Tasks covered:
- Initialize Spark Session.
- Load raw sales CSV data into an RDD.
- Map to extract product and revenue fields.
- Filter invalid records.
- reduceByKey to compute total sales per product.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from retailx_config import DEFAULT_DATA_DIR, default_spark_builder, ensure_safe_child_path


def parse_sales_csv_line(line: str) -> dict[str, object] | None:
    """Parse one CSV line into a sales dictionary, returning None for invalid rows."""

    try:
        row = next(csv.reader([line]))
        if len(row) != 7 or row[0] == "transaction_id":
            return None
        quantity = int(row[4])
        price = float(row[5])
        if not row[2] or quantity <= 0 or price < 0:
            return None
        return {
            "transaction_id": row[0],
            "customer_id": row[1],
            "product_id": row[2],
            "store_id": row[3],
            "quantity": quantity,
            "price": price,
            "timestamp": row[6],
            "revenue": quantity * price,
        }
    except (csv.Error, IndexError, TypeError, ValueError):
        return None


def total_sales_per_product(sales_csv_path: Path) -> list[tuple[str, float]]:
    """Compute product-level revenue using RDD transformations."""

    safe_path = ensure_safe_child_path(sales_csv_path)
    spark = default_spark_builder("RetailX_Phase1_RDD").getOrCreate()
    try:
        raw_rdd = spark.sparkContext.textFile(str(safe_path))
        parsed_rdd = raw_rdd.map(parse_sales_csv_line).filter(lambda row: row is not None)
        revenue_by_product_rdd = parsed_rdd.map(
            lambda row: (str(row["product_id"]), float(row["revenue"]))
        )
        return revenue_by_product_rdd.reduceByKey(lambda left, right: left + right).collect()
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RetailX Phase 1 RDD analytics.")
    parser.add_argument(
        "--sales-file",
        type=Path,
        default=DEFAULT_DATA_DIR / "sales_data.csv",
        help="Path to sales_data.csv inside the project workspace.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    for product_id, revenue in sorted(total_sales_per_product(args.sales_file)):
        print(f"{product_id},{revenue:.2f}")

