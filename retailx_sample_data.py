"""
Generate small sample datasets for the RetailX capstone.

Use this when the assessment CSV files are not already present locally.
The generated files match the required columns:
- sales_data.csv
- customer_data.csv
- product_data.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from retailx_config import DEFAULT_DATA_DIR, ensure_safe_child_path


CUSTOMERS = [
    ("C001", "Aarav Sharma", "Mumbai", "Premium"),
    ("C002", "Diya Patel", "Ahmedabad", "Value"),
    ("C003", "Kabir Singh", "Delhi", "Premium"),
    ("C004", "Meera Nair", "Bengaluru", "Standard"),
    ("C005", "Ishaan Reddy", "Hyderabad", "Standard"),
    ("C006", "Ananya Gupta", "Pune", "Value"),
]

PRODUCTS = [
    ("P001", "Electronics", "VoltEdge"),
    ("P002", "Grocery", "DailyKart"),
    ("P003", "Fashion", "UrbanThread"),
    ("P004", "Home", "CasaCraft"),
    ("P005", "Beauty", "GlowUp"),
]

SALES = [
    ("T001", "C001", "P001", "S001", 1, 54999.0, "2026-01-04 10:15:00"),
    ("T002", "C002", "P002", "S002", 8, 120.0, "2026-01-05 12:20:00"),
    ("T003", "C003", "P003", "S003", 2, 1499.0, "2026-02-06 18:35:00"),
    ("T004", "C004", "P001", "S004", 1, 59999.0, "2026-02-10 09:05:00"),
    ("T005", "C005", "P004", "S005", 3, 2499.0, "2026-03-12 14:55:00"),
    ("T006", "C006", "P005", "S006", 4, 799.0, "2026-03-14 16:10:00"),
    ("T007", "C001", "P002", "S001", 10, 115.0, "2026-04-01 11:00:00"),
    ("T008", "C003", "P001", "S003", 2, 52999.0, "2026-04-09 19:25:00"),
    ("T009", "C004", "P003", "S004", 1, 2999.0, "2026-04-11 20:30:00"),
    ("T010", "C005", "P004", "S005", 5, 1999.0, "2026-05-01 13:45:00"),
    ("T011", "C006", "P005", "S006", 0, 899.0, "2026-05-01 15:05:00"),
    ("T012", "C002", "P002", "S002", 6, -50.0, "2026-05-01 15:10:00"),
]


def write_csv(path: Path, headers: list[str], rows: list[tuple[object, ...]]) -> None:
    """Write a CSV file using UTF-8 and deterministic newline handling."""

    safe_path = ensure_safe_child_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    with safe_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        writer.writerows(rows)


def generate_sample_data(data_dir: Path = DEFAULT_DATA_DIR) -> None:
    """Generate all sample datasets."""

    safe_data_dir = ensure_safe_child_path(data_dir)
    write_csv(
        safe_data_dir / "customer_data.csv",
        ["customer_id", "name", "city", "segment"],
        CUSTOMERS,
    )
    write_csv(
        safe_data_dir / "product_data.csv",
        ["product_id", "category", "brand"],
        PRODUCTS,
    )
    write_csv(
        safe_data_dir / "sales_data.csv",
        [
            "transaction_id",
            "customer_id",
            "product_id",
            "store_id",
            "quantity",
            "price",
            "timestamp",
        ],
        SALES,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate RetailX sample CSV data.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_sample_data(args.data_dir)
    print(f"Sample data written to {args.data_dir.resolve()}")

