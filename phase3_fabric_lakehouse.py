"""
Phase 3: Microsoft Fabric Lakehouse implementation guide as executable Python.

This file does not call Microsoft Fabric APIs directly because production Fabric
access requires tenant-specific authentication, workspace IDs, and governance
approval. Instead, it captures the exact implementation plan and creates local
Bronze/Silver/Gold folders that mirror a Fabric Lakehouse layout.

Fabric mapping:
- Bronze: raw CSV/JSON files in OneLake Files.
- Silver: cleaned Delta tables with validated schema.
- Gold: aggregated tables for Power BI semantic models.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from retailx_config import DEFAULT_DATA_DIR, DEFAULT_OUTPUT_DIR, ensure_safe_child_path


FABRIC_ARCHITECTURE_STEPS = [
    "Create Microsoft Fabric workspace: RetailX Analytics.",
    "Create Lakehouse: retailx_lakehouse.",
    "Upload raw CSV files to OneLake Files/bronze.",
    "Create Spark notebook to validate and write cleaned Silver Delta tables.",
    "Create Gold tables: fact_sales, dim_customer, dim_product, agg_city_sales, agg_product_sales.",
    "Create Data Pipeline to orchestrate Bronze to Silver to Gold refresh.",
    "Create Eventstream or streaming notebook for live sales ingestion.",
    "Create Data Activator rule: alert when hourly sales drop below configured threshold.",
    "Connect Power BI semantic model to Gold Lakehouse tables.",
]


def create_lakehouse_layout(data_dir: Path, output_dir: Path) -> dict[str, Path]:
    """Create local Bronze/Silver/Gold folders and copy raw data into Bronze."""

    safe_data_dir = ensure_safe_child_path(data_dir)
    lakehouse_root = ensure_safe_child_path(output_dir) / "fabric_lakehouse"
    bronze_dir = lakehouse_root / "Files" / "bronze"
    silver_dir = lakehouse_root / "Tables" / "silver"
    gold_dir = lakehouse_root / "Tables" / "gold"

    for directory in (bronze_dir, silver_dir, gold_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for filename in ("sales_data.csv", "customer_data.csv", "product_data.csv"):
        source = safe_data_dir / filename
        if source.exists():
            shutil.copy2(source, bronze_dir / filename)

    return {"bronze": bronze_dir, "silver": silver_dir, "gold": gold_dir}


def data_activator_rule(threshold: float = 10000.0) -> dict[str, object]:
    """Return a clear Data Activator rule definition for implementation in Fabric."""

    return {
        "name": "Hourly Sales Drop Alert",
        "metric": "SUM(fact_sales.revenue)",
        "window": "1 hour",
        "condition": "less_than",
        "threshold": threshold,
        "action": "Send Teams or email notification to retail operations team",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create local Fabric Lakehouse layout.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    layout = create_lakehouse_layout(args.data_dir, args.output_dir)
    print("Fabric implementation steps:")
    for step_number, step in enumerate(FABRIC_ARCHITECTURE_STEPS, start=1):
        print(f"{step_number}. {step}")
    print("Local Lakehouse layout:")
    for layer, path in layout.items():
        print(f"{layer}: {path}")
    print(f"Data Activator rule: {data_activator_rule()}")

