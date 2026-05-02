"""
Phase 2: DataFrames and Spark SQL.

Tasks covered:
- Read CSV files using explicit schemas.
- Clean and validate sales data.
- Convert RDD to DataFrame for parity with Phase 1.
- Filter high-value transactions.
- Aggregate revenue per city.
- Join sales, customer, and product datasets.
- Run SQL queries for top products and monthly sales trends.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format, desc, sum as spark_sum

from retailx_config import (
    DEFAULT_DATA_DIR,
    DEFAULT_OUTPUT_DIR,
    default_spark_builder,
    ensure_safe_child_path,
)
from retailx_schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, SALES_SCHEMA, clean_sales_frame


def read_csv(spark: SparkSession, path: Path, schema) -> DataFrame:
    """Read a CSV file with a strict schema."""

    return spark.read.csv(
        str(ensure_safe_child_path(path)),
        header=True,
        schema=schema,
        mode="PERMISSIVE",
    )


def build_joined_sales_model(spark: SparkSession, data_dir: Path) -> DataFrame:
    """Return the joined fact table enriched with customer and product dimensions."""

    safe_data_dir = ensure_safe_child_path(data_dir)
    sales_df = clean_sales_frame(read_csv(spark, safe_data_dir / "sales_data.csv", SALES_SCHEMA))
    customer_df = read_csv(spark, safe_data_dir / "customer_data.csv", CUSTOMER_SCHEMA)
    product_df = read_csv(spark, safe_data_dir / "product_data.csv", PRODUCT_SCHEMA)

    return (
        sales_df.join(customer_df, "customer_id", "inner")
        .join(product_df, "product_id", "inner")
        .select(
            "transaction_id",
            "customer_id",
            "product_id",
            "store_id",
            "name",
            "city",
            "segment",
            "category",
            "brand",
            "quantity",
            "price",
            "revenue",
            "event_timestamp",
        )
    )


def high_value_transactions(joined_df: DataFrame, minimum_revenue: float) -> DataFrame:
    """Return transactions whose revenue is greater than or equal to the threshold."""

    return joined_df.filter(col("revenue") >= minimum_revenue)


def total_revenue_per_city(joined_df: DataFrame) -> DataFrame:
    """Aggregate total revenue by customer city."""

    return joined_df.groupBy("city").agg(spark_sum("revenue").alias("total_revenue"))


def run_sql_analytics(joined_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Run required SQL analytics: top products and monthly trend."""

    joined_df.createOrReplaceTempView("sales_enriched")

    top_products_df = joined_df.sparkSession.sql(
        """
        SELECT
            product_id,
            category,
            brand,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales_enriched
        GROUP BY product_id, category, brand
        ORDER BY total_revenue DESC
        LIMIT 5
        """
    )

    monthly_trend_df = joined_df.withColumn(
        "sales_month", date_format(col("event_timestamp"), "yyyy-MM")
    ).groupBy("sales_month").agg(spark_sum("revenue").alias("monthly_revenue")).orderBy(
        "sales_month"
    )

    return top_products_df, monthly_trend_df


def write_gold_outputs(joined_df: DataFrame, output_dir: Path) -> None:
    """Write curated analytics tables to local Parquet as a Fabric Lakehouse stand-in."""

    safe_output_dir = ensure_safe_child_path(output_dir)
    top_products_df, monthly_trend_df = run_sql_analytics(joined_df)
    city_revenue_df = total_revenue_per_city(joined_df).orderBy(desc("total_revenue"))

    joined_df.write.mode("overwrite").parquet(str(safe_output_dir / "gold_fact_sales"))
    top_products_df.write.mode("overwrite").parquet(str(safe_output_dir / "gold_top_products"))
    monthly_trend_df.write.mode("overwrite").parquet(str(safe_output_dir / "gold_monthly_trend"))
    city_revenue_df.write.mode("overwrite").parquet(str(safe_output_dir / "gold_city_revenue"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RetailX Phase 2 DataFrame and SQL analytics.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--high-value-threshold", type=float, default=50000.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark_session = default_spark_builder("RetailX_Phase2_DataFrames_SQL").getOrCreate()
    try:
        model_df = build_joined_sales_model(spark_session, args.data_dir)
        high_value_transactions(model_df, args.high_value_threshold).show(truncate=False)
        total_revenue_per_city(model_df).show(truncate=False)
        for analytics_df in run_sql_analytics(model_df):
            analytics_df.show(truncate=False)
        write_gold_outputs(model_df, args.output_dir)
    finally:
        spark_session.stop()

