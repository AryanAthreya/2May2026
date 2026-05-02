"""
End-to-end local runner for the RetailX capstone solution.

This executes the offline components:
1. Generate sample CSV files.
2. Run Phase 1 RDD analytics.
3. Run Phase 2 DataFrame and Spark SQL analytics.
4. Create a local Fabric Lakehouse-style folder layout.
5. Print Power BI model and DAX deliverables.

The streaming consumer is intentionally not started here because it is a
long-running process. Use phase4_realtime_streaming.py for streaming.
"""

from __future__ import annotations

from retailx_config import RetailXPathConfig, default_spark_builder
from retailx_sample_data import generate_sample_data
from phase1_rdd import total_sales_per_product
from phase2_dataframe_sql import build_joined_sales_model, run_sql_analytics, write_gold_outputs
from phase3_fabric_lakehouse import create_lakehouse_layout
from phase5_powerbi_model import print_powerbi_model
from phase6_dax_dashboard import print_dax_and_dashboard_plan


def main() -> None:
    """Run all non-streaming project phases."""

    paths = RetailXPathConfig()
    paths.ensure_directories()

    generate_sample_data(paths.data_dir)

    print("Phase 1: Total sales per product")
    for product_id, revenue in sorted(total_sales_per_product(paths.data_dir / "sales_data.csv")):
        print(f"{product_id}: {revenue:.2f}")

    print("Phase 2: DataFrame and SQL analytics")
    spark = default_spark_builder("RetailX_End_To_End").getOrCreate()
    try:
        model_df = build_joined_sales_model(spark, paths.data_dir)
        top_products_df, monthly_trend_df = run_sql_analytics(model_df)
        top_products_df.show(truncate=False)
        monthly_trend_df.show(truncate=False)
        write_gold_outputs(model_df, paths.output_dir)
    finally:
        spark.stop()

    print("Phase 3: Local Fabric Lakehouse layout")
    print(create_lakehouse_layout(paths.data_dir, paths.output_dir))

    print("Phase 5: Power BI model")
    print_powerbi_model()

    print("Phase 6: DAX and dashboard")
    print_dax_and_dashboard_plan()


if __name__ == "__main__":
    main()

