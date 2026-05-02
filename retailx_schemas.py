"""
Spark schemas and validation helpers for RetailX datasets.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SALES_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("timestamp", StringType(), False),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("segment", StringType(), False),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("brand", StringType(), False),
    ]
)


def clean_sales_frame(sales_df: DataFrame) -> DataFrame:
    """
    Return valid sales rows with typed timestamp and computed revenue.

    Invalid rows are filtered out:
    - missing identifiers
    - non-positive quantity
    - negative price
    - unparsable timestamp
    """

    typed_df = sales_df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"))
    ).withColumn("revenue", col("quantity") * col("price"))

    return typed_df.filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("store_id").isNotNull()
        & (col("quantity") > 0)
        & (col("price") >= 0)
        & col("event_timestamp").isNotNull()
    )

