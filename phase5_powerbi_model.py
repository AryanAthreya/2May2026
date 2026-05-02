"""
Phase 5: Power BI data modelling definitions.

This Python file documents the semantic model that should be created in Power BI
after connecting to the Fabric Lakehouse Gold tables.
"""

from __future__ import annotations


FACT_TABLE = {
    "name": "Sales",
    "source": "gold_fact_sales",
    "grain": "One row per retail transaction line",
    "columns": [
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "price",
        "revenue",
        "event_timestamp",
    ],
}

DIMENSION_TABLES = [
    {
        "name": "Customer",
        "source": "customer_data or dim_customer",
        "key": "customer_id",
        "attributes": ["name", "city", "segment"],
    },
    {
        "name": "Product",
        "source": "product_data or dim_product",
        "key": "product_id",
        "attributes": ["category", "brand"],
    },
    {
        "name": "Date",
        "source": "Power BI calculated date table",
        "key": "Date",
        "attributes": ["Year", "Month", "Month Name", "Quarter"],
    },
]

RELATIONSHIPS = [
    "Customer[customer_id] 1 -> * Sales[customer_id]",
    "Product[product_id] 1 -> * Sales[product_id]",
    "Date[Date] 1 -> * Sales[event_date]",
]

STAR_SCHEMA_DESCRIPTION = """
Sales is the central fact table. Customer, Product, and Date are dimension tables.
Filters flow from dimensions to Sales using one-to-many relationships.
This supports city, segment, category, brand, and time-series analysis without
duplicating dimension attributes in measures.
""".strip()


def print_powerbi_model() -> None:
    """Print Power BI modelling instructions."""

    print("Fact table:")
    print(FACT_TABLE)
    print("Dimension tables:")
    for table in DIMENSION_TABLES:
        print(table)
    print("Relationships:")
    for relationship in RELATIONSHIPS:
        print(relationship)
    print("Star schema:")
    print(STAR_SCHEMA_DESCRIPTION)


if __name__ == "__main__":
    print_powerbi_model()

