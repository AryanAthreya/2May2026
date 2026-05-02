"""
Phase 6: DAX measures and dashboard requirements for Power BI.

The constants below can be pasted into Power BI measures after connecting to
the Fabric Lakehouse semantic model.
"""

from __future__ import annotations


DAX_MEASURES = {
    "Total Sales": "Total Sales = SUMX(Sales, Sales[price] * Sales[quantity])",
    "Average Order Value": (
        "Average Order Value = "
        "DIVIDE([Total Sales], DISTINCTCOUNT(Sales[transaction_id]))"
    ),
    "Previous Month Sales": (
        "Previous Month Sales = "
        "CALCULATE([Total Sales], DATEADD('Date'[Date], -1, MONTH))"
    ),
    "Sales Growth %": (
        "Sales Growth % = "
        "DIVIDE([Total Sales] - [Previous Month Sales], [Previous Month Sales])"
    ),
    "Top Category": (
        "Top Category = "
        "VAR RankedCategory = "
        "TOPN(1, SUMMARIZE(Product, Product[category], \"CategorySales\", [Total Sales]), "
        "[CategorySales], DESC) "
        "RETURN CONCATENATEX(RankedCategory, Product[category], \", \")"
    ),
}

DASHBOARD_REQUIREMENTS = [
    {
        "visual": "Clustered column chart",
        "purpose": "Sales by City",
        "fields": "Customer[city], [Total Sales]",
    },
    {
        "visual": "Line chart",
        "purpose": "Sales Trend",
        "fields": "Date[Month], [Total Sales], [Sales Growth %]",
    },
    {
        "visual": "Bar chart",
        "purpose": "Top Products",
        "fields": "Product[brand], Product[category], [Total Sales]",
    },
    {
        "visual": "Donut or stacked bar chart",
        "purpose": "Customer Segmentation",
        "fields": "Customer[segment], [Total Sales]",
    },
    {
        "visual": "Cards",
        "purpose": "Executive KPIs",
        "fields": "[Total Sales], [Average Order Value], [Top Category]",
    },
]

KEY_INSIGHTS_TEMPLATE = [
    "Electronics typically drives the largest revenue because of high unit prices.",
    "Premium customers contribute strongly to high-value transactions.",
    "Monthly sales trend should be monitored for sudden drops before inventory planning.",
    "Demand spike alerts help operations respond to fast-moving products.",
]


def print_dax_and_dashboard_plan() -> None:
    """Print DAX measures and dashboard layout guidance."""

    print("DAX measures:")
    for name, expression in DAX_MEASURES.items():
        print(f"{name}: {expression}")
    print("Dashboard requirements:")
    for requirement in DASHBOARD_REQUIREMENTS:
        print(requirement)
    print("Key insights:")
    for insight in KEY_INSIGHTS_TEMPLATE:
        print(f"- {insight}")


if __name__ == "__main__":
    print_dax_and_dashboard_plan()

