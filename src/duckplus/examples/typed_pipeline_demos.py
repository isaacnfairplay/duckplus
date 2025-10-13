from __future__ import annotations

from datetime import date
from typing import List

import duckdb

from duckplus import AggregateExpression, DuckRel, col, ducktypes

__all__ = [
    "typed_orders_demo_relation",
    "priority_order_snapshot",
    "regional_revenue_summary",
    "apply_manual_tax_projection",
    "describe_markers",
]


def typed_orders_demo_relation(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    """Return a sample relation with column typing metadata applied."""

    base = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'north', 'Alice', 120, 5, DATE '2024-01-01', TRUE),
                (2, 'north', 'Bob', 45, 1, DATE '2024-01-02', FALSE),
                (3, 'south', 'Alice', 98, 3, DATE '2024-01-03', TRUE),
                (4, 'west', 'Cathy', 155, 2, DATE '2024-01-04', TRUE),
                (5, 'east', 'Dan', 15, 1, DATE '2024-01-05', FALSE),
                (6, 'north', 'Eve', 200, 4, DATE '2024-01-06', TRUE)
            ) AS orders(order_id, region, customer, order_total, items, placed_at, priority)
            """
        )
    )

    return base.project(
        {
            "order_id": col("order_id", duck_type=ducktypes.Integer),
            "region": col("region", duck_type=ducktypes.Varchar),
            "customer": col("customer", duck_type=ducktypes.Varchar),
            "order_total": col("order_total", duck_type=ducktypes.Integer),
            "items": col("items", duck_type=ducktypes.Integer),
            "placed_at": col("placed_at", duck_type=ducktypes.Date),
            "priority": col("priority", duck_type=ducktypes.Boolean),
        }
    )


def priority_order_snapshot(
    orders: DuckRel,
) -> List[tuple[int, str, str, int, int, date, bool]]:
    """Return high-value priority orders using typed fetch semantics."""

    filtered = orders.filter(
        (col("priority", duck_type=ducktypes.Boolean) == True)  # noqa: E712
        & (col("order_total", duck_type=ducktypes.Integer) >= 100)
    )
    ordered = filtered.order_by((col("placed_at", duck_type=ducktypes.Date), "asc"))
    return ordered.fetch_typed()


def regional_revenue_summary(
    orders: DuckRel,
) -> List[tuple[str, int, int]]:
    """Return typed grouped revenue statistics by region."""

    summary = orders.aggregate(
        col("region", duck_type=ducktypes.Varchar),
        order_count=AggregateExpression.count(col("order_id", duck_type=ducktypes.Integer)),
        total_revenue=AggregateExpression.sum(col("order_total", duck_type=ducktypes.Integer)),
    )
    ordered = summary.order_by((col("region", duck_type=ducktypes.Varchar), "asc"))
    return ordered.fetch_typed()


def apply_manual_tax_projection(orders: DuckRel) -> DuckRel:
    """Adjust order totals using raw SQL, demonstrating Unknown typing fallback."""

    return orders.transform_columns(order_total="({column}) * 1.08")


def describe_markers(orders: DuckRel) -> List[str]:
    """Return a human-readable view of the relation's column type markers."""

    return [marker.describe() for marker in orders.column_type_markers]
