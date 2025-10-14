from __future__ import annotations

from datetime import date
from typing import Any, List, cast

import duckdb

from duckplus import AggregateExpression, Relation, col, ducktypes
from duckplus.schema import AnyRow
from duckplus.filters import ColumnDuckType

__all__ = [
    "typed_orders_demo_relation",
    "priority_order_snapshot",
    "regional_revenue_summary",
    "priority_region_rollup",
    "customer_priority_profile",
    "regional_customer_diversity",
    "daily_priority_summary",
    "schema_driven_projection",
    "apply_manual_tax_projection",
    "describe_schema",
    "describe_markers",
]


def _schema_expression(orders: Relation[AnyRow], name: str) -> Any:
    """Return a column expression derived from the relation's schema."""

    definition = orders.schema.column(name)
    marker = definition.duck_type
    if marker is ducktypes.Unknown:
        return col(definition.name)
    typed_marker = cast(ColumnDuckType, marker)
    return col(definition.name, duck_type=typed_marker)


def _python_type_repr(value: object) -> str:
    """Return a readable representation for stored Python annotations."""

    if isinstance(value, type):
        return value.__name__
    return str(value)


def typed_orders_demo_relation(
    connection: duckdb.DuckDBPyConnection,
) -> Relation[AnyRow]:
    """Return a sample relation with column typing metadata applied."""

    base: Relation[AnyRow] = Relation(
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

    return base.select(
        order_id=col("order_id", duck_type=ducktypes.Integer),
        region=col("region", duck_type=ducktypes.Varchar),
        customer=col("customer", duck_type=ducktypes.Varchar),
        order_total=col("order_total", duck_type=ducktypes.Integer),
        items=col("items", duck_type=ducktypes.Integer),
        placed_at=col("placed_at", duck_type=ducktypes.Date),
        priority=col("priority", duck_type=ducktypes.Boolean),
    )


def priority_order_snapshot(
    orders: Relation[AnyRow],
) -> List[tuple[int, str, str, int, int, date, bool]]:
    """Return high-value priority orders using typed fetch semantics."""

    filtered = orders.where(
        lambda c: (c["priority"] == True) & (c["order_total"] >= 100)  # noqa: E712
    )
    ordered = filtered.order_by(lambda c: c["placed_at"].asc())
    return ordered.fetch_typed()


def regional_revenue_summary(
    orders: Relation[AnyRow],
) -> List[tuple[str, int, int]]:
    """Return typed grouped revenue statistics by region."""

    region = _schema_expression(orders, "region")
    order_total = _schema_expression(orders, "order_total")
    summary = orders.aggregate(
        region,
        order_count=AggregateExpression.count(),
        total_revenue=AggregateExpression.sum(order_total),
    )
    ordered = summary.order_by((region, "asc"))
    return ordered.fetch_typed()


def priority_region_rollup(
    orders: Relation[AnyRow],
) -> List[tuple[str, int, int, int]]:
    """Return typed metrics that highlight regional priority order behavior."""

    region = _schema_expression(orders, "region")
    total = _schema_expression(orders, "order_total")
    items = _schema_expression(orders, "items")
    priority = _schema_expression(orders, "priority")

    prepared = orders.select("region", order_total=total, items=items, priority=priority)

    summary = prepared.aggregate(
        region,
        priority_orders=AggregateExpression.count().with_filter(priority == True),  # noqa: E712
        high_value_orders=AggregateExpression.count().with_filter(total >= 150),
        total_items=AggregateExpression.sum(items),
    )
    ordered = summary.order_by((region, "asc"))
    return ordered.fetch_typed()


def customer_priority_profile(
    orders: Relation[AnyRow],
) -> List[tuple[str, date, int, int]]:
    """Summarise customer behaviour including first purchase and priority counts."""

    customer = _schema_expression(orders, "customer")
    placed_at = _schema_expression(orders, "placed_at")
    total = _schema_expression(orders, "order_total")
    priority = _schema_expression(orders, "priority")

    summary = orders.aggregate(
        customer,
        first_purchase=AggregateExpression.min(placed_at),
        lifetime_value=AggregateExpression.sum(total),
        priority_orders=AggregateExpression.count().with_filter(priority == True),  # noqa: E712
    )
    ordered = summary.order_by((customer, "asc"))
    return ordered.fetch_typed()


def regional_customer_diversity(
    orders: Relation[AnyRow],
) -> List[tuple[str, int, int]]:
    """Report distinct and priority customers per region with typed results."""

    region = _schema_expression(orders, "region")
    customer = _schema_expression(orders, "customer")
    priority = _schema_expression(orders, "priority")

    summary = orders.aggregate(
        region,
        distinct_customers=AggregateExpression.count(customer).distinct(),
        priority_customers=AggregateExpression.count(customer)
        .with_filter(priority == True)  # noqa: E712
        .distinct(),
    )
    ordered = summary.order_by((region, "asc"))
    return ordered.fetch_typed()


def daily_priority_summary(
    orders: Relation[AnyRow],
) -> List[tuple[date, int, int]]:
    """Track daily revenue alongside the number of flagged priority orders."""

    placed_at = _schema_expression(orders, "placed_at")
    total = _schema_expression(orders, "order_total")
    priority = _schema_expression(orders, "priority")

    summary = orders.aggregate(
        placed_at,
        daily_revenue=AggregateExpression.sum(total),
        priority_orders=AggregateExpression.count().with_filter(priority == True),  # noqa: E712
    )
    ordered = summary.order_by((placed_at, "asc"))
    return ordered.fetch_typed()


def schema_driven_projection(
    orders: Relation[AnyRow],
) -> List[tuple[int, str, bool]]:
    """Return a typed projection driven entirely by the stored schema metadata."""

    subset = orders.schema.select(["order_id", "region", "priority"])
    projection = {
        definition.name: _schema_expression(orders, definition.name)
        for definition in subset.definitions
    }
    projected = orders.select(**projection)
    return projected.fetch_typed()


def apply_manual_tax_projection(orders: Relation[AnyRow]) -> Relation[AnyRow]:
    """Adjust order totals using a computed expression with Unknown typing fallback."""

    return orders.mutate(order_total=lambda c: c["order_total"] * 1.08)


def describe_schema(orders: Relation[AnyRow]) -> List[dict[str, str]]:
    """Return a human-readable view of the relation's column type metadata."""

    schema = orders.schema
    report: List[dict[str, str]] = []
    for definition in schema.definitions:
        report.append(
            {
                "name": definition.name,
                "duckdb_type": definition.duckdb_type,
                "marker": definition.duck_type.describe(),
                "python": _python_type_repr(definition.python_type),
            }
        )
    return report


def describe_markers(orders: Relation[AnyRow]) -> List[str]:
    """Return DuckDB marker descriptions for backward-compatible demos."""

    return [entry["marker"] for entry in describe_schema(orders)]
