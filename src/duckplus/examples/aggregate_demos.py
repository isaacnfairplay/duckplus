"""Demonstrations of DuckRel.aggregate and AggregateExpression helpers."""

from __future__ import annotations

from numbers import Integral
from typing import List, Sequence

import duckdb

from duckplus import AggregateExpression, DuckRel, FilterExpression, col


def sales_demo_relation(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    """Return a sample sales relation used across aggregate demonstrations."""

    return DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('north', 50, DATE '2024-01-03'),
                ('north', 60, DATE '2024-01-02'),
                ('south', 30, DATE '2024-01-01'),
                ('east', 20, DATE '2024-01-04'),
                ('west', 70, DATE '2024-01-05')
            ) AS t(region, amount, sale_date)
            """
        )
    )


def total_sales_amount(sales: DuckRel) -> int:
    """Return the sum of the ``amount`` column across all sales rows."""

    summary = sales.aggregate(total_amount=AggregateExpression.sum("amount"))
    return _coerce_integral(_fetch_single_value(summary))


def sales_by_region(sales: DuckRel) -> List[tuple[str, int]]:
    """Return grouped totals ordered alphabetically by ``region``."""

    grouped = sales.aggregate("region", total_amount=AggregateExpression.sum("amount"))
    ordered = grouped.order_by(region="asc")
    rows = ordered.relation.fetchall()
    return [(str(region), int(total)) for region, total in rows]


def regions_over_target(sales: DuckRel, *, minimum_total: int) -> List[str]:
    """Return regions whose aggregated ``amount`` exceeds ``minimum_total``."""

    grouped = sales.aggregate(
        "region",
        total_amount=AggregateExpression.sum("amount"),
        having_expressions=[
            FilterExpression.raw(f'SUM("amount") > {int(minimum_total)}')
        ],
    )
    ordered = grouped.order_by(region="asc")
    return [str(region) for region, _ in ordered.relation.fetchall()]


def distinct_region_count(sales: DuckRel) -> int:
    """Return the number of distinct regions present in the sales data."""

    summary = sales.aggregate(
        unique_regions=AggregateExpression.count("region", distinct=True)
    )
    return _coerce_integral(_fetch_single_value(summary))


def filtered_total_excluding_north(sales: DuckRel) -> int:
    """Return the total ``amount`` excluding sales where the region is ``north``."""

    filtered_sum = AggregateExpression.sum("amount").with_filter(col("region") != "north")
    summary = sales.aggregate(total_excluding_north=filtered_sum)
    return _coerce_integral(_fetch_single_value(summary))


def ordered_region_list(sales: DuckRel) -> List[str]:
    """Return a list of regions sorted by ``amount`` in descending order."""

    ordered_list = AggregateExpression.function("list", "region").with_order_by(
        ("amount", "desc")
    )
    summary = sales.aggregate(region_rollup=ordered_list)
    values = _fetch_single_value(summary)
    return [str(region) for region in _coerce_sequence(values)]


def first_sale_amount(sales: DuckRel) -> int:
    """Return the ``amount`` from the earliest sale by ``sale_date``."""

    first_amount = AggregateExpression.function("first", "amount").with_order_by(
        ("sale_date", "asc")
    )
    summary = sales.aggregate(first_sale=first_amount)
    return _coerce_integral(_fetch_single_value(summary))


def _fetch_single_value(relation: DuckRel) -> object:
    """Return the first column of the first row for *relation* or raise."""

    row = relation.relation.fetchone()
    if row is None:
        raise RuntimeError("Aggregate relation did not produce any rows.")
    return row[0]


def _coerce_sequence(value: object) -> Sequence[object]:
    """Return *value* as a sequence for list-oriented aggregates."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    raise TypeError(
        "Expected aggregate to return a sequence of values; "
        f"received {type(value).__name__}."
    )


def _coerce_integral(value: object) -> int:
    """Return *value* as an ``int`` when it implements the integral protocol."""

    if isinstance(value, Integral):
        return int(value)
    raise TypeError(
        "Expected aggregate to return an integral value; "
        f"received {type(value).__name__}."
    )
