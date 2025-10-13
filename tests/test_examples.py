from __future__ import annotations

from datetime import date

import duckdb
import pytest

from duckplus import DuckRel
from duckplus.examples import aggregate_demos, typed_pipeline_demos


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def sales_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    return aggregate_demos.sales_demo_relation(connection)


@pytest.fixture()
def orders_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    return typed_pipeline_demos.typed_orders_demo_relation(connection)


def test_total_sales_amount(sales_rel: DuckRel) -> None:
    assert aggregate_demos.total_sales_amount(sales_rel) == 230


def test_sales_by_region(sales_rel: DuckRel) -> None:
    assert aggregate_demos.sales_by_region(sales_rel) == [
        ("east", 20),
        ("north", 110),
        ("south", 30),
        ("west", 70),
    ]


def test_regions_over_target(sales_rel: DuckRel) -> None:
    assert aggregate_demos.regions_over_target(sales_rel, minimum_total=100) == ["north"]
    assert aggregate_demos.regions_over_target(sales_rel, minimum_total=60) == [
        "north",
        "west",
    ]


def test_distinct_region_count(sales_rel: DuckRel) -> None:
    assert aggregate_demos.distinct_region_count(sales_rel) == 4


def test_filtered_total_excluding_north(sales_rel: DuckRel) -> None:
    assert aggregate_demos.filtered_total_excluding_north(sales_rel) == 120


def test_ordered_region_list(sales_rel: DuckRel) -> None:
    assert aggregate_demos.ordered_region_list(sales_rel) == [
        "west",
        "north",
        "north",
        "south",
        "east",
    ]


def test_first_sale_amount(sales_rel: DuckRel) -> None:
    assert aggregate_demos.first_sale_amount(sales_rel) == 30


def test_typed_orders_demo_relation_markers(orders_rel: DuckRel) -> None:
    assert typed_pipeline_demos.describe_markers(orders_rel) == [
        "INTEGER",
        "VARCHAR",
        "VARCHAR",
        "INTEGER",
        "INTEGER",
        "DATE",
        "BOOLEAN",
    ]


def test_priority_order_snapshot(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.priority_order_snapshot(orders_rel)
    assert rows == [
        (1, "north", "Alice", 120, 5, date(2024, 1, 1), True),
        (4, "west", "Cathy", 155, 2, date(2024, 1, 4), True),
        (6, "north", "Eve", 200, 4, date(2024, 1, 6), True),
    ]


def test_regional_revenue_summary(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.regional_revenue_summary(orders_rel)
    assert rows == [
        ("east", 1, 15),
        ("north", 3, 365),
        ("south", 1, 98),
        ("west", 1, 155),
    ]


def test_apply_manual_tax_projection_marks_unknown(orders_rel: DuckRel) -> None:
    adjusted = typed_pipeline_demos.apply_manual_tax_projection(orders_rel)
    assert typed_pipeline_demos.describe_markers(adjusted) == [
        "INTEGER",
        "VARCHAR",
        "VARCHAR",
        "UNKNOWN",
        "INTEGER",
        "DATE",
        "BOOLEAN",
    ]
