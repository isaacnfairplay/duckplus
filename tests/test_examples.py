from __future__ import annotations

from datetime import date
from typing import Iterator

import duckdb
import pytest

from duckplus import DuckConnection, DuckRel, connect
from duckplus.examples import aggregate_demos, reliability_demos, typed_pipeline_demos


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def duck_connection() -> Iterator[DuckConnection]:
    with connect() as conn:
        yield conn


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


def test_describe_schema_reports_metadata(orders_rel: DuckRel) -> None:
    report = typed_pipeline_demos.describe_schema(orders_rel)
    assert report[0] == {
        "name": "order_id",
        "duckdb_type": "INTEGER",
        "marker": "INTEGER",
        "python": "int",
    }
    assert report[-1] == {
        "name": "priority",
        "duckdb_type": "BOOLEAN",
        "marker": "BOOLEAN",
        "python": "bool",
    }


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
    schema_report = typed_pipeline_demos.describe_schema(adjusted)
    assert schema_report[3] == {
        "name": "order_total",
        "duckdb_type": "DECIMAL(13,2)",
        "marker": "UNKNOWN",
        "python": "Any",
    }


def test_schema_driven_projection(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.schema_driven_projection(orders_rel)
    assert rows == [
        (1, "north", True),
        (2, "north", False),
        (3, "south", True),
        (4, "west", True),
        (5, "east", False),
        (6, "north", True),
    ]


def test_priority_region_rollup(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.priority_region_rollup(orders_rel)
    assert rows == [
        ("east", 0, 0, 1),
        ("north", 2, 1, 10),
        ("south", 1, 0, 3),
        ("west", 1, 1, 2),
    ]


def test_customer_priority_profile(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.customer_priority_profile(orders_rel)
    assert rows == [
        ("Alice", date(2024, 1, 1), 218, 2),
        ("Bob", date(2024, 1, 2), 45, 0),
        ("Cathy", date(2024, 1, 4), 155, 1),
        ("Dan", date(2024, 1, 5), 15, 0),
        ("Eve", date(2024, 1, 6), 200, 1),
    ]


def test_regional_customer_diversity(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.regional_customer_diversity(orders_rel)
    assert rows == [
        ("east", 1, 0),
        ("north", 3, 2),
        ("south", 1, 1),
        ("west", 1, 1),
    ]


def test_daily_priority_summary(orders_rel: DuckRel) -> None:
    rows = typed_pipeline_demos.daily_priority_summary(orders_rel)
    assert rows == [
        (date(2024, 1, 1), 120, 1),
        (date(2024, 1, 2), 45, 0),
        (date(2024, 1, 3), 98, 1),
        (date(2024, 1, 4), 155, 1),
        (date(2024, 1, 5), 15, 0),
        (date(2024, 1, 6), 200, 1),
    ]


def test_priority_dispatch_payload(duck_connection: DuckConnection) -> None:
    rows = reliability_demos.priority_dispatch_payload(duck_connection)
    assert rows == [
        (6, "north", 200, True),
        (4, "west", 155, True),
    ]


def test_incremental_fact_ingest(duck_connection: DuckConnection) -> None:
    inserted, snapshot = reliability_demos.incremental_fact_ingest(duck_connection)
    assert inserted == 3
    assert snapshot == [
        (1, "north", 120),
        (2, "north", 45),
        (3, "south", 98),
        (4, "west", 155),
        (5, "east", 15),
        (6, "north", 200),
    ]


def test_customer_spike_detector(duck_connection: DuckConnection) -> None:
    rows = reliability_demos.customer_spike_detector(duck_connection)
    assert rows == [
        ("Eve", 1, 200),
        ("Cathy", 1, 155),
    ]


def test_regional_order_kpis(duck_connection: DuckConnection) -> None:
    rows = reliability_demos.regional_order_kpis(duck_connection)
    assert rows == [
        ("east", 1, 0, 15),
        ("north", 3, 2, 365),
        ("south", 1, 1, 98),
        ("west", 1, 1, 155),
    ]


def test_arrow_priority_snapshot(duck_connection: DuckConnection) -> None:
    rows = reliability_demos.arrow_priority_snapshot(duck_connection)
    assert rows == [
        (1, "Alice"),
        (4, "Cathy"),
        (6, "Eve"),
    ]


def test_lean_projection_shortcut(duck_connection: DuckConnection) -> None:
    rows = reliability_demos.lean_projection_shortcut(duck_connection)
    assert rows == [
        (1, "NORTH", "Alice"),
        (2, "NORTH", "Bob"),
        (3, "SOUTH", "Alice"),
        (4, "WEST", "Cathy"),
        (5, "EAST", "Dan"),
        (6, "NORTH", "Eve"),
    ]
