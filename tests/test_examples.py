from __future__ import annotations

import duckdb
import pytest

from duckplus import DuckRel
from duckplus.examples import aggregate_demos


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
