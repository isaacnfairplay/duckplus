from __future__ import annotations

from typing import Iterator

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import sales_pipeline


@pytest.fixture()
def demo_data() -> Iterator[sales_pipeline.SalesDemoData]:
    manager = DuckCon()
    with manager:
        yield sales_pipeline.load_demo_relations(manager)


def test_build_enriched_orders_adds_expected_columns(demo_data: sales_pipeline.SalesDemoData) -> None:
    orders = demo_data.orders
    returns = demo_data.returns
    enriched = sales_pipeline.build_enriched_orders(orders, returns)
    assert enriched.columns == (
        "order_id",
        "order_date",
        "region",
        "customer",
        "channel",
        "is_repeat",
        "order_total",
        "shipping_cost",
        "return_reason",
        "net_revenue",
        "tax_amount",
        "contribution",
        "is_high_value",
        "service_tier",
        "is_returned",
    )


def test_region_summary_matches_expected(demo_data: sales_pipeline.SalesDemoData) -> None:
    orders = demo_data.orders
    returns = demo_data.returns
    enriched = sales_pipeline.build_enriched_orders(orders, returns)
    summary = sales_pipeline.summarise_by_region(enriched)
    rows = summary.relation.order("region").fetchall()
    assert summary.columns == (
        "region",
        "total_orders",
        "net_revenue",
        "high_value_orders",
        "return_rate",
    )
    expected = [
        ("east", 2, pytest.approx(301.0), 1, pytest.approx(0.5)),
        ("north", 2, pytest.approx(319.5), 0, pytest.approx(0.5)),
        ("south", 2, pytest.approx(448.0), 1, pytest.approx(0.5)),
        ("west", 2, pytest.approx(440.0), 1, pytest.approx(0.0)),
    ]
    assert rows == expected


def test_run_sales_demo_returns_projection_sql() -> None:
    report = sales_pipeline.run_sales_demo()
    assert report.region_columns == (
        "region",
        "total_orders",
        "net_revenue",
        "high_value_orders",
        "return_rate",
    )
    assert report.channel_columns == (
        "channel",
        "total_orders",
        "repeat_orders",
        "average_contribution",
    )
    assert report.channel_rows == [
        ("field", 2, 1, pytest.approx(229.245)),
        ("online", 4, 1, pytest.approx(166.12125)),
        ("partner", 2, 1, pytest.approx(139.965)),
    ]
    assert len(report.preview_rows) == 5
    assert "SELECT * REPLACE" in report.projection_sql
    assert 'CASE WHEN "return_reason" IS NULL' in report.projection_sql
    assert report.projection_sql.strip().endswith("FROM enriched_orders")
