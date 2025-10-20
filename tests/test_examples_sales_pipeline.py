from __future__ import annotations

from typing import Iterator

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import sales_pipeline
from duckplus.relation import Relation


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
    assert 'CASE WHEN ("return_reason" IS NULL)' in report.projection_sql
    assert report.projection_sql.strip().endswith("FROM enriched_orders")


def _fan_out_sales_demo(
    demo_data: sales_pipeline.SalesDemoData, copies: int
) -> sales_pipeline.SalesDemoData:
    duckcon = demo_data.orders.duckcon
    connection = duckcon.connection

    orders_sql = demo_data.orders.relation.sql_query()
    returns_sql = demo_data.returns.relation.sql_query()

    order_step_row = connection.sql(
        f"WITH base AS ({orders_sql}) SELECT MAX(order_id) FROM base"
    ).fetchone()
    order_step = int(order_step_row[0]) + 1 if order_step_row and order_step_row[0] else 1

    expanded_orders = connection.sql(
        f"""
        WITH base AS ({orders_sql})
        SELECT
            order_id + ({order_step} * copy_index) AS order_id,
            order_date,
            region,
            customer,
            order_total,
            shipping_cost,
            channel,
            is_repeat
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_returns = connection.sql(
        f"""
        WITH base AS ({returns_sql})
        SELECT
            returned_order_id + ({order_step} * copy_index) AS returned_order_id,
            returned_at,
            return_reason
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    return sales_pipeline.SalesDemoData(
        orders=Relation.from_relation(duckcon, expanded_orders),
        returns=Relation.from_relation(duckcon, expanded_returns),
    )


def test_sales_demo_scales_with_large_dataset(
    demo_data: sales_pipeline.SalesDemoData,
) -> None:
    base_enriched = sales_pipeline.build_enriched_orders(
        demo_data.orders, demo_data.returns
    )
    base_region_rows = (
        sales_pipeline.summarise_by_region(base_enriched)
        .order_by("region")
        .relation.fetchall()
    )
    base_channel_rows = (
        sales_pipeline.summarise_by_channel(base_enriched)
        .order_by("channel")
        .relation.fetchall()
    )

    copies = 25
    expanded_demo = _fan_out_sales_demo(demo_data, copies)
    expanded_enriched = sales_pipeline.build_enriched_orders(
        expanded_demo.orders, expanded_demo.returns
    )

    assert expanded_enriched.row_count() == base_enriched.row_count() * copies

    expanded_region_rows = (
        sales_pipeline.summarise_by_region(expanded_enriched)
        .order_by("region")
        .relation.fetchall()
    )
    expanded_channel_rows = (
        sales_pipeline.summarise_by_channel(expanded_enriched)
        .order_by("channel")
        .relation.fetchall()
    )

    for base_row, expanded_row in zip(base_region_rows, expanded_region_rows, strict=True):
        region, base_total, base_net, base_high, base_return_rate = base_row
        (
            expanded_region,
            expanded_total,
            expanded_net,
            expanded_high,
            expanded_return_rate,
        ) = expanded_row
        assert expanded_region == region
        assert expanded_total == base_total * copies
        assert expanded_net == pytest.approx(base_net * copies)
        assert expanded_high == base_high * copies
        assert expanded_return_rate == pytest.approx(base_return_rate)

    for base_row, expanded_row in zip(
        base_channel_rows, expanded_channel_rows, strict=True
    ):
        channel, base_total, base_repeat, base_avg = base_row
        (
            expanded_channel,
            expanded_total,
            expanded_repeat,
            expanded_avg,
        ) = expanded_row
        assert expanded_channel == channel
        assert expanded_total == base_total * copies
        assert expanded_repeat == base_repeat * copies
        assert expanded_avg == pytest.approx(base_avg)
