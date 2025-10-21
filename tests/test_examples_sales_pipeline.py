from __future__ import annotations

from typing import Iterator

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import sales_pipeline
from duckplus.static_typed import ducktype


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
    if copies <= 0:
        msg = "copies must be a positive integer"
        raise ValueError(msg)

    duckcon = demo_data.orders.duckcon

    max_order_id_relation = (
        demo_data.orders.aggregate()
        .start_agg()
        .agg(ducktype.Numeric.Aggregate.max("order_id"), alias="max_order_id")
        .all()
    )
    max_order_row = max_order_id_relation.relation.fetchone()
    max_order_id = int(max_order_row[0]) if max_order_row and max_order_row[0] else 0
    order_step = max_order_id + 1 if max_order_id else 1

    expanded_orders = demo_data.orders
    expanded_returns = demo_data.returns

    for index in range(1, copies):
        order_offset = ducktype.Numeric.literal(order_step * index)
        orders_copy = (
            demo_data.orders.select()
            .star(
                replace={
                    "order_id": ducktype.Numeric("order_id") + order_offset,
                }
            )
            .from_()
        )
        expanded_orders = expanded_orders.union(orders_copy)

        returns_copy = (
            demo_data.returns.select()
            .star(
                replace={
                    "returned_order_id": (
                        ducktype.Numeric("returned_order_id") + order_offset
                    ),
                }
            )
            .from_()
        )
        expanded_returns = expanded_returns.union(returns_copy)

    return sales_pipeline.SalesDemoData(
        orders=expanded_orders,
        returns=expanded_returns,
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
