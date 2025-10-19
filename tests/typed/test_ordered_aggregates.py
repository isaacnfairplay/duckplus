"""Regression tests for ordered aggregate rendering."""

from __future__ import annotations

from duckplus.typed import AGGREGATE_FUNCTIONS
from duckplus.typed.dependencies import ExpressionDependency


def col_dep(name: str, *, table: str | None = None) -> ExpressionDependency:
    """Helper for building column dependencies."""

    return ExpressionDependency.column(name, table=table)


def test_order_by_clause_appended_inside_aggregate_call() -> None:
    expression = AGGREGATE_FUNCTIONS.Generic.array_agg(
        "value",
        order_by=[{"expression": "sort_key", "direction": "DESC"}],
    )

    assert (
        expression.render()
        == 'array_agg("value" ORDER BY "sort_key" DESC)'
    )
    assert expression.dependencies == {col_dep("value"), col_dep("sort_key")}


def test_within_group_clause_rendering() -> None:
    expression = AGGREGATE_FUNCTIONS.Numeric.quantile_cont(
        "measure",
        0.5,
        within_group=[("measure", "ASC")],
    )

    assert (
        expression.render()
        == 'quantile_cont("measure", 0.5) WITHIN GROUP (ORDER BY "measure" ASC)'
    )
    assert expression.dependencies == {col_dep("measure")}


def test_window_clause_includes_partition_order_and_frame() -> None:
    expression = AGGREGATE_FUNCTIONS.Numeric.sum(
        "amount",
        partition_by=["customer"],
        over_order_by=[("order_date", "ASC")],
        frame="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    )

    assert (
        expression.render()
        == (
            'sum("amount") OVER (PARTITION BY "customer" '
            'ORDER BY "order_date" ASC ROWS BETWEEN UNBOUNDED PRECEDING '
            'AND CURRENT ROW)'
        )
    )
    assert expression.dependencies == {
        col_dep("amount"),
        col_dep("customer"),
        col_dep("order_date"),
    }


def test_filter_variant_places_window_after_filter_clause() -> None:
    expression = AGGREGATE_FUNCTIONS.Numeric.sum_filter(
        "flag",
        "amount",
        partition_by=["segment"],
        over_order_by=["event_time"],
    )

    assert (
        expression.render()
        == (
            'sum("amount") FILTER (WHERE "flag") '
            'OVER (PARTITION BY "segment" ORDER BY "event_time")'
        )
    )
    assert expression.dependencies == {
        col_dep("amount"),
        col_dep("flag"),
        col_dep("segment"),
        col_dep("event_time"),
    }
