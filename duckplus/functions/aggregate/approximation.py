"""Approximation-focused aggregate helpers exposed as direct Python methods."""

# pylint: disable=too-many-arguments,protected-access,import-outside-toplevel

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.typed.expression import NumericExpression
from duckplus.typed.functions import (
    DuckDBFunctionDefinition,
    call_duckdb_filter_function,
    call_duckdb_function,
    duckdb_function,
)
from duckplus.typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.typed._generated_function_namespaces import AggregateNumericFunctions


_APPROX_COUNT_DISTINCT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_count_distinct",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(parse_type("ANY"),),
        parameters=("any",),
        varargs=None,
        description="Computes the approximate count of distinct elements using HyperLogLog.",
        comment=None,
        macro_definition=None,
    ),
)


@duckdb_function("approx_count_distinct")
def approx_count_distinct(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_count_distinct``.

    Computes the approximate count of distinct elements using HyperLogLog.

    Overloads:
    - main.approx_count_distinct(ANY any) -> BIGINT
    """

    return cast(
        NumericExpression,
        call_duckdb_function(
            _APPROX_COUNT_DISTINCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@duckdb_function("approx_count_distinct_filter")
def approx_count_distinct_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_count_distinct`` with ``FILTER``.

    Computes the approximate count of distinct elements using HyperLogLog.

    Overloads:
    - main.approx_count_distinct(ANY any) -> BIGINT
    """

    return cast(
        NumericExpression,
        call_duckdb_filter_function(
            predicate,
            _APPROX_COUNT_DISTINCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


def _register() -> None:
    """Attach approximation helpers onto the aggregate numeric namespace."""

    from duckplus.typed._generated_function_namespaces import AggregateNumericFunctions

    namespace: Any = AggregateNumericFunctions

    namespace._APPROX_COUNT_DISTINCT_SIGNATURES = _APPROX_COUNT_DISTINCT_SIGNATURES
    namespace.approx_count_distinct = approx_count_distinct  # type: ignore[assignment]
    namespace.approx_count_distinct_filter = (
        approx_count_distinct_filter  # type: ignore[assignment]
    )
    namespace._register_function(
        "approx_count_distinct",
        names=getattr(approx_count_distinct, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_count_distinct, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "approx_count_distinct_filter",
        names=getattr(approx_count_distinct_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_count_distinct_filter, "__duckdb_symbols__", ()),
    )


_register()

__all__ = [
    "approx_count_distinct",
    "approx_count_distinct_filter",
]
