"""Decorator-backed statistical aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import NumericExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )


_SKEWNESS_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="skewness",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("DOUBLE"),),
        parameters=("x",),
        varargs=None,
        description="Returns the skewness of all input values.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("skewness")
def skewness(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``skewness``.

    Returns the skewness of all input values.

    Overloads:
    - main.skewness(DOUBLE x) -> DOUBLE
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _SKEWNESS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("skewness_filter")
def skewness_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``skewness`` with ``FILTER``.

    Returns the skewness of all input values.

    Overloads:
    - main.skewness(DOUBLE x) -> DOUBLE
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _SKEWNESS_SIGNATURES,
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
    """Attach statistical aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )

    namespace: Any = AggregateNumericFunctions

    namespace._SKEWNESS_SIGNATURES = _SKEWNESS_SIGNATURES
    namespace.skewness = skewness  # type: ignore[assignment]
    namespace.skewness_filter = skewness_filter  # type: ignore[assignment]
    namespace._register_function(
        "skewness",
        names=getattr(skewness, "__duckdb_identifiers__", ()),
        symbols=getattr(skewness, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "skewness_filter",
        names=getattr(skewness_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(skewness_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "skewness",
    "skewness_filter",
]
