"""Decorator-backed median aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import TypedExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )


_MEDIAN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="median",
        function_type="aggregate",
        return_type=parse_type("ANY"),
        parameter_types=(parse_type("ANY"),),
        parameters=("x",),
        varargs=None,
        description=(
            "Returns the middle value of the set. NULL values are ignored. For "
            "even value counts, interpolate-able types (numeric, date/time) "
            "return the average of the two middle values. Non-interpolate-able "
            "types (everything else) return the lower of the two middle values."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("median")
def median(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``median``.

    Returns the middle value of the set. NULL values are ignored. For even
    value counts, interpolate-able types (numeric, date/time) return the
    average of the two middle values. Non-interpolate-able types (everything
    else) return the lower of the two middle values.

    Overloads:
    - main.median(ANY x) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _MEDIAN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("median_filter")
def median_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``median`` with ``FILTER``.

    Returns the middle value of the set. NULL values are ignored. For even
    value counts, interpolate-able types (numeric, date/time) return the
    average of the two middle values. Non-interpolate-able types (everything
    else) return the lower of the two middle values.

    Overloads:
    - main.median(ANY x) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _MEDIAN_SIGNATURES,
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
    """Attach median aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._MEDIAN_SIGNATURES = _MEDIAN_SIGNATURES
    namespace.median = median  # type: ignore[assignment]
    namespace.median_filter = median_filter  # type: ignore[assignment]
    namespace._register_function(
        "median",
        names=getattr(median, "__duckdb_identifiers__", ()),
        symbols=getattr(median, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "median_filter",
        names=getattr(median_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(median_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "median",
    "median_filter",
]
