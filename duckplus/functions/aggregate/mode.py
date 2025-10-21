"""Decorator-backed mode aggregate helpers."""

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


_MODE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="mode",
        function_type="aggregate",
        return_type=parse_type("ANY"),
        parameter_types=(parse_type("ANY"),),
        parameters=("x",),
        varargs=None,
        description=(
            "Returns the most frequent value for the values within x. NULL"
            " values are ignored."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("mode")
def mode(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``mode``.

    Returns the most frequent value for the values within x. NULL values are
    ignored.

    Overloads:
    - main.mode(ANY x) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _MODE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("mode_filter")
def mode_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``mode`` with ``FILTER``.

    Returns the most frequent value for the values within x. NULL values are
    ignored.

    Overloads:
    - main.mode(ANY x) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _MODE_SIGNATURES,
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
    """Attach mode aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._MODE_SIGNATURES = _MODE_SIGNATURES
    namespace.mode = mode  # type: ignore[assignment]
    namespace.mode_filter = mode_filter  # type: ignore[assignment]
    namespace._register_function(
        "mode",
        names=getattr(mode, "__duckdb_identifiers__", ()),
        symbols=getattr(mode, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "mode_filter",
        names=getattr(mode_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(mode_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "mode",
    "mode_filter",
]
