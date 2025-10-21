"""Decorator-backed list aggregation helpers."""

from __future__ import annotations

# pylint: disable=redefined-builtin

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


_LIST_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="list",
        function_type="aggregate",
        return_type=parse_type("T[]"),
        parameter_types=(parse_type("T"),),
        parameters=("arg",),
        varargs=None,
        description="Returns a LIST containing all the values of a column.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("list")
def list(  # noqa: A002 - matching DuckDB helper name.
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``list``.

    Returns a LIST containing all the values of a column.

    Overloads:
    - main.list(T arg) -> T[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _LIST_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("list_filter")
def list_filter(  # noqa: A002 - matching DuckDB helper name.
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``list`` with ``FILTER``.

    Returns a LIST containing all the values of a column.

    Overloads:
    - main.list(T arg) -> T[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _LIST_SIGNATURES,
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
    """Attach list aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._LIST_SIGNATURES = _LIST_SIGNATURES
    namespace.list = list  # type: ignore[assignment]
    namespace.list_filter = list_filter  # type: ignore[assignment]
    namespace._register_function(
        "list",
        names=getattr(list, "__duckdb_identifiers__", ()),
        symbols=getattr(list, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "list_filter",
        names=getattr(list_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(list_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "list",
    "list_filter",
]
