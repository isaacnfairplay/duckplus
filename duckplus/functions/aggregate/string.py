"""Decorator-backed string aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import VarcharExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateVarcharFunctions,
    )


_STRING_AGG_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="string_agg",
        function_type="aggregate",
        return_type=parse_type("VARCHAR"),
        parameter_types=(parse_type("ANY"),),
        parameters=("str",),
        varargs=None,
        description=(
            "Concatenates the column string values with an optional separator."
        ),
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="string_agg",
        function_type="aggregate",
        return_type=parse_type("VARCHAR"),
        parameter_types=(parse_type("ANY"), parse_type("VARCHAR")),
        parameters=("str", "arg"),
        varargs=None,
        description=(
            "Concatenates the column string values with an optional separator."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("string_agg")
def string_agg(
    self: "AggregateVarcharFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> VarcharExpression:
    """Call DuckDB function ``string_agg``.

    Concatenates the column string values with an optional separator.

    Overloads:
    - main.string_agg(ANY str) -> VARCHAR
    - main.string_agg(ANY str, VARCHAR arg) -> VARCHAR
    """

    return cast(
        VarcharExpression,
        invoke_duckdb_function(
            _STRING_AGG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("string_agg_filter")
def string_agg_filter(
    self: "AggregateVarcharFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> VarcharExpression:
    """Call DuckDB function ``string_agg`` with ``FILTER``.

    Concatenates the column string values with an optional separator.

    Overloads:
    - main.string_agg(ANY str) -> VARCHAR
    - main.string_agg(ANY str, VARCHAR arg) -> VARCHAR
    """

    return cast(
        VarcharExpression,
        invoke_duckdb_filter_function(
            predicate,
            _STRING_AGG_SIGNATURES,
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
    """Attach string aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateVarcharFunctions,
    )

    namespace: Any = AggregateVarcharFunctions

    namespace._STRING_AGG_SIGNATURES = _STRING_AGG_SIGNATURES
    namespace.string_agg = string_agg  # type: ignore[assignment]
    namespace.string_agg_filter = string_agg_filter  # type: ignore[assignment]
    namespace._register_function(
        "string_agg",
        names=getattr(string_agg, "__duckdb_identifiers__", ()),
        symbols=getattr(string_agg, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "string_agg_filter",
        names=getattr(string_agg_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(string_agg_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "string_agg",
    "string_agg_filter",
]
