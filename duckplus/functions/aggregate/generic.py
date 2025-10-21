"""Decorator-backed generic aggregate helpers."""

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


_ANY_VALUE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="any_value",
        function_type="aggregate",
        return_type=parse_type("ANY"),
        parameter_types=(parse_type("ANY"),),
        parameters=("arg",),
        varargs=None,
        description=(
            "Returns the first non-NULL value from arg. This function is affected"
            " by ordering."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("any_value")
def any_value(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``any_value``.

    Returns the first non-NULL value from arg. This function is affected by
    ordering.

    Overloads:
    - main.any_value(ANY arg) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _ANY_VALUE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("any_value_filter")
def any_value_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``any_value`` with ``FILTER``.

    Returns the first non-NULL value from arg. This function is affected by
    ordering.

    Overloads:
    - main.any_value(ANY arg) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _ANY_VALUE_SIGNATURES,
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
    """Attach generic aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._ANY_VALUE_SIGNATURES = _ANY_VALUE_SIGNATURES
    namespace.any_value = any_value  # type: ignore[assignment]
    namespace.any_value_filter = any_value_filter  # type: ignore[assignment]
    namespace._register_function(
        "any_value",
        names=getattr(any_value, "__duckdb_identifiers__", ()),
        symbols=getattr(any_value, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "any_value_filter",
        names=getattr(any_value_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(any_value_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "any_value",
    "any_value_filter",
]
