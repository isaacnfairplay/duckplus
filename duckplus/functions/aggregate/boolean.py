"""Boolean aggregate helpers exposed as direct Python functions."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import BooleanExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateBooleanFunctions,
    )


_BOOL_AND_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="bool_and",
        function_type="aggregate",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(parse_type("BOOLEAN"),),
        parameters=("arg",),
        varargs=None,
        description="Returns TRUE if every input value is TRUE, otherwise FALSE.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("bool_and")
def bool_and(
    self: "AggregateBooleanFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> BooleanExpression:
    """Call DuckDB function ``bool_and``.

    Returns TRUE if every input value is TRUE, otherwise FALSE.

    Overloads:
    - main.bool_and(BOOLEAN arg) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _BOOL_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bool_and_filter")
def bool_and_filter(
    self: "AggregateBooleanFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> BooleanExpression:
    """Call DuckDB function ``bool_and`` with ``FILTER``.

    Returns TRUE if every input value is TRUE, otherwise FALSE.

    Overloads:
    - main.bool_and(BOOLEAN arg) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BOOL_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_BOOL_OR_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="bool_or",
        function_type="aggregate",
        return_type=parse_type("BOOLEAN"),
        parameter_types=(parse_type("BOOLEAN"),),
        parameters=("arg",),
        varargs=None,
        description="Returns TRUE if any input value is TRUE, otherwise FALSE.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("bool_or")
def bool_or(
    self: "AggregateBooleanFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> BooleanExpression:
    """Call DuckDB function ``bool_or``.

    Returns TRUE if any input value is TRUE, otherwise FALSE.

    Overloads:
    - main.bool_or(BOOLEAN arg) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        invoke_duckdb_function(
            _BOOL_OR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bool_or_filter")
def bool_or_filter(
    self: "AggregateBooleanFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> BooleanExpression:
    """Call DuckDB function ``bool_or`` with ``FILTER``.

    Returns TRUE if any input value is TRUE, otherwise FALSE.

    Overloads:
    - main.bool_or(BOOLEAN arg) -> BOOLEAN
    """

    return cast(
        BooleanExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BOOL_OR_SIGNATURES,
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
    """Attach boolean aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateBooleanFunctions,
    )

    namespace: Any = AggregateBooleanFunctions

    namespace._BOOL_AND_SIGNATURES = _BOOL_AND_SIGNATURES
    namespace.bool_and = bool_and  # type: ignore[assignment]
    namespace.bool_and_filter = bool_and_filter  # type: ignore[assignment]
    namespace._register_function(
        "bool_and",
        names=getattr(bool_and, "__duckdb_identifiers__", ()),
        symbols=getattr(bool_and, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bool_and_filter",
        names=getattr(bool_and_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bool_and_filter, "__duckdb_symbols__", ()),
    )

    namespace._BOOL_OR_SIGNATURES = _BOOL_OR_SIGNATURES
    namespace.bool_or = bool_or  # type: ignore[assignment]
    namespace.bool_or_filter = bool_or_filter  # type: ignore[assignment]
    namespace._register_function(
        "bool_or",
        names=getattr(bool_or, "__duckdb_identifiers__", ()),
        symbols=getattr(bool_or, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bool_or_filter",
        names=getattr(bool_or_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bool_or_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "bool_and",
    "bool_and_filter",
    "bool_or",
    "bool_or_filter",
]
