"""Decorator-backed bitstring aggregation helpers."""

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


_INTEGER_TYPES: tuple[str, ...] = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
)
_DESCRIPTION = "Returns a bitstring with bits set for each distinct value."


_PARAMETER_SETS: tuple[tuple[str, ...], ...] = (
    ("arg",),
    ("arg", "col1", "col2"),
)


_BITSTRING_AGG_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = tuple(
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="bitstring_agg",
        function_type="aggregate",
        return_type=parse_type("BIT"),
        parameter_types=tuple(parse_type(type_name) for _ in parameters),
        parameters=parameters,
        varargs=None,
        description=_DESCRIPTION,
        comment=None,
        macro_definition=None,
    )
    for parameters in _PARAMETER_SETS
    for type_name in _INTEGER_TYPES
)


_TYPES_DOC = ", ".join(_INTEGER_TYPES)
@register_duckdb_function("bitstring_agg")
def bitstring_agg(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``bitstring_agg``."""

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _BITSTRING_AGG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bitstring_agg_filter")
def bitstring_agg_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``bitstring_agg`` with ``FILTER``."""

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BITSTRING_AGG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )




_COMMON_DOC = (
    f"{_DESCRIPTION}\n\n"
    f"Overloads:\n"
    f"- main.bitstring_agg(TYPE arg) -> BIT for each TYPE in [{_TYPES_DOC}]\n"
    f"- main.bitstring_agg(TYPE arg, TYPE col1, TYPE col2) -> BIT for each TYPE in [{_TYPES_DOC}]"
)

bitstring_agg.__doc__ = (
    "Call DuckDB function ``bitstring_agg``.\n\n" + _COMMON_DOC
)
bitstring_agg_filter.__doc__ = (
    "Call DuckDB function ``bitstring_agg`` with ``FILTER``.\n\n" + _COMMON_DOC
)

def _register() -> None:
    """Attach bitstring aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._BITSTRING_AGG_SIGNATURES = _BITSTRING_AGG_SIGNATURES
    namespace.bitstring_agg = bitstring_agg  # type: ignore[assignment]
    namespace.bitstring_agg_filter = bitstring_agg_filter  # type: ignore[assignment]
    namespace._register_function(
        "bitstring_agg",
        names=getattr(bitstring_agg, "__duckdb_identifiers__", ()),
        symbols=getattr(bitstring_agg, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bitstring_agg_filter",
        names=getattr(bitstring_agg_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bitstring_agg_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "bitstring_agg",
    "bitstring_agg_filter",
]
