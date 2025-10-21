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


_BITWISE_INTEGER_TYPES: tuple[str, ...] = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
)


def _make_bitwise_signatures(
    function_name: str,
    description: str,
) -> tuple[DuckDBFunctionDefinition, ...]:
    return tuple(
        DuckDBFunctionDefinition(
            schema_name="main",
            function_name=function_name,
            function_type="aggregate",
            return_type=parse_type(sql_type),
            parameter_types=(parse_type(sql_type),),
            parameters=("arg",),
            varargs=None,
            description=description,
            comment=None,
            macro_definition=None,
        )
        for sql_type in _BITWISE_INTEGER_TYPES
    )


_BIT_AND_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = _make_bitwise_signatures(
    "bit_and",
    "Returns the bitwise AND of all bits in a given expression.",
)


@register_duckdb_function("bit_and")
def bit_and(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_and``.

    Returns the bitwise AND of all bits in a given expression.

    Overloads:
    - main.bit_and(TINYINT arg) -> TINYINT
    - main.bit_and(SMALLINT arg) -> SMALLINT
    - main.bit_and(INTEGER arg) -> INTEGER
    - main.bit_and(BIGINT arg) -> BIGINT
    - main.bit_and(HUGEINT arg) -> HUGEINT
    - main.bit_and(UTINYINT arg) -> UTINYINT
    - main.bit_and(USMALLINT arg) -> USMALLINT
    - main.bit_and(UINTEGER arg) -> UINTEGER
    - main.bit_and(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _BIT_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bit_and_filter")
def bit_and_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_and`` with ``FILTER``.

    Returns the bitwise AND of all bits in a given expression.

    Overloads:
    - main.bit_and(TINYINT arg) -> TINYINT
    - main.bit_and(SMALLINT arg) -> SMALLINT
    - main.bit_and(INTEGER arg) -> INTEGER
    - main.bit_and(BIGINT arg) -> BIGINT
    - main.bit_and(HUGEINT arg) -> HUGEINT
    - main.bit_and(UTINYINT arg) -> UTINYINT
    - main.bit_and(USMALLINT arg) -> USMALLINT
    - main.bit_and(UINTEGER arg) -> UINTEGER
    - main.bit_and(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BIT_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_BIT_OR_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = _make_bitwise_signatures(
    "bit_or",
    "Returns the bitwise OR of all bits in a given expression.",
)


@register_duckdb_function("bit_or")
def bit_or(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_or``.

    Returns the bitwise OR of all bits in a given expression.

    Overloads:
    - main.bit_or(TINYINT arg) -> TINYINT
    - main.bit_or(SMALLINT arg) -> SMALLINT
    - main.bit_or(INTEGER arg) -> INTEGER
    - main.bit_or(BIGINT arg) -> BIGINT
    - main.bit_or(HUGEINT arg) -> HUGEINT
    - main.bit_or(UTINYINT arg) -> UTINYINT
    - main.bit_or(USMALLINT arg) -> USMALLINT
    - main.bit_or(UINTEGER arg) -> UINTEGER
    - main.bit_or(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _BIT_OR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bit_or_filter")
def bit_or_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_or`` with ``FILTER``.

    Returns the bitwise OR of all bits in a given expression.

    Overloads:
    - main.bit_or(TINYINT arg) -> TINYINT
    - main.bit_or(SMALLINT arg) -> SMALLINT
    - main.bit_or(INTEGER arg) -> INTEGER
    - main.bit_or(BIGINT arg) -> BIGINT
    - main.bit_or(HUGEINT arg) -> HUGEINT
    - main.bit_or(UTINYINT arg) -> UTINYINT
    - main.bit_or(USMALLINT arg) -> USMALLINT
    - main.bit_or(UINTEGER arg) -> UINTEGER
    - main.bit_or(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BIT_OR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_BIT_XOR_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = _make_bitwise_signatures(
    "bit_xor",
    "Returns the bitwise XOR of all bits in a given expression.",
)


@register_duckdb_function("bit_xor")
def bit_xor(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_xor``.

    Returns the bitwise XOR of all bits in a given expression.

    Overloads:
    - main.bit_xor(TINYINT arg) -> TINYINT
    - main.bit_xor(SMALLINT arg) -> SMALLINT
    - main.bit_xor(INTEGER arg) -> INTEGER
    - main.bit_xor(BIGINT arg) -> BIGINT
    - main.bit_xor(HUGEINT arg) -> HUGEINT
    - main.bit_xor(UTINYINT arg) -> UTINYINT
    - main.bit_xor(USMALLINT arg) -> USMALLINT
    - main.bit_xor(UINTEGER arg) -> UINTEGER
    - main.bit_xor(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _BIT_XOR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("bit_xor_filter")
def bit_xor_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``bit_xor`` with ``FILTER``.

    Returns the bitwise XOR of all bits in a given expression.

    Overloads:
    - main.bit_xor(TINYINT arg) -> TINYINT
    - main.bit_xor(SMALLINT arg) -> SMALLINT
    - main.bit_xor(INTEGER arg) -> INTEGER
    - main.bit_xor(BIGINT arg) -> BIGINT
    - main.bit_xor(HUGEINT arg) -> HUGEINT
    - main.bit_xor(UTINYINT arg) -> UTINYINT
    - main.bit_xor(USMALLINT arg) -> USMALLINT
    - main.bit_xor(UINTEGER arg) -> UINTEGER
    - main.bit_xor(UBIGINT arg) -> UBIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _BIT_XOR_SIGNATURES,
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
    """Attach bitwise aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )

    namespace: Any = AggregateNumericFunctions

    namespace._BIT_AND_SIGNATURES = _BIT_AND_SIGNATURES
    namespace.bit_and = bit_and  # type: ignore[assignment]
    namespace.bit_and_filter = bit_and_filter  # type: ignore[assignment]
    namespace._register_function(
        "bit_and",
        names=getattr(bit_and, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_and, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bit_and_filter",
        names=getattr(bit_and_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_and_filter, "__duckdb_symbols__", ()),
    )

    namespace._BIT_OR_SIGNATURES = _BIT_OR_SIGNATURES
    namespace.bit_or = bit_or  # type: ignore[assignment]
    namespace.bit_or_filter = bit_or_filter  # type: ignore[assignment]
    namespace._register_function(
        "bit_or",
        names=getattr(bit_or, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_or, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bit_or_filter",
        names=getattr(bit_or_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_or_filter, "__duckdb_symbols__", ()),
    )

    namespace._BIT_XOR_SIGNATURES = _BIT_XOR_SIGNATURES
    namespace.bit_xor = bit_xor  # type: ignore[assignment]
    namespace.bit_xor_filter = bit_xor_filter  # type: ignore[assignment]
    namespace._register_function(
        "bit_xor",
        names=getattr(bit_xor, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_xor, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "bit_xor_filter",
        names=getattr(bit_xor_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(bit_xor_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "bit_and",
    "bit_and_filter",
    "bit_or",
    "bit_or_filter",
    "bit_xor",
    "bit_xor_filter",
]
