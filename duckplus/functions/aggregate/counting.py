"""Counting aggregate helpers implemented as direct Python functions."""

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


_COUNT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="count",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description="Returns the number of non-NULL values in arg.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="count",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(parse_type("ANY"),),
        parameters=("arg",),
        varargs=None,
        description="Returns the number of non-NULL values in arg.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("count")
def count(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count``.

    Returns the number of non-NULL values in arg.

    Overloads:
    - main.count() -> BIGINT
    - main.count(ANY arg) -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _COUNT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("count_filter")
def count_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count`` with ``FILTER``.

    Returns the number of non-NULL values in arg.

    Overloads:
    - main.count() -> BIGINT
    - main.count(ANY arg) -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _COUNT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_COUNT_IF_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="count_if",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("BOOLEAN"),),
        parameters=("arg",),
        varargs=None,
        description="Counts the total number of TRUE values for a boolean column",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("count_if")
def count_if(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count_if``.

    Counts the total number of TRUE values for a boolean column

    Overloads:
    - main.count_if(BOOLEAN arg) -> HUGEINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _COUNT_IF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("count_if_filter")
def count_if_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count_if`` with ``FILTER``.

    Counts the total number of TRUE values for a boolean column

    Overloads:
    - main.count_if(BOOLEAN arg) -> HUGEINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _COUNT_IF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


# ``countif`` aliases ``count_if`` while maintaining legacy docstrings.
_COUNTIF_SIGNATURES = _COUNT_IF_SIGNATURES


@register_duckdb_function("countif")
def countif(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``countif``.

    Counts the total number of TRUE values for a boolean column

    Overloads:
    - main.countif(BOOLEAN arg) -> HUGEINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _COUNTIF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("countif_filter")
def countif_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``countif`` with ``FILTER``.

    Counts the total number of TRUE values for a boolean column

    Overloads:
    - main.countif(BOOLEAN arg) -> HUGEINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _COUNTIF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_COUNT_STAR_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="count_star",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description=None,
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("count_star")
def count_star(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count_star``.

    Overloads:
    - main.count_star() -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _COUNT_STAR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("count_star_filter")
def count_star_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``count_star`` with ``FILTER``.

    Overloads:
    - main.count_star() -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _COUNT_STAR_SIGNATURES,
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
    """Attach counting aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )

    namespace: Any = AggregateNumericFunctions

    namespace._COUNT_SIGNATURES = _COUNT_SIGNATURES
    namespace.count = count  # type: ignore[assignment]
    namespace.count_filter = count_filter  # type: ignore[assignment]
    namespace._register_function(
        "count",
        names=getattr(count, "__duckdb_identifiers__", ()),
        symbols=getattr(count, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "count_filter",
        names=getattr(count_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(count_filter, "__duckdb_symbols__", ()),
    )

    namespace._COUNT_IF_SIGNATURES = _COUNT_IF_SIGNATURES
    namespace.count_if = count_if  # type: ignore[assignment]
    namespace.count_if_filter = count_if_filter  # type: ignore[assignment]
    namespace._register_function(
        "count_if",
        names=getattr(count_if, "__duckdb_identifiers__", ()),
        symbols=getattr(count_if, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "count_if_filter",
        names=getattr(count_if_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(count_if_filter, "__duckdb_symbols__", ()),
    )

    namespace._COUNTIF_SIGNATURES = _COUNTIF_SIGNATURES
    namespace.countif = countif  # type: ignore[assignment]
    namespace.countif_filter = countif_filter  # type: ignore[assignment]
    namespace._register_function(
        "countif",
        names=getattr(countif, "__duckdb_identifiers__", ()),
        symbols=getattr(countif, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "countif_filter",
        names=getattr(countif_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(countif_filter, "__duckdb_symbols__", ()),
    )

    namespace._COUNT_STAR_SIGNATURES = _COUNT_STAR_SIGNATURES
    namespace.count_star = count_star  # type: ignore[assignment]
    namespace.count_star_filter = count_star_filter  # type: ignore[assignment]
    namespace._register_function(
        "count_star",
        names=getattr(count_star, "__duckdb_identifiers__", ()),
        symbols=getattr(count_star, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "count_star_filter",
        names=getattr(count_star_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(count_star_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "count",
    "count_filter",
    "count_if",
    "count_if_filter",
    "countif",
    "countif_filter",
    "count_star",
    "count_star_filter",
]
