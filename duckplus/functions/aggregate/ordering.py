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


_FIRST_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="first",
        function_type="aggregate",
        return_type=parse_type("ANY"),
        parameter_types=(parse_type("ANY"),),
        parameters=("arg",),
        varargs=None,
        description=(
            "Returns the first value (NULL or non-NULL) from arg. This function"
            " is affected by ordering."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("first")
def first(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``first``.

    Returns the first value (NULL or non-NULL) from arg. This function is
    affected by ordering.

    Overloads:
    - main.first(ANY arg) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _FIRST_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("first_filter")
def first_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``first`` with ``FILTER``.

    Returns the first value (NULL or non-NULL) from arg. This function is
    affected by ordering.

    Overloads:
    - main.first(ANY arg) -> ANY
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _FIRST_SIGNATURES,
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
    """Attach ordering aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._FIRST_SIGNATURES = _FIRST_SIGNATURES
    namespace.first = first  # type: ignore[assignment]
    namespace.first_filter = first_filter  # type: ignore[assignment]
    namespace._register_function(
        "first",
        names=getattr(first, "__duckdb_identifiers__", ()),
        symbols=getattr(first, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "first_filter",
        names=getattr(first_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(first_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "first",
    "first_filter",
]
