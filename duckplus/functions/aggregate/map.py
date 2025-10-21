from __future__ import annotations

# pylint: disable=redefined-builtin

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import invoke_duckdb_function, register_duckdb_function
from duckplus.static_typed.expression import TypedExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )


_MAP_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="map",
        function_type="aggregate",
        return_type=parse_type('MAP("NULL", "NULL")'),
        parameter_types=(),
        parameters=(),
        varargs=None,
        description="Creates a map from a set of keys and values",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="map",
        function_type="aggregate",
        return_type=parse_type("MAP(K, V)"),
        parameter_types=(parse_type("K[]"), parse_type("V[]")),
        parameters=("keys", "values"),
        varargs=None,
        description="Creates a map from a set of keys and values",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("map")
def map(  # noqa: A002 - matching DuckDB helper name.
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``map``.

    Creates a map from a set of keys and values

    Overloads:
    - main.map() -> MAP("NULL", "NULL")
    - main.map(K[] keys, V[] values) -> MAP(K, V)
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _MAP_SIGNATURES,
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
    """Attach map aggregate helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._MAP_SIGNATURES = _MAP_SIGNATURES
    namespace.map = map  # type: ignore[assignment]
    namespace._register_function(
        "map",
        names=getattr(map, "__duckdb_identifiers__", ()),
        symbols=getattr(map, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "map",
]
