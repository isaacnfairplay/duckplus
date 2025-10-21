"""Shared utilities for DuckDB function helper modules."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Callable, TypeVar

from duckplus.static_typed.expression import TypedExpression
from duckplus.static_typed.functions import (
    DuckDBFunctionDefinition,
    call_duckdb_filter_function,
    call_duckdb_function,
    duckdb_function,
)

_ExpressionT = TypeVar("_ExpressionT", bound=TypedExpression)


def register_duckdb_function(
    *names: str,
    symbols: Iterable[str] = (),
) -> Callable[[Callable[..., _ExpressionT]], Callable[..., _ExpressionT]]:
    """Expose the ``duckdb_function`` decorator for domain modules.

    Helpers defined under ``duckplus.functions`` should import this wrapper so
    registration stays centralised without reaching into the typed namespace
    internals directly.
    """

    return duckdb_function(*names, symbols=symbols)


def invoke_duckdb_function(
    signatures: Sequence[DuckDBFunctionDefinition],
    *,
    return_category: str,
    operands: tuple[object, ...],
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Invoke a DuckDB function helper using the provided signatures."""

    return call_duckdb_function(
        signatures,
        return_category=return_category,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


def invoke_duckdb_filter_function(
    predicate: object,
    signatures: Sequence[DuckDBFunctionDefinition],
    *,
    return_category: str,
    operands: tuple[object, ...],
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Invoke a DuckDB function helper that applies a ``FILTER`` clause."""

    return call_duckdb_filter_function(
        predicate,
        signatures,
        return_category=return_category,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


__all__ = [
    "register_duckdb_function",
    "invoke_duckdb_function",
    "invoke_duckdb_filter_function",
]
