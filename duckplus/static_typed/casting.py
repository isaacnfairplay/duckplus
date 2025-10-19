"""Explicit casting helpers for the static typed expression API."""

# pylint: disable=protected-access,cyclic-import

from __future__ import annotations

from typing import Type

from duckplus.typed.types import (
    BlobType,
    BooleanType,
    DecimalType,
    DuckDBType,
    FloatingType,
    GenericType,
    IntegerType,
    NumericType,
    TemporalType,
    UnknownType,
    VarcharType,
)
from duckplus.typed.types.parser import parse_type

from .expressions import (
    BooleanExpression,
    GenericExpression,
    NumericExpression,
    TemporalExpression,
    TypedExpression,
    VarcharExpression,
)

__all__ = ["cast_expression"]


def cast_expression(
    expression: TypedExpression,
    target: object,
    *,
    try_cast: bool = False,
) -> TypedExpression:
    """Return a new expression casting ``expression`` to ``target``."""

    duck_type = _normalise_duck_type(target)
    expression_type = _expression_type_for_duck_type(duck_type)

    function = "TRY_CAST" if try_cast else "CAST"
    sql = f"{function}({expression.render()} AS {duck_type.render()})"
    return expression_type._raw(  # type: ignore[attr-defined]
        sql,
        dependencies=expression.dependencies,
        duck_type=duck_type,
    )


def _normalise_duck_type(target: object) -> DuckDBType:
    if isinstance(target, DuckDBType):
        return target
    if isinstance(target, str):
        duck_type = parse_type(target)
        if duck_type is None:
            msg = "CAST target cannot be None"
            raise TypeError(msg)
        return duck_type
    if isinstance(target, type) and issubclass(target, DuckDBType):
        return target()  # type: ignore[call-arg]
    msg = (
        "CAST targets must be DuckDB types, type strings, or DuckDBType subclasses"
    )
    raise TypeError(msg)


def _expression_type_for_duck_type(
    duck_type: DuckDBType,
) -> Type[TypedExpression]:
    if isinstance(duck_type, BooleanType):
        return BooleanExpression
    if isinstance(duck_type, VarcharType):
        return VarcharExpression
    if isinstance(duck_type, (DecimalType, IntegerType, NumericType, FloatingType)):
        return NumericExpression
    if isinstance(duck_type, TemporalType):
        return TemporalExpression
    if isinstance(duck_type, (GenericType, UnknownType, BlobType)):
        return GenericExpression
    return GenericExpression
