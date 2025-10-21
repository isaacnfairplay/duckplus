"""Helpers for casting typed expressions to new DuckDB types."""

# pylint: disable=cyclic-import

from __future__ import annotations

from typing import Type

from ..types import (
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
from ..types.parser import parse_type
from .base import TypedExpression


def cast_expression(
    expression: TypedExpression,
    target: object,
    *,
    try_cast: bool = False,
) -> TypedExpression:
    """Return a new typed expression casting ``expression`` to ``target``."""

    duck_type = _normalise_duck_type(target)
    expression_type = _expression_type_for_duck_type(duck_type)

    function = "TRY_CAST" if try_cast else "CAST"
    sql = f"{function}({expression.render()} AS {duck_type.render()})"
    return expression_type._raw(  # pylint: disable=protected-access
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
        "CAST targets must be DuckDB types, type specifications, or DuckDBType "
        f"subclasses (got {type(target)!r})"
    )
    raise TypeError(msg)


def _expression_type_for_duck_type(
    duck_type: DuckDBType,
) -> Type[TypedExpression]:
    # pylint: disable=too-many-locals
    from .binary import BlobExpression  # pylint: disable=import-outside-toplevel
    from .boolean import BooleanExpression  # pylint: disable=import-outside-toplevel
    from .generic import GenericExpression  # pylint: disable=import-outside-toplevel
    from .numeric import NumericExpression  # pylint: disable=import-outside-toplevel
    from .temporal import (  # pylint: disable=import-outside-toplevel
        DateExpression,
        TemporalExpression,
        TimestampExpression,
        TimestampMicrosecondsExpression,
        TimestampMillisecondsExpression,
        TimestampNanosecondsExpression,
        TimestampSecondsExpression,
        TimestampWithTimezoneExpression,
    )
    from .text import VarcharExpression  # pylint: disable=import-outside-toplevel

    temporal_map: dict[str, Type[TypedExpression]] = {
        "DATE": DateExpression,
        "TIMESTAMP": TimestampExpression,
        "TIMESTAMP_S": TimestampSecondsExpression,
        "TIMESTAMP_MS": TimestampMillisecondsExpression,
        "TIMESTAMP_US": TimestampMicrosecondsExpression,
        "TIMESTAMP_NS": TimestampNanosecondsExpression,
        "TIMESTAMP WITH TIME ZONE": TimestampWithTimezoneExpression,
    }

    expression_type: Type[TypedExpression] = GenericExpression
    if isinstance(duck_type, BooleanType):
        expression_type = BooleanExpression
    elif isinstance(duck_type, VarcharType):
        expression_type = VarcharExpression
    elif isinstance(duck_type, BlobType):
        expression_type = BlobExpression
    elif isinstance(duck_type, (DecimalType, IntegerType, NumericType, FloatingType)):
        expression_type = NumericExpression
    elif isinstance(duck_type, TemporalType):
        expression_type = temporal_map.get(duck_type.render(), TemporalExpression)
    elif isinstance(duck_type, (GenericType, UnknownType)):
        expression_type = GenericExpression
    return expression_type
