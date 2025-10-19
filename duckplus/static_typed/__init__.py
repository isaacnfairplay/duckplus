"""Experimental static typed expression API."""

# pylint: disable=import-outside-toplevel

from __future__ import annotations
from .expressions import (
    AliasedExpression,
    BooleanExpression,
    BooleanFactory,
    DuckTypeNamespace,
    GenericExpression,
    GenericFactory,
    NumericExpression,
    NumericFactory,
    TemporalExpression,
    TemporalFactory,
    TypedExpression,
    VarcharExpression,
    VarcharFactory,
    WindowOrder,
)

__all__ = [
    "TypedExpression",
    "AliasedExpression",
    "BooleanExpression",
    "NumericExpression",
    "VarcharExpression",
    "GenericExpression",
    "TemporalExpression",
    "BooleanFactory",
    "NumericFactory",
    "VarcharFactory",
    "GenericFactory",
    "TemporalFactory",
    "WindowOrder",
    "DuckTypeNamespace",
    "ducktype",
    "cast",
    "try_cast",
]


def try_cast(expression: TypedExpression, target: object) -> TypedExpression:
    """TRY_CAST wrapper for parity with the legacy API."""

    from .casting import cast_expression

    return cast_expression(expression, target, try_cast=True)


def cast(expression: TypedExpression, target: object) -> TypedExpression:
    """CAST wrapper for parity with the legacy API."""

    from .casting import cast_expression

    return cast_expression(expression, target, try_cast=False)


ducktype = DuckTypeNamespace()
