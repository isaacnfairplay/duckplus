"""Factory helpers for typed expressions."""

from __future__ import annotations

from typing import cast

from duckplus.typed.protocols import (
    BooleanExprProto,
    NumericExprProto,
    StringExprProto,
    TemporalExprProto,
)

from .boolean import PredicateExpr
from .numeric import IntegerExpr, NumericExpr
from .string import VarcharExpr
from .temporal import TimestampExpr


def varchar(identifier: str) -> VarcharExpr:
    """Return a varchar expression bound to the given identifier."""

    return VarcharExpr(identifier)


def as_string_proto(expr: VarcharExpr) -> StringExprProto:
    """Cast a varchar expression to the string protocol for static analysers."""

    return cast(StringExprProto, expr)


def predicate(identifier: str) -> PredicateExpr:
    """Return a boolean predicate expression bound to the given identifier."""

    return PredicateExpr(identifier)


def as_boolean_proto(expr: PredicateExpr) -> BooleanExprProto:
    """Cast a predicate expression to the boolean protocol for static analysers."""

    return cast(BooleanExprProto, expr)


def integer(identifier: str) -> IntegerExpr:
    """Return an integer expression bound to the given identifier."""

    return IntegerExpr(identifier)


def as_numeric_proto(expr: NumericExpr) -> NumericExprProto:
    """Cast a numeric expression to the numeric protocol for static analysers."""

    return cast(NumericExprProto, expr)


def timestamp(identifier: str) -> TimestampExpr:
    """Return a timestamp expression bound to the given identifier."""

    return TimestampExpr(identifier)


def as_temporal_proto(expr: TimestampExpr) -> TemporalExprProto:
    """Cast a timestamp expression to the temporal protocol for static analysers."""

    return cast(TemporalExprProto, expr)
