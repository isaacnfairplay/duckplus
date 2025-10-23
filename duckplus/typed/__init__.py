"""Typed expression namespace for DuckPlus."""

from .base import ExprMeta, TypedExpression
from .string import StringExpr, VarcharExpr
from .numeric import NumericExpr, IntegerExpr
from .boolean import BooleanExpr, PredicateExpr
from .temporal import TemporalExpr, TimestampExpr
from . import protocols
from .factory import (
    varchar,
    predicate,
    timestamp,
    integer,
    as_string_proto,
    as_boolean_proto,
    as_temporal_proto,
    as_numeric_proto,
)

__all__ = [
    "ExprMeta",
    "TypedExpression",
    "StringExpr",
    "VarcharExpr",
    "NumericExpr",
    "IntegerExpr",
    "BooleanExpr",
    "PredicateExpr",
    "TemporalExpr",
    "TimestampExpr",
    "protocols",
    "varchar",
    "predicate",
    "timestamp",
    "integer",
    "as_string_proto",
    "as_boolean_proto",
    "as_temporal_proto",
    "as_numeric_proto",
]
