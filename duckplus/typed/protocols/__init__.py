"""Protocols describing the static expression surface for DuckPlus."""

from .string import StringExprProto
from .numeric import NumericExprProto, IntegerExprProto
from .boolean import BooleanExprProto
from .temporal import TemporalExprProto

__all__ = [
    "StringExprProto",
    "NumericExprProto",
    "IntegerExprProto",
    "BooleanExprProto",
    "TemporalExprProto",
]
