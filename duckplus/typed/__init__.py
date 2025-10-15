"""Typed expression primitives for DuckPlus."""

from .expression import (
    AliasedExpression,
    BlobExpression,
    BooleanExpression,
    GenericExpression,
    NumericExpression,
    TypedExpression,
    VarcharExpression,
    ducktype,
)
from .functions import (
    AGGREGATE_FUNCTIONS,
    DuckDBFunctionNamespace,
    DuckDBFunctionSignature,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
)

__all__ = [
    "AliasedExpression",
    "BlobExpression",
    "BooleanExpression",
    "GenericExpression",
    "NumericExpression",
    "TypedExpression",
    "VarcharExpression",
    "ducktype",
    "DuckDBFunctionNamespace",
    "DuckDBFunctionSignature",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
]
