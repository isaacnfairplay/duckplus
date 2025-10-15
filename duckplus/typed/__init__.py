"""Typed expression primitives for DuckPlus."""

# pylint: disable=duplicate-code

from .expression import (
    AliasedExpression,
    BlobExpression,
    BooleanExpression,
    CaseExpressionBuilder,
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
    "CaseExpressionBuilder",
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
