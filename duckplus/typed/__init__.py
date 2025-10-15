"""Typed expression primitives for DuckPlus."""

# pylint: disable=duplicate-code

from .dependencies import ExpressionDependency
from .expression import (
    AliasedExpression,
    BlobExpression,
    BooleanExpression,
    CaseExpressionBuilder,
    GenericExpression,
    NumericExpression,
    SelectStatementBuilder,
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
    "SelectStatementBuilder",
    "TypedExpression",
    "VarcharExpression",
    "ducktype",
    "ExpressionDependency",
    "DuckDBFunctionNamespace",
    "DuckDBFunctionSignature",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
]
