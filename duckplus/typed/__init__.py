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
)
from .functions import (
    AGGREGATE_FUNCTIONS,
    DuckDBFunctionNamespace,
    DuckDBFunctionSignature,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
)
from .ducktype import (
    Blob,
    Boolean,
    Functions,
    Generic,
    Numeric,
    Varchar,
    ducktype,
    select,
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
    "ExpressionDependency",
    "DuckDBFunctionNamespace",
    "DuckDBFunctionSignature",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
    "ducktype",
    "Numeric",
    "Varchar",
    "Boolean",
    "Blob",
    "Generic",
    "Functions",
    "select",
]
