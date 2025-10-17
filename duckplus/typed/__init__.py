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
from .ducktype import (
    Blob,
    Boolean,
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
    "ducktype",
    "Numeric",
    "Varchar",
    "Boolean",
    "Blob",
    "Generic",
    "select",
]
