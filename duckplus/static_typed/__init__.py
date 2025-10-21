"""Typed expression primitives for DuckPlus."""

# pylint: disable=duplicate-code,wildcard-import,unused-wildcard-import,undefined-all-variable,E0603

from .dependencies import ExpressionDependency
from .expression import (
    AliasedExpression,
    BlobExpression,
    BooleanExpression,
    CaseExpressionBuilder,
    DateExpression,
    GenericExpression,
    NumericAggregateFactory,
    NumericExpression,
    TemporalAggregateFactory,
    SelectStatementBuilder,
    TimestampExpression,
    TypedExpression,
    VarcharExpression,
)
from .expressions.decimal import DECIMAL_FACTORY_NAMES as _DECIMAL_FACTORY_NAMES
from .expressions.decimal import *  # noqa: F401,F403 - re-export decimal factories
from .ducktype import (
    Blob,
    Boolean,
    Date,
    Double,
    Generic,
    Integer,
    Numeric,
    Smallint,
    Tinyint,
    Timestamp,
    Timestamp_ms,
    Timestamp_ns,
    Timestamp_s,
    Timestamp_tz,
    Timestamp_us,
    Utinyint,
    Usmallint,
    Uinteger,
    Float,
    Varchar,
    ducktype,
    select,
)
from ._generated_function_namespaces import (
    AGGREGATE_FUNCTIONS,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
    DuckDBFunctionNamespace,
)
from ..functions.aggregate import approximation as _aggregate_approximation  # noqa: F401
from . import function_overrides as _function_overrides  # noqa: F401

__all__ = [
    "AliasedExpression",
    "BlobExpression",
    "BooleanExpression",
    "CaseExpressionBuilder",
    "DateExpression",
    "GenericExpression",
    "NumericAggregateFactory",
    "NumericExpression",
    "TemporalAggregateFactory",
    "SelectStatementBuilder",
    "TimestampExpression",
    "TypedExpression",
    "VarcharExpression",
    "ExpressionDependency",
    "ducktype",
    "Numeric",
    "Varchar",
    "Boolean",
    "Blob",
    "Generic",
    "Tinyint",
    "Smallint",
    "Integer",
    "Utinyint",
    "Usmallint",
    "Uinteger",
    "Float",
    "Double",
    "Date",
    "Timestamp",
    "Timestamp_s",
    "Timestamp_ms",
    "Timestamp_us",
    "Timestamp_ns",
    "Timestamp_tz",
    "select",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
    "DuckDBFunctionNamespace",
]

__all__.extend(_DECIMAL_FACTORY_NAMES)
