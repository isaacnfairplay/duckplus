"""Typed expression primitives for DuckPlus."""

# pylint: disable=duplicate-code

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

for _decimal_name in ducktype.decimal_factory_names:
    globals()[_decimal_name] = getattr(ducktype, _decimal_name)

del _decimal_name

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
]

__all__.extend(ducktype.decimal_factory_names)
