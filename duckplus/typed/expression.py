"""Public typed expression API built from modular components."""

# pylint: disable=too-few-public-methods,invalid-name,import-outside-toplevel,cyclic-import,protected-access,too-many-instance-attributes

from __future__ import annotations

from .expressions.base import (
    AliasedExpression,
    BooleanExpression,
    GenericExpression,
    TypedExpression,
)
from .expressions.case import CaseExpressionBuilder
from .expressions.binary import BlobExpression, BlobFactory
from .expressions.boolean import BooleanFactory
from .expressions.generic import GenericFactory
from .expressions.numeric import (
    DoubleExpression,
    FloatExpression,
    IntegerExpression,
    NumericAggregateFactory,
    NumericExpression,
    NumericFactory,
    NumericOperand,
    SmallintExpression,
    TinyintExpression,
    UnsignedIntegerExpression,
    UnsignedSmallintExpression,
    UnsignedTinyintExpression,
)
from .expressions.text import VarcharExpression, VarcharFactory
from .expressions.temporal import (
    DateExpression,
    TemporalAggregateFactory,
    TemporalFactory,
    TimestampExpression,
)
from .expressions.utils import format_numeric as _format_numeric
from .expressions.utils import quote_identifier as _quote_identifier
from .expressions.utils import quote_string as _quote_string
from .select import SelectStatementBuilder


class DuckTypeNamespace:
    """Container exposing typed expression factories."""

    def __init__(self) -> None:
        self.Numeric = NumericFactory()
        self.Varchar = VarcharFactory()
        self.Boolean = BooleanFactory()
        self.Blob = BlobFactory()
        self.Generic = GenericFactory()
        self.Tinyint = NumericFactory(TinyintExpression)
        self.Smallint = NumericFactory(SmallintExpression)
        self.Integer = NumericFactory(IntegerExpression)
        self.Utinyint = NumericFactory(UnsignedTinyintExpression)
        self.Usmallint = NumericFactory(UnsignedSmallintExpression)
        self.Uinteger = NumericFactory(UnsignedIntegerExpression)
        self.Float = NumericFactory(FloatExpression)
        self.Double = NumericFactory(DoubleExpression)
        self.Date = TemporalFactory(DateExpression)
        self.Datetime = TemporalFactory(TimestampExpression)
        self.Timestamp = self.Datetime

    def select(self) -> SelectStatementBuilder:
        return SelectStatementBuilder()

    def row_number(self) -> NumericExpression:
        """Return a typed expression invoking ``ROW_NUMBER()``."""

        return NumericExpression._raw("row_number()")


ducktype = DuckTypeNamespace()

__all__ = [
    "AliasedExpression",
    "BlobExpression",
    "BlobFactory",
    "BooleanExpression",
    "CaseExpressionBuilder",
    "BooleanFactory",
    "DuckTypeNamespace",
    "GenericExpression",
    "GenericFactory",
    "NumericAggregateFactory",
    "NumericExpression",
    "NumericFactory",
    "NumericOperand",
    "TemporalAggregateFactory",
    "TemporalFactory",
    "DateExpression",
    "TimestampExpression",
    "SelectStatementBuilder",
    "TypedExpression",
    "VarcharExpression",
    "VarcharFactory",
    "_format_numeric",
    "_quote_identifier",
    "_quote_string",
    "ducktype",
]
