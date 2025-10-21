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
from .expressions.decimal import DECIMAL_FACTORY_NAMES, register_decimal_factories
from .expressions.text import VarcharExpression, VarcharFactory
from .expressions.temporal import (
    DateExpression,
    TemporalAggregateFactory,
    TemporalFactory,
    TimestampExpression,
    TimestampMillisecondsExpression,
    TimestampMicrosecondsExpression,
    TimestampNanosecondsExpression,
    TimestampSecondsExpression,
    TimestampWithTimezoneExpression,
)
from .expressions.utils import format_numeric as _format_numeric
from .expressions.utils import quote_identifier as _quote_identifier
from .expressions.utils import quote_string as _quote_string
from .select import SelectStatementBuilder


@register_decimal_factories
class DuckTypeNamespace:
    """Container exposing typed expression factories."""

    _DECIMAL_FACTORY_NAMES: tuple[str, ...] = DECIMAL_FACTORY_NAMES

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
        self.Timestamp = TemporalFactory(TimestampExpression)
        self.Timestamp_s = TemporalFactory(TimestampSecondsExpression)
        self.Timestamp_ms = TemporalFactory(TimestampMillisecondsExpression)
        self.Timestamp_us = TemporalFactory(TimestampMicrosecondsExpression)
        self.Timestamp_ns = TemporalFactory(TimestampNanosecondsExpression)
        self.Timestamp_tz = TemporalFactory(TimestampWithTimezoneExpression)

    def select(self) -> SelectStatementBuilder:
        return SelectStatementBuilder()

    def row_number(self) -> NumericExpression:
        """Return a typed expression invoking ``ROW_NUMBER()``."""

        return NumericExpression._raw("row_number()")

    @property
    def decimal_factory_names(self) -> tuple[str, ...]:
        return type(self)._DECIMAL_FACTORY_NAMES


ducktype = DuckTypeNamespace()

# Populate the module namespace with each decimal factory to match ``__all__``.
globals().update(
    {
        name: getattr(DuckTypeNamespace, name)
        for name in DECIMAL_FACTORY_NAMES
    }
)

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
    "TimestampMillisecondsExpression",
    "TimestampMicrosecondsExpression",
    "TimestampNanosecondsExpression",
    "TimestampSecondsExpression",
    "TimestampWithTimezoneExpression",
    "SelectStatementBuilder",
    "TypedExpression",
    "VarcharExpression",
    "VarcharFactory",
    "_format_numeric",
    "_quote_identifier",
    "_quote_string",
    "ducktype",
]

__all__.extend(DECIMAL_FACTORY_NAMES)
