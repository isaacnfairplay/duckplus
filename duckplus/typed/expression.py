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
from .types import DecimalType, DuckDBType


def _create_decimal_expression(precision: int, scale: int) -> type[NumericExpression]:
    class DecimalExpression(NumericExpression):  # type: ignore[misc]
        __slots__ = ()

        @classmethod
        def default_type(cls) -> DuckDBType:  # type: ignore[override]
            return DecimalType(precision, scale)

        @classmethod
        def default_literal_type(
            cls, value: NumericOperand
        ) -> DuckDBType:  # type: ignore[override]
            return DecimalType(precision, scale)

    DecimalExpression.__name__ = f"Decimal{precision}_{scale}Expression"
    DecimalExpression.__qualname__ = DecimalExpression.__name__
    return DecimalExpression


def _build_decimal_factories() -> tuple[tuple[str, NumericFactory], ...]:
    factories: list[tuple[str, NumericFactory]] = []
    for precision in range(1, 39):
        for scale in range(0, precision + 1):
            name = f"Decimal_{precision}_{scale}"
            expression_type = _create_decimal_expression(precision, scale)
            factories.append((name, NumericFactory(expression_type)))
    return tuple(factories)


_DECIMAL_FACTORY_ITEMS = _build_decimal_factories()
_DECIMAL_FACTORY_NAMES = tuple(name for name, _ in _DECIMAL_FACTORY_ITEMS)


def _attach_decimal_factories(
    namespace: type["DuckTypeNamespace"],
) -> type["DuckTypeNamespace"]:
    for name, factory in _DECIMAL_FACTORY_ITEMS:
        setattr(namespace, name, factory)
    namespace._DECIMAL_FACTORY_NAMES = _DECIMAL_FACTORY_NAMES
    return namespace


@_attach_decimal_factories
class DuckTypeNamespace:
    """Container exposing typed expression factories."""

    _DECIMAL_FACTORY_NAMES: tuple[str, ...] = ()

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
        self._decimal_names: list[str] = []
        self._register_decimal_factories()

    def select(self) -> SelectStatementBuilder:
        return SelectStatementBuilder()

    def row_number(self) -> NumericExpression:
        """Return a typed expression invoking ``ROW_NUMBER()``."""

        return NumericExpression._raw("row_number()")

    @property
    def decimal_factory_names(self) -> tuple[str, ...]:
        return tuple(self._decimal_names)

    # Transitional shim -------------------------------------------------
    def _register_decimal_factories(self) -> None:
        """Rebind decimal factories for compatibility with legacy callers."""

        self._decimal_names = []
        for name in type(self)._DECIMAL_FACTORY_NAMES:
            setattr(self, name, getattr(type(self), name))
            self._decimal_names.append(name)


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
