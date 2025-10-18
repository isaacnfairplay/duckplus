"""Temporal expression primitives."""

# pylint: disable=protected-access

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

from ..dependencies import DependencyLike, ExpressionDependency
from ..types import DuckDBType, TemporalType
from .base import TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .numeric import NumericFactory
from .utils import quote_qualified_identifier, quote_string


class TemporalExpression(TypedExpression):
    """Base class for DuckDB temporal expressions."""

    __slots__ = ()
    _TYPE_NAME = "DATE"

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or self.default_type(),
            dependencies=dependencies,
        )

    @classmethod
    def default_type(cls) -> DuckDBType:
        return TemporalType(cls._TYPE_NAME)

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "TemporalExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,), duck_type=cls.default_type())

    @classmethod
    def literal(
        cls,
        value: object,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "TemporalExpression":
        rendered = cls._format_literal(value)
        return cls(rendered, duck_type=duck_type or cls.default_type())

    @classmethod
    def _format_literal(cls, value: object) -> str:
        raise TypeError(f"{cls.__name__} does not support literal value {value!r}")

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "TemporalExpression":
        return cls(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or cls.default_type(),
        )

    def _coerce_operand(self, other: object) -> "TemporalExpression":
        expression_type = type(self)
        if isinstance(other, expression_type):
            return other
        if isinstance(other, TemporalExpression) and self.duck_type.accepts(other.duck_type):
            return expression_type._raw(
                other.render(),
                dependencies=other.dependencies,
                duck_type=other.duck_type,
            )
        try:
            return expression_type.literal(other)
        except TypeError as exc:  # pragma: no cover - defensive guard
            msg = (
                f"{self.duck_type.render()} expressions only accept compatible "
                "temporal operands"
            )
            raise TypeError(msg) from exc

    def coalesce(self, *others: object) -> "TemporalExpression":
        if not others:
            return self
        operands = [self]
        dependencies = set(self.dependencies)
        for other in others:
            operand = self._coerce_operand(other)
            operands.append(operand)
            dependencies.update(operand.dependencies)
        sql = ", ".join(expression.render() for expression in operands)
        return type(self)(
            f"COALESCE({sql})",
            dependencies=dependencies,
            duck_type=self.duck_type,
        )


class DateExpression(TemporalExpression):
    _TYPE_NAME = "DATE"

    @classmethod
    def _format_literal(cls, value: object) -> str:
        if isinstance(value, date) and not isinstance(value, datetime):
            rendered = value.isoformat()
        elif isinstance(value, str):
            rendered = value
        else:
            msg = "DATE literals must be date or string values"
            raise TypeError(msg)
        return f"DATE {quote_string(rendered)}"


class TimestampExpression(TemporalExpression):
    _TYPE_NAME = "TIMESTAMP"

    @classmethod
    def _format_literal(cls, value: object) -> str:
        if isinstance(value, datetime):
            rendered = value.isoformat(sep=" ")
        elif isinstance(value, str):
            rendered = value
        else:
            msg = "TIMESTAMP literals must be datetime or string values"
            raise TypeError(msg)
        return f"TIMESTAMP {quote_string(rendered)}"


class TimestampSecondsExpression(TimestampExpression):
    _TYPE_NAME = "TIMESTAMP_S"


class TimestampMillisecondsExpression(TimestampExpression):
    _TYPE_NAME = "TIMESTAMP_MS"


class TimestampMicrosecondsExpression(TimestampExpression):
    _TYPE_NAME = "TIMESTAMP_US"


class TimestampNanosecondsExpression(TimestampExpression):
    _TYPE_NAME = "TIMESTAMP_NS"


class TimestampWithTimezoneExpression(TimestampExpression):
    _TYPE_NAME = "TIMESTAMP WITH TIME ZONE"


class TemporalFactory:
    def __init__(self, expression_type: type[TemporalExpression]) -> None:
        self.expression_type = expression_type
        self._aggregate = TemporalAggregateFactory(self)
        self._boolean_factory = BooleanFactory()

    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> TemporalExpression:
        return self.expression_type.column(column, table=table)

    def literal(
        self,
        value: object,
        *,
        duck_type: DuckDBType | None = None,
    ) -> TemporalExpression:
        return self.expression_type.literal(value, duck_type=duck_type)

    def _raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> TemporalExpression:
        return self.expression_type._raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def coerce(self, operand: object) -> TemporalExpression:
        expression_type = self.expression_type
        if isinstance(operand, expression_type):
            return operand
        if isinstance(operand, TemporalExpression) and expression_type.default_type().accepts(
            operand.duck_type
        ):
            return expression_type._raw(
                operand.render(),
                dependencies=operand.dependencies,
                duck_type=operand.duck_type,
            )
        if isinstance(operand, str):
            return self(operand)
        if isinstance(operand, tuple) and len(operand) == 2:
            table, column = operand
            if isinstance(table, str) and isinstance(column, str):
                return expression_type.column(column, table=table)
        try:
            return self.literal(operand)
        except TypeError as exc:  # pragma: no cover - defensive guard
            msg = "Unsupported operand for temporal expression"
            raise TypeError(msg) from exc

    def case(self) -> CaseExpressionBuilder[TemporalExpression]:
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=self._boolean_factory.coerce,
        )

    @property
    def Aggregate(self) -> "TemporalAggregateFactory":  # pylint: disable=invalid-name
        return self._aggregate


class TemporalAggregateFactory:
    def __init__(self, factory: TemporalFactory) -> None:
        self._factory = factory
        self._numeric_factory = NumericFactory()

    def _wrap(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> TemporalExpression:
        expression_type = self._factory.expression_type
        return expression_type._raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def min(self, operand: object) -> TemporalExpression:
        expression = self._factory.coerce(operand)
        sql = f"min({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def max(self, operand: object) -> TemporalExpression:
        expression = self._factory.coerce(operand)
        sql = f"max({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def max_by(self, value: object, order: object) -> TemporalExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._numeric_factory.coerce(order)
        sql = f"max_by({value_expr.render()}, {order_expr.render()})"
        dependencies = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=dependencies, duck_type=value_expr.duck_type)

    def min_by(self, value: object, order: object) -> TemporalExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._numeric_factory.coerce(order)
        sql = f"min_by({value_expr.render()}, {order_expr.render()})"
        dependencies = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=dependencies, duck_type=value_expr.duck_type)
__all__ = [
    "DateExpression",
    "TemporalAggregateFactory",
    "TemporalExpression",
    "TemporalFactory",
    "TimestampExpression",
    "TimestampMillisecondsExpression",
    "TimestampMicrosecondsExpression",
    "TimestampNanosecondsExpression",
    "TimestampSecondsExpression",
    "TimestampWithTimezoneExpression",
]
