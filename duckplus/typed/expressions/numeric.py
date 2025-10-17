"""Numeric expression primitives and factories."""

from __future__ import annotations

# pylint: disable=protected-access

from decimal import Decimal
from typing import Iterable

from ..dependencies import DependencyLike, ExpressionDependency
from ..types import DuckDBType, NumericType, infer_numeric_literal_type
from .base import GenericExpression, TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .utils import format_numeric, quote_qualified_identifier

NumericOperand = int | float | Decimal


class NumericExpression(TypedExpression):
    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or NumericType("NUMERIC"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "NumericExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,))

    @classmethod
    def literal(
        cls,
        value: NumericOperand,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        inferred_type = duck_type or infer_numeric_literal_type(value)
        return cls(
            format_numeric(value),
            duck_type=inferred_type,
        )

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        return cls(sql, dependencies=dependencies, duck_type=duck_type)

    def _coerce_operand(self, other: object) -> "NumericExpression":
        if isinstance(other, NumericExpression):
            return other
        if isinstance(other, (int, float, Decimal)):
            return NumericExpression.literal(other)
        msg = "Numeric expressions only accept numeric operands"
        raise TypeError(msg)

    def _binary(self, operator: str, other: object) -> "NumericExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} {operator} {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return NumericExpression(sql, dependencies=dependencies)

    def __add__(self, other: object) -> "NumericExpression":
        return self._binary("+", other)

    def __sub__(self, other: object) -> "NumericExpression":
        return self._binary("-", other)

    def __mul__(self, other: object) -> "NumericExpression":
        return self._binary("*", other)

    def __truediv__(self, other: object) -> "NumericExpression":
        return self._binary("/", other)

    def __mod__(self, other: object) -> "NumericExpression":
        return self._binary("%", other)

    def __pow__(self, other: object) -> "NumericExpression":
        return self._binary("^", other)

    def coalesce(self, *others: object) -> "NumericExpression":
        """Return the first non-null numeric expression from the arguments."""

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

    def abs(self) -> "NumericExpression":
        """Return the absolute value of the numeric expression."""

        sql = f"abs({self.render()})"
        return type(self)(sql, dependencies=self.dependencies, duck_type=self.duck_type)

    def pow(self, exponent: object) -> "NumericExpression":
        """Raise the expression to the provided ``exponent``."""

        operand = self._coerce_operand(exponent)
        dependencies = self.dependencies.union(operand.dependencies)
        sql = f"pow({self.render()}, {operand.render()})"
        return type(self)(sql, dependencies=dependencies)

    def nullif(self, other: object) -> "NumericExpression":
        """Return ``NULLIF`` between the expression and ``other``."""

        operand = self._coerce_operand(other)
        dependencies = self.dependencies.union(operand.dependencies)
        sql = f"NULLIF({self.render()}, {operand.render()})"
        return type(self)(sql, dependencies=dependencies, duck_type=self.duck_type)

    # Aggregation -----------------------------------------------------
    def sum(self) -> "NumericExpression":
        sql = f"sum({self.render()})"
        return type(self)(sql, dependencies=self.dependencies)

    def avg(self) -> "NumericExpression":
        sql = f"avg({self.render()})"
        return type(self)(sql, dependencies=self.dependencies)


class NumericFactory:
    """Factory for creating numeric expressions."""

    def __init__(self) -> None:
        self._aggregate = NumericAggregateFactory(self)

    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> NumericExpression:
        return NumericExpression.column(column, table=table)

    def literal(
        self,
        value: NumericOperand,
        *,
        duck_type: DuckDBType | None = None,
    ) -> NumericExpression:
        return NumericExpression.literal(value, duck_type=duck_type)

    def _raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> NumericExpression:
        return NumericExpression._raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def coerce(self, operand: object) -> NumericExpression:
        if isinstance(operand, NumericExpression):
            return operand
        if isinstance(operand, str):
            return self(operand)
        if isinstance(operand, tuple) and len(operand) == 2:
            table, column = operand
            if isinstance(table, str) and isinstance(column, str):
                return NumericExpression.column(column, table=table)
        if isinstance(operand, (int, float, Decimal)):
            return self.literal(operand)
        msg = "Unsupported operand for numeric expression"
        raise TypeError(msg)

    def case(self) -> CaseExpressionBuilder[NumericExpression]:
        boolean_factory = BooleanFactory()
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=boolean_factory.coerce,
        )

    @property
    def Aggregate(self) -> "NumericAggregateFactory":  # pylint: disable=invalid-name
        return self._aggregate


class NumericAggregateFactory:
    def __init__(
        self,
        factory: NumericFactory,
    ) -> None:
        self._factory = factory
        self._boolean_factory = BooleanFactory()

    def _wrap(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> NumericExpression:
        return NumericExpression._raw(sql, dependencies=dependencies, duck_type=duck_type)

    def _coerce_order_operand(self, operand: object) -> TypedExpression:
        if isinstance(operand, TypedExpression):
            return operand
        if isinstance(operand, str):
            return GenericExpression.column(operand)
        if isinstance(operand, tuple) and len(operand) == 2:
            table, column = operand
            if isinstance(table, str) and isinstance(column, str):
                return GenericExpression.column(column, table=table)
        if isinstance(operand, (int, float, Decimal)):
            return self._factory.coerce(operand)
        msg = "Unsupported operand for ordering expression"
        raise TypeError(msg)

    def sum(self, operand: object) -> NumericExpression:
        expression = self._factory.coerce(operand)
        sql = f"sum({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def avg(self, operand: object) -> NumericExpression:
        expression = self._factory.coerce(operand)
        sql = f"avg({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies)

    def min(self, operand: object) -> NumericExpression:
        expression = self._factory.coerce(operand)
        sql = f"min({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def max(self, operand: object) -> NumericExpression:
        expression = self._factory.coerce(operand)
        sql = f"max({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def count(self, operand: object | None = None) -> NumericExpression:
        if operand is None:
            return self._wrap("count(*)")
        expression = self._factory.coerce(operand)
        sql = f"count({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies)

    def count_if(self, predicate: object) -> NumericExpression:
        condition = self._boolean_factory.coerce(predicate)
        sql = f"count_if({condition.render()})"
        return self._wrap(sql, dependencies=condition.dependencies)

    def sum_filter(
        self,
        predicate: object,
        operand: object,
    ) -> NumericExpression:
        condition = self._boolean_factory.coerce(predicate)
        expression = self._factory.coerce(operand)
        sql = f"sum({expression.render()}) FILTER (WHERE {condition.render()})"
        deps = expression.dependencies.union(condition.dependencies)
        return self._wrap(sql, dependencies=deps, duck_type=expression.duck_type)

    def max_by(self, value: object, order: object) -> NumericExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._coerce_order_operand(order)
        sql = f"max_by({value_expr.render()}, {order_expr.render()})"
        deps = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=deps, duck_type=value_expr.duck_type)

    def min_by(self, value: object, order: object) -> NumericExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._coerce_order_operand(order)
        sql = f"min_by({value_expr.render()}, {order_expr.render()})"
        deps = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=deps, duck_type=value_expr.duck_type)
