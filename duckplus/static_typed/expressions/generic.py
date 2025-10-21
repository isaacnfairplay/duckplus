"""Generic expression factory."""

from __future__ import annotations

from typing import Iterable

from ..dependencies import DependencyLike
from ..types import DuckDBType, GenericType
from .base import GenericExpression, TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .numeric import NumericFactory


class GenericFactory:
    def __init__(self) -> None:
        self._aggregate = GenericAggregateFactory(self)

    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> GenericExpression:
        return GenericExpression.column(column, table=table)

    def _raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> GenericExpression:
        return GenericExpression(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or GenericType("UNKNOWN"),
        )

    def null(self) -> GenericExpression:
        """Return a typed expression representing ``NULL``."""

        return GenericExpression(
            "NULL",
            duck_type=GenericType("UNKNOWN"),
        )

    def coerce(self, operand: object) -> GenericExpression:
        if isinstance(operand, GenericExpression):
            return operand
        if isinstance(operand, TypedExpression):
            return GenericExpression(
                operand.render(),
                duck_type=operand.duck_type,
                dependencies=operand.dependencies,
            )
        if isinstance(operand, str):
            return self(operand)
        msg = "Unsupported operand for generic expression"
        raise TypeError(msg)

    def case(self) -> CaseExpressionBuilder[GenericExpression]:
        boolean_factory = BooleanFactory()
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=boolean_factory.coerce,
        )

    @property
    def Aggregate(self) -> "GenericAggregateFactory":  # pylint: disable=invalid-name
        return self._aggregate


class GenericAggregateFactory:
    def __init__(self, factory: GenericFactory) -> None:
        self._factory = factory
        self._numeric_factory = NumericFactory()

    def _wrap(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> GenericExpression:
        return GenericExpression(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or GenericType("UNKNOWN"),
        )

    def max(self, operand: object) -> GenericExpression:
        expression = self._factory.coerce(operand)
        sql = f"max({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def min(self, operand: object) -> GenericExpression:
        expression = self._factory.coerce(operand)
        sql = f"min({expression.render()})"
        return self._wrap(sql, dependencies=expression.dependencies, duck_type=expression.duck_type)

    def max_by(self, value: object, order: object) -> GenericExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._numeric_factory.coerce(order)
        sql = f"max_by({value_expr.render()}, {order_expr.render()})"
        deps = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=deps, duck_type=value_expr.duck_type)

    def min_by(self, value: object, order: object) -> GenericExpression:
        value_expr = self._factory.coerce(value)
        order_expr = self._numeric_factory.coerce(order)
        sql = f"min_by({value_expr.render()}, {order_expr.render()})"
        deps = value_expr.dependencies.union(order_expr.dependencies)
        return self._wrap(sql, dependencies=deps, duck_type=value_expr.duck_type)
