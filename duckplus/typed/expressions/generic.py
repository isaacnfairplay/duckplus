"""Generic expression factory."""

from __future__ import annotations

from typing import Iterable

from ..dependencies import DependencyLike
from ..types import DuckDBType, GenericType
from .base import GenericExpression, TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder


class GenericFactory:
    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> GenericExpression:
        return GenericExpression.column(column, table=table)

    def raw(
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
