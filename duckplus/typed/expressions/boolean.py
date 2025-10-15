"""Boolean expression factories."""

from __future__ import annotations

from typing import Iterable

from ..dependencies import DependencyLike
from .base import BooleanExpression
from .case import CaseExpressionBuilder


class BooleanFactory:
    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> BooleanExpression:
        return BooleanExpression.column(column, table=table)

    def literal(self, value: bool) -> BooleanExpression:
        return BooleanExpression.literal(value)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
    ) -> BooleanExpression:
        return BooleanExpression(sql, dependencies=dependencies)

    def coerce(self, operand: object) -> BooleanExpression:
        if isinstance(operand, BooleanExpression):
            return operand
        if isinstance(operand, bool):
            return self.literal(operand)
        msg = "Boolean operands must be expression or bool"
        raise TypeError(msg)

    def case(self) -> CaseExpressionBuilder[BooleanExpression]:
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=self.coerce,
        )
