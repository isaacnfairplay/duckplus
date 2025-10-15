"""Generic expression factory."""

from __future__ import annotations

from typing import Iterable

from ..types import DuckDBType, GenericType
from .base import GenericExpression


class GenericFactory:
    def __call__(self, column: str) -> GenericExpression:
        return GenericExpression.column(column)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
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
        if isinstance(operand, str):
            return self(operand)
        msg = "Unsupported operand for generic expression"
        raise TypeError(msg)
