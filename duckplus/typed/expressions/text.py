"""Text expression primitives and factories."""

from __future__ import annotations

from typing import Iterable

from ..dependencies import DependencyLike, ExpressionDependency
from ..types import DuckDBType, VarcharType
from .base import TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .utils import quote_qualified_identifier, quote_string


class VarcharExpression(TypedExpression):
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
            duck_type=duck_type or VarcharType("VARCHAR"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "VarcharExpression":
        dependency = ExpressionDependency.column(name, table=table)
        return cls(
            quote_qualified_identifier(name, table=table),
            dependencies=(dependency,),
        )

    @classmethod
    def literal(
        cls,
        value: str,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "VarcharExpression":
        return cls(
            quote_string(value),
            duck_type=duck_type or VarcharType("VARCHAR"),
        )

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "VarcharExpression":
        return cls(sql, dependencies=dependencies, duck_type=duck_type)

    def _coerce_operand(self, other: object) -> "VarcharExpression":
        if isinstance(other, VarcharExpression):
            return other
        if isinstance(other, str):
            return VarcharExpression.literal(other)
        msg = "Varchar expressions only accept string operands"
        raise TypeError(msg)

    def _concat(self, other: object) -> "VarcharExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} || {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return VarcharExpression(sql, dependencies=dependencies)

    def __add__(self, other: object) -> "VarcharExpression":
        return self._concat(other)

    def __radd__(self, other: object) -> "VarcharExpression":
        if isinstance(other, (VarcharExpression, str)):
            return type(self).coerce_literal(other)._concat(self)
        return NotImplemented

    @classmethod
    def coerce_literal(cls, value: object) -> "VarcharExpression":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls.literal(value)
        msg = "Varchar literals must be string values"
        raise TypeError(msg)


class VarcharFactory:
    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> VarcharExpression:
        return VarcharExpression.column(column, table=table)

    def literal(self, value: str) -> VarcharExpression:
        return VarcharExpression.literal(value)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> VarcharExpression:
        return VarcharExpression.raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def coerce(self, operand: object) -> VarcharExpression:
        if isinstance(operand, VarcharExpression):
            return operand
        if isinstance(operand, str):
            return self.literal(operand)
        msg = "Unsupported operand for varchar expression"
        raise TypeError(msg)

    def case(self) -> CaseExpressionBuilder[VarcharExpression]:
        boolean_factory = BooleanFactory()
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=boolean_factory.coerce,
        )
