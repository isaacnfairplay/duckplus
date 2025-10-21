"""Text expression primitives and factories."""

from __future__ import annotations

# pylint: disable=protected-access

from typing import Iterable

from ..dependencies import DependencyLike, ExpressionDependency
from ..types import DuckDBType, VarcharType
from .base import TypedExpression, BooleanExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .numeric import NumericExpression
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
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "VarcharExpression":
        return cls(sql, dependencies=dependencies, duck_type=duck_type)

    def coalesce(self, *others: object) -> "VarcharExpression":
        """Return the first non-null value from the provided expressions."""

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

    def length(self) -> "NumericExpression":
        """Return the number of characters in the expression."""

        sql = f"length({self.render()})"
        return NumericExpression._raw(sql, dependencies=self.dependencies)

    def slice(self, start: int, length: int | None = None) -> "VarcharExpression":
        """Return a substring starting at ``start`` with optional ``length``."""

        if not isinstance(start, int):  # pragma: no cover - defensive guard
            msg = "slice start must be an integer"
            raise TypeError(msg)
        if length is not None and not isinstance(length, int):  # pragma: no cover
            msg = "slice length must be an integer or None"
            raise TypeError(msg)

        if length is None:
            sql = f"substr({self.render()}, {start})"
            return VarcharExpression._raw(sql, dependencies=self.dependencies)

        sql = f"substr({self.render()}, {start}, {length})"
        return VarcharExpression._raw(sql, dependencies=self.dependencies)

    def contains(self, needle: object) -> "BooleanExpression":
        """Return whether ``needle`` occurs within the expression."""

        operand = self._coerce_operand(needle)

        sql = f"strpos({self.render()}, {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        position = NumericExpression._raw(sql, dependencies=dependencies)
        return position > NumericExpression.literal(0)

    def starts_with(self, prefix: object) -> "BooleanExpression":
        """Return whether the expression begins with ``prefix``."""

        operand = self._coerce_operand(prefix)
        dependencies = self.dependencies.union(operand.dependencies)
        sql = f"starts_with({self.render()}, {operand.render()})"
        return BooleanExpression._raw(sql, dependencies=dependencies)

    def trim(self, characters: object | None = None) -> "VarcharExpression":
        """Return the expression with leading and trailing characters removed."""

        if characters is None:
            sql = f"trim({self.render()})"
            return type(self)._raw(sql, dependencies=self.dependencies)

        operand = self._coerce_operand(characters)
        dependencies = self.dependencies.union(operand.dependencies)
        sql = f"trim({self.render()}, {operand.render()})"
        return type(self)._raw(sql, dependencies=dependencies)

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

    def _raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> VarcharExpression:
        return VarcharExpression._raw(
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
