"""Base expression classes shared across typed operations."""

from __future__ import annotations

from types import NotImplementedType
from typing import Iterable, TypeVar, Union

from ..types import BooleanType, DuckDBType, GenericType
from .utils import quote_identifier

ExpressionT = TypeVar("ExpressionT", bound="TypedExpression")
ComparisonResult = Union["BooleanExpression", NotImplementedType]


class TypedExpression:
    """Representation of a typed SQL expression."""

    __slots__ = ("_sql", "duck_type", "_dependencies")

    def __init__(
        self,
        sql: str,
        *,
        duck_type: DuckDBType,
        dependencies: Iterable[str] = (),
    ) -> None:
        self._sql = sql
        self.duck_type = duck_type
        self._dependencies = frozenset(dependencies)

    def render(self) -> str:
        return self._sql

    def __str__(self) -> str:  # pragma: no cover - delegation to ``render``
        return self.render()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{self.__class__.__name__}({self.render()!r}, {self.duck_type!r})"

    @property
    def dependencies(self) -> frozenset[str]:
        return self._dependencies

    def alias(self, alias: str) -> "AliasedExpression":
        return AliasedExpression(base=self, alias=alias)

    def _comparison(self: ExpressionT, operator: str, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} {operator} {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return BooleanExpression(sql, dependencies=dependencies)

    def _coerce_operand(self: ExpressionT, other: object) -> ExpressionT:
        raise NotImplementedError

    def __eq__(self, other: object) -> ComparisonResult:  # type: ignore[override]
        if isinstance(other, (TypedExpression, str, int, float, bool, bytes)):
            return self._comparison("=", other)
        return NotImplemented

    def __ne__(self, other: object) -> ComparisonResult:  # type: ignore[override]
        if isinstance(other, (TypedExpression, str, int, float, bool, bytes)):
            return self._comparison("!=", other)
        return NotImplemented


class AliasedExpression(TypedExpression):
    """Adapter adding an alias to an expression during rendering."""

    __slots__ = ("base", "alias_name")

    def __init__(self, *, base: TypedExpression, alias: str) -> None:
        self.base = base
        self.alias_name = alias
        super().__init__(
            base.render(),
            duck_type=base.duck_type,
            dependencies=base.dependencies,
        )

    def render(self) -> str:
        return f"{self.base.render()} AS {quote_identifier(self.alias_name)}"

    def _coerce_operand(self, other: object) -> TypedExpression:  # type: ignore[override]
        return self.base._coerce_operand(other)  # pylint: disable=protected-access


class BooleanExpression(TypedExpression):
    """Boolean expressions support logical composition."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        duck_type: DuckDBType | None = None,
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or BooleanType("BOOLEAN"),
            dependencies=dependencies,
        )

    def __and__(self, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} AND {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return BooleanExpression(sql, dependencies=dependencies)

    def __or__(self, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} OR {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return BooleanExpression(sql, dependencies=dependencies)

    def __invert__(self) -> "BooleanExpression":
        return BooleanExpression(f"(NOT {self.render()})", dependencies=self.dependencies)

    def _coerce_operand(self, other: object) -> "BooleanExpression":
        if isinstance(other, BooleanExpression):
            return other
        if isinstance(other, bool):
            sql = "TRUE" if other else "FALSE"
            return BooleanExpression(sql)
        msg = "Boolean expressions only accept other boolean expressions or bool literals"
        raise TypeError(msg)

    @classmethod
    def column(cls, name: str) -> "BooleanExpression":
        return cls(
            quote_identifier(name),
            dependencies=(name,),
        )

    @classmethod
    def literal(cls, value: bool) -> "BooleanExpression":
        return cls("TRUE" if value else "FALSE")

    @classmethod
    def raw(cls, sql: str, *, dependencies: Iterable[str] = ()) -> "BooleanExpression":
        return cls(sql, dependencies=dependencies)


class GenericExpression(TypedExpression):
    """Representation of a DuckDB expression with unknown concrete type."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        duck_type: DuckDBType | None = None,
        dependencies: Iterable[str] = (),
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or GenericType("UNKNOWN"),
            dependencies=dependencies,
        )

    def _coerce_operand(self, other: object) -> "GenericExpression":
        if isinstance(other, TypedExpression):
            return GenericExpression(
                other.render(),
                duck_type=other.duck_type,
                dependencies=other.dependencies,
            )
        msg = "Generic expressions only accept other SQL expressions"
        raise TypeError(msg)

    @classmethod
    def column(cls, name: str) -> "GenericExpression":
        return cls(
            quote_identifier(name),
            dependencies=(name,),
        )

    @classmethod
    def raw(cls, sql: str, *, dependencies: Iterable[str] = ()) -> "GenericExpression":
        return cls(sql, dependencies=dependencies)

    def max_by(self, order: "TypedExpression") -> "TypedExpression":
        from ..functions import AGGREGATE_FUNCTIONS

        aggregator = AGGREGATE_FUNCTIONS.Generic.max_by
        return aggregator(self, order)
