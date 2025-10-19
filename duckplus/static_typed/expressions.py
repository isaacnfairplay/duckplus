"""Static typed expression scaffolding for the experimental API."""

# pylint: disable=too-many-lines,too-many-locals,too-few-public-methods,import-outside-toplevel,protected-access,invalid-name,cyclic-import

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence, TypeVar, Generic, Protocol, cast

from duckplus.typed.dependencies import (
    DependencyLike,
    ExpressionDependency,
    normalise_dependencies,
)
from duckplus.typed.expressions.utils import (
    format_numeric,
    quote_identifier,
    quote_qualified_identifier,
)
from duckplus.typed.types import (
    BooleanType,
    DuckDBType,
    GenericType,
    NumericType,
    TemporalType,
    VarcharType,
    infer_numeric_literal_type,
)

__all__ = [
    "TypedExpression",
    "AliasedExpression",
    "BooleanExpression",
    "NumericExpression",
    "VarcharExpression",
    "GenericExpression",
    "TemporalExpression",
    "WindowOrder",
    "BooleanFactory",
    "NumericFactory",
    "VarcharFactory",
    "GenericFactory",
    "TemporalFactory",
    "DuckTypeNamespace",
]


ExpressionT = TypeVar("ExpressionT", bound="TypedExpression")
ExpressionT_co = TypeVar("ExpressionT_co", bound="TypedExpression", covariant=True)


class _SupportsFactory(Protocol[ExpressionT_co]):
    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> ExpressionT_co: ...

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = ...,
        duck_type: DuckDBType | None = ...,
    ) -> ExpressionT_co: ...


class TypedExpression:
    """Representation of a statically defined SQL expression."""

    __slots__ = ("_sql", "duck_type", "_dependencies")

    def __init__(
        self,
        sql: str,
        *,
        duck_type: DuckDBType,
        dependencies: Iterable[DependencyLike] = (),
    ) -> None:
        self._sql = sql
        self.duck_type = duck_type
        self._dependencies = normalise_dependencies(dependencies)

    def render(self) -> str:
        return self._sql

    def __str__(self) -> str:  # pragma: no cover - delegation helper
        return self.render()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{self.__class__.__name__}({self.render()!r}, {self.duck_type!r})"

    @property
    def dependencies(self) -> frozenset[ExpressionDependency]:
        return self._dependencies

    def alias(self, alias: str) -> "AliasedExpression":
        return AliasedExpression(base=self, alias=alias)

    def clone_with_sql(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike],
    ) -> "TypedExpression":
        return type(self)(
            sql,
            duck_type=self.duck_type,
            dependencies=dependencies,
        )

    def cast(self, target: object) -> "TypedExpression":
        """Cast the expression to ``target`` using a strict CAST."""

        from .casting import cast_expression

        return cast_expression(self, target, try_cast=False)

    def try_cast(self, target: object) -> "TypedExpression":
        """Cast the expression to ``target`` using DuckDB's TRY_CAST."""

        from .casting import cast_expression

        return cast_expression(self, target, try_cast=True)

    def _coerce_operand(self, other: object) -> "TypedExpression":
        raise NotImplementedError

    def _binary(self, operator: str, other: object) -> "TypedExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} {operator} {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return type(self)(
            sql,
            duck_type=self.duck_type,
            dependencies=dependencies,
        )

    def over(
        self,
        *,
        partition_by: Sequence[object] | object | None = None,
        order_by: Sequence[object] | object | None = None,
        frame: str | None = None,
    ) -> "TypedExpression":
        """Wrap the expression with a DuckDB window clause."""

        partitions = _normalise_window_operands(partition_by)
        orderings = _normalise_window_operands(order_by)

        dependencies = set(self.dependencies)
        partition_sql: list[str] = []
        for operand in partitions:
            expression = _coerce_window_operand(operand)
            partition_sql.append(expression.render())
            dependencies.update(expression.dependencies)

        order_sql: list[str] = []
        for operand in orderings:
            clause_sql, clause_dependencies = _render_order_operand(operand)
            order_sql.append(clause_sql)
            dependencies.update(clause_dependencies)

        components: list[str] = []
        if partition_sql:
            components.append(f"PARTITION BY {', '.join(partition_sql)}")
        if order_sql:
            components.append(f"ORDER BY {', '.join(order_sql)}")
        if frame is not None:
            frame_sql = frame.strip()
            if not frame_sql:
                msg = "Window frame clause cannot be empty"
                raise ValueError(msg)
            components.append(frame_sql)

        window_spec = " ".join(components)
        window_clause = f"({window_spec})" if components else "()"
        sql = f"({self.render()} OVER {window_clause})"
        return self.clone_with_sql(sql, dependencies=dependencies)


class AliasedExpression(TypedExpression):
    """Expression wrapper adding an alias during rendering."""

    __slots__ = ("_base", "_alias")

    def __init__(self, *, base: TypedExpression, alias: str) -> None:
        self._base = base
        self._alias = alias
        super().__init__(
            f"{base.render()} AS {quote_identifier(alias)}",
            duck_type=base.duck_type,
            dependencies=base.dependencies,
        )

    @property
    def base(self) -> TypedExpression:
        return self._base

    @property
    def alias_name(self) -> str:
        return self._alias

    def _coerce_operand(self, other: object) -> TypedExpression:
        return self._base._coerce_operand(other)


class BooleanExpression(TypedExpression):
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
            duck_type=duck_type or BooleanType("BOOLEAN"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "BooleanExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,))

    @classmethod
    def literal(cls, value: bool) -> "BooleanExpression":
        literal = "TRUE" if value else "FALSE"
        return cls(literal)

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "BooleanExpression":
        return cls(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or BooleanType("BOOLEAN"),
        )

    def _coerce_operand(self, other: object) -> "BooleanExpression":
        if isinstance(other, BooleanExpression):
            return other
        if isinstance(other, bool):
            return type(self).literal(other)
        msg = "Boolean expressions only accept boolean operands"
        raise TypeError(msg)

    def __and__(self, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} AND {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return type(self)(sql, dependencies=dependencies)

    def __or__(self, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} OR {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return type(self)(sql, dependencies=dependencies)

    def __invert__(self) -> "BooleanExpression":
        sql = f"(NOT {self.render()})"
        return type(self)(sql, dependencies=self.dependencies)


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
        value: int | float | Decimal,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        inferred_type = duck_type or infer_numeric_literal_type(value)
        return cls(format_numeric(value), duck_type=inferred_type)

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        return cls(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or NumericType("NUMERIC"),
        )

    def _coerce_operand(self, other: object) -> "NumericExpression":
        if isinstance(other, NumericExpression):
            return other
        if isinstance(other, (int, float, Decimal)):
            return type(self).literal(other)
        msg = "Numeric expressions only accept numeric operands"
        raise TypeError(msg)

    def __add__(self, other: object) -> "NumericExpression":
        return self._binary("+", other)  # type: ignore[return-value]

    def __sub__(self, other: object) -> "NumericExpression":
        return self._binary("-", other)  # type: ignore[return-value]

    def __mul__(self, other: object) -> "NumericExpression":
        return self._binary("*", other)  # type: ignore[return-value]

    def __truediv__(self, other: object) -> "NumericExpression":
        return self._binary("/", other)  # type: ignore[return-value]

    def __mod__(self, other: object) -> "NumericExpression":
        return self._binary("%", other)  # type: ignore[return-value]


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
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,))

    @classmethod
    def literal(
        cls,
        value: str,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "VarcharExpression":
        escaped = value.replace("'", "''")
        return cls(
            f"'{escaped}'",
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
        return cls(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or VarcharType("VARCHAR"),
        )

    def _coerce_operand(self, other: object) -> "VarcharExpression":
        if isinstance(other, VarcharExpression):
            return other
        if isinstance(other, str):
            return type(self).literal(other)
        msg = "Varchar expressions only accept string operands"
        raise TypeError(msg)

    def concat(self, other: object) -> "VarcharExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} || {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return type(self)(sql, dependencies=dependencies)


class GenericExpression(TypedExpression):
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
            duck_type=duck_type or GenericType("UNKNOWN"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "GenericExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,))

    @classmethod
    def _raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "GenericExpression":
        return cls(
            sql,
            dependencies=dependencies,
            duck_type=duck_type or GenericType("UNKNOWN"),
        )

    def _coerce_operand(self, other: object) -> "GenericExpression":
        if isinstance(other, GenericExpression):
            return other
        if isinstance(other, TypedExpression):
            return GenericExpression._raw(
                other.render(),
                dependencies=other.dependencies,
                duck_type=other.duck_type,
            )
        if isinstance(other, str):
            return type(self).column(other)
        msg = "Generic expressions require typed operands"
        raise TypeError(msg)


class TemporalExpression(TypedExpression):
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
            duck_type=duck_type or TemporalType("TIMESTAMP"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
        duck_type: DuckDBType | None = None,
    ) -> "TemporalExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,), duck_type=duck_type)

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
            duck_type=duck_type or TemporalType("TIMESTAMP"),
        )

    def _coerce_operand(self, other: object) -> "TemporalExpression":
        if isinstance(other, TemporalExpression):
            return other
        if isinstance(other, str):
            return type(self).column(other)
        msg = "Temporal expressions only accept temporal operands"
        raise TypeError(msg)


@dataclass(frozen=True, slots=True)
class WindowOrder:
    """Represents an ORDER BY clause within an OVER window."""

    expression: object
    descending: bool = False
    nulls: str | None = None

    def render(self) -> tuple[str, frozenset[ExpressionDependency]]:
        expression = _coerce_window_operand(self.expression)
        sql_parts = [expression.render()]
        if self.descending:
            sql_parts.append("DESC")
        if self.nulls is not None:
            direction = self.nulls.upper()
            if direction not in {"FIRST", "LAST"}:
                msg = "Window null ordering must be 'FIRST' or 'LAST'"
                raise ValueError(msg)
            sql_parts.append(f"NULLS {direction}")
        return " ".join(sql_parts), expression.dependencies


def _normalise_window_operands(values: Sequence[object] | object | None) -> list[object]:
    if values is None:
        return []
    if isinstance(values, (list, tuple)):
        return list(values)
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        return list(values)
    return [values]


def _coerce_window_operand(value: object) -> TypedExpression:
    if isinstance(value, TypedExpression):
        return value
    if isinstance(value, str):
        return GenericExpression.column(value)
    if isinstance(value, tuple) and len(value) == 2:
        table, column = value
        if isinstance(table, str) and isinstance(column, str):
            return GenericExpression.column(column, table=table)
    msg = "Window operands must be typed expressions or column references"
    raise TypeError(msg)


def _render_order_operand(operand: object) -> tuple[str, frozenset[ExpressionDependency]]:
    if isinstance(operand, WindowOrder):
        return operand.render()
    expression = _coerce_window_operand(operand)
    return expression.render(), expression.dependencies


class _ExpressionFactoryMixin(Generic[ExpressionT]):
    expression_type: type[_SupportsFactory[ExpressionT]]

    def column(self, name: str, *, table: str | None = None) -> ExpressionT:
        factory = self.expression_type
        return factory.column(name, table=table)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> ExpressionT:
        factory = self.expression_type
        return factory._raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )


class BooleanFactory(_ExpressionFactoryMixin[BooleanExpression]):
    def __init__(self) -> None:
        self.expression_type = cast(
            type[_SupportsFactory[BooleanExpression]],
            BooleanExpression,
        )

    def literal(self, value: bool) -> BooleanExpression:
        return BooleanExpression.literal(value)

    def coerce(self, operand: object) -> BooleanExpression:
        if isinstance(operand, BooleanExpression):
            return operand
        if isinstance(operand, bool):
            return BooleanExpression.literal(operand)
        msg = "Boolean operands must be expressions or booleans"
        raise TypeError(msg)


class NumericFactory(_ExpressionFactoryMixin[NumericExpression]):
    def __init__(self, expression_type: type[NumericExpression] | None = None) -> None:
        self.expression_type = cast(
            type[_SupportsFactory[NumericExpression]],
            expression_type or NumericExpression,
        )

    def literal(
        self,
        value: int | float | Decimal,
        *,
        duck_type: DuckDBType | None = None,
    ) -> NumericExpression:
        return self.expression_type.literal(value, duck_type=duck_type)

    def coerce(self, operand: object) -> NumericExpression:
        if isinstance(operand, NumericExpression):
            return operand
        if isinstance(operand, (int, float, Decimal)):
            return self.literal(operand)
        if isinstance(operand, str):
            return self.column(operand)
        if isinstance(operand, tuple) and len(operand) == 2:
            table, column = operand
            if isinstance(table, str) and isinstance(column, str):
                return self.column(column, table=table)
        msg = "Unsupported operand for numeric expression"
        raise TypeError(msg)


class VarcharFactory(_ExpressionFactoryMixin[VarcharExpression]):
    def __init__(self) -> None:
        self.expression_type = cast(
            type[_SupportsFactory[VarcharExpression]],
            VarcharExpression,
        )

    def literal(
        self,
        value: str,
        *,
        duck_type: DuckDBType | None = None,
    ) -> VarcharExpression:
        return self.expression_type.literal(value, duck_type=duck_type)

    def coerce(self, operand: object) -> VarcharExpression:
        if isinstance(operand, VarcharExpression):
            return operand
        if isinstance(operand, str):
            return self.literal(operand)
        msg = "Unsupported operand for varchar expression"
        raise TypeError(msg)


class GenericFactory(_ExpressionFactoryMixin[GenericExpression]):
    def __init__(self) -> None:
        self.expression_type = cast(
            type[_SupportsFactory[GenericExpression]],
            GenericExpression,
        )

    def coerce(self, operand: object) -> GenericExpression:
        if isinstance(operand, GenericExpression):
            return operand
        if isinstance(operand, TypedExpression):
            return GenericExpression._raw(
                operand.render(),
                dependencies=operand.dependencies,
                duck_type=operand.duck_type,
            )
        if isinstance(operand, str):
            return self.column(operand)
        msg = "Unsupported operand for generic expression"
        raise TypeError(msg)


class TemporalFactory(_ExpressionFactoryMixin[TemporalExpression]):
    def __init__(self, expression_type: type[TemporalExpression] | None = None) -> None:
        self.expression_type = cast(
            type[_SupportsFactory[TemporalExpression]],
            expression_type or TemporalExpression,
        )

    def coerce(self, operand: object) -> TemporalExpression:
        if isinstance(operand, TemporalExpression):
            return operand
        if isinstance(operand, str):
            return self.column(operand)
        msg = "Unsupported operand for temporal expression"
        raise TypeError(msg)


class DuckTypeNamespace:
    """Static namespace exposing typed expression factories."""

    def __init__(self) -> None:
        self.Numeric = NumericFactory()
        self.Boolean = BooleanFactory()
        self.Varchar = VarcharFactory()
        self.Generic = GenericFactory()
        self.Date = TemporalFactory()
        self.Timestamp = TemporalFactory()
        self._decimal_names: list[str] = []

    def row_number(self) -> NumericExpression:
        return NumericExpression._raw("row_number()")

    @property
    def decimal_factory_names(self) -> tuple[str, ...]:
        return tuple(self._decimal_names)
