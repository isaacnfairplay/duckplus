"""Typed expression primitives used for building DuckDB SQL."""

# pylint: disable=too-few-public-methods,missing-function-docstring,missing-class-docstring,invalid-name,import-outside-toplevel

from __future__ import annotations

from decimal import Decimal
from types import NotImplementedType
from typing import TYPE_CHECKING, Callable, Iterable, TypeVar, Union

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checking
    from .functions import DuckDBFunctionNamespace


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def _quote_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


ExpressionT = TypeVar("ExpressionT", bound="TypedExpression")
ComparisonResult = Union["BooleanExpression", NotImplementedType]


class TypedExpression:
    """Representation of a typed SQL expression."""

    __slots__ = ("_sql", "type_annotation", "_dependencies")

    def __init__(
        self,
        sql: str,
        *,
        type_annotation: str,
        dependencies: Iterable[str] = (),
    ) -> None:
        self._sql = sql
        self.type_annotation = type_annotation
        self._dependencies = frozenset(dependencies)

    def render(self) -> str:
        """Return a SQL representation of the expression."""

        return self._sql

    def __str__(self) -> str:  # pragma: no cover - delegation to ``render``
        return self.render()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{self.__class__.__name__}({self.render()!r})"

    @property
    def dependencies(self) -> frozenset[str]:
        """Columns referenced by the expression."""

        return self._dependencies

    def alias(self, alias: str) -> "AliasedExpression":
        """Return an aliased version of the expression."""

        return AliasedExpression(base=self, alias=alias)

    # Comparisons -----------------------------------------------------
    def _comparison(self: ExpressionT, operator: str, other: object) -> "BooleanExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} {operator} {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return BooleanExpression(sql, dependencies=dependencies)

    def _coerce_operand(self: ExpressionT, other: object) -> ExpressionT:
        raise NotImplementedError

    def __eq__(self, other: object) -> ComparisonResult:  # type: ignore[override]
        if isinstance(other, (TypedExpression, str, int, float, Decimal, bool, bytes)):
            return self._comparison("=", other)
        return NotImplemented

    def __ne__(self, other: object) -> ComparisonResult:  # type: ignore[override]
        if isinstance(other, (TypedExpression, str, int, float, Decimal, bool, bytes)):
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
            type_annotation=base.type_annotation,
            dependencies=base.dependencies,
        )

    def render(self) -> str:
        return f"{self.base.render()} AS {_quote_identifier(self.alias_name)}"

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
        type_annotation: str = "BOOLEAN",
    ) -> None:
        super().__init__(sql, type_annotation=type_annotation, dependencies=dependencies)

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


NumericOperand = int | float | Decimal


class NumericExpression(TypedExpression):
    """Numeric expressions provide arithmetic helpers."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "NUMERIC",
    ) -> None:
        super().__init__(sql, type_annotation=type_annotation, dependencies=dependencies)

    @classmethod
    def column(cls, name: str) -> "NumericExpression":
        return cls(_quote_identifier(name), dependencies=(name,))

    @classmethod
    def literal(
        cls,
        value: NumericOperand,
        *,
        type_annotation: str = "NUMERIC",
    ) -> "NumericExpression":
        return cls(_format_numeric(value), type_annotation=type_annotation)

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "NUMERIC",
    ) -> "NumericExpression":
        return cls(sql, dependencies=dependencies, type_annotation=type_annotation)

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

class VarcharExpression(TypedExpression):
    """Varchar expressions enable string comparisons."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "VARCHAR",
    ) -> None:
        super().__init__(sql, type_annotation=type_annotation, dependencies=dependencies)

    @classmethod
    def column(cls, name: str) -> "VarcharExpression":
        return cls(_quote_identifier(name), dependencies=(name,))

    @classmethod
    def literal(
        cls,
        value: str,
        *,
        type_annotation: str = "VARCHAR",
    ) -> "VarcharExpression":
        return cls(_quote_string(value), type_annotation=type_annotation)

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "VARCHAR",
    ) -> "VarcharExpression":
        return cls(sql, dependencies=dependencies, type_annotation=type_annotation)

    def _coerce_operand(self, other: object) -> "VarcharExpression":
        if isinstance(other, VarcharExpression):
            return other
        if isinstance(other, str):
            return VarcharExpression.literal(other)
        msg = "Varchar expressions only accept string operands"
        raise TypeError(msg)


class BlobExpression(TypedExpression):
    """Binary expressions represent DuckDB BLOB values."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "BLOB",
    ) -> None:
        super().__init__(sql, type_annotation=type_annotation, dependencies=dependencies)

    @classmethod
    def column(cls, name: str) -> "BlobExpression":
        return cls(_quote_identifier(name), dependencies=(name,))

    @classmethod
    def literal(
        cls,
        value: bytes,
        *,
        type_annotation: str = "BLOB",
    ) -> "BlobExpression":
        hex_literal = value.hex()
        return cls(f"BLOB '\\x{hex_literal}'", type_annotation=type_annotation)

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "BLOB",
    ) -> "BlobExpression":
        return cls(sql, dependencies=dependencies, type_annotation=type_annotation)

    def _coerce_operand(self, other: object) -> "BlobExpression":
        if isinstance(other, BlobExpression):
            return other
        if isinstance(other, bytes):
            return BlobExpression.literal(other)
        msg = "Blob expressions only accept bytes operands"
        raise TypeError(msg)


class GenericExpression(TypedExpression):
    """Representation of a DuckDB expression with unknown concrete type."""

    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        type_annotation: str = "UNKNOWN",
        dependencies: Iterable[str] = (),
    ) -> None:
        super().__init__(sql, type_annotation=type_annotation, dependencies=dependencies)

    def _coerce_operand(self, other: object) -> "GenericExpression":
        if isinstance(other, TypedExpression):
            return GenericExpression(
                other.render(),
                type_annotation=other.type_annotation,
                dependencies=other.dependencies,
            )
        msg = "Generic expressions only accept other SQL expressions"
        raise TypeError(msg)


class _NumericAggregateFactory:
    def __init__(
        self,
        factory: "NumericFactory",
        *,
        function_namespace: "DuckDBFunctionNamespace | None" = None,
    ) -> None:
        self._factory = factory
        self._function_namespace = function_namespace

    def _from_function_namespace(
        self, name: str
    ) -> Callable[..., NumericExpression] | None:
        if self._function_namespace is None:
            return None
        try:
            accessor = self._function_namespace.Aggregate.Numeric
        except RuntimeError:
            return None
        if hasattr(accessor, name):
            function = getattr(accessor, name)
            return function  # type: ignore[return-value]
        try:
            function = accessor[name]
        except KeyError:
            return None
        return function  # type: ignore[return-value]

    def __getattr__(self, name: str):
        function = self._from_function_namespace(name)
        if function is None:
            msg = f"Aggregate function '{name}' is not available"
            raise AttributeError(msg) from None

        def wrapper(*operands: object) -> NumericExpression:
            return function(*operands)

        return wrapper

    def sum(self, operand: object) -> NumericExpression:
        function = self._from_function_namespace("sum")
        if function is not None:
            return function(operand)
        expression = self._factory.coerce(operand)
        sql = f"sum({expression.render()})"
        return NumericExpression(sql, dependencies=expression.dependencies)


class NumericFactory:
    """Factory for creating numeric expressions."""

    def __init__(
        self,
        function_namespace: "DuckDBFunctionNamespace | None" = None,
    ) -> None:
        self._function_namespace = function_namespace

    def __call__(self, column: str) -> NumericExpression:
        return NumericExpression.column(column)

    def literal(self, value: NumericOperand) -> NumericExpression:
        return NumericExpression.literal(value)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        type_annotation: str = "NUMERIC",
    ) -> NumericExpression:
        return NumericExpression.raw(
            sql,
            dependencies=dependencies,
            type_annotation=type_annotation,
        )

    def coerce(self, operand: object) -> NumericExpression:
        if isinstance(operand, NumericExpression):
            return operand
        if isinstance(operand, str):
            return self(operand)
        if isinstance(operand, (int, float, Decimal)):
            return self.literal(operand)
        msg = "Unsupported operand for numeric expression"
        raise TypeError(msg)

    @property
    def Aggregate(self) -> _NumericAggregateFactory:
        return _NumericAggregateFactory(
            self,
            function_namespace=self._function_namespace,
        )


class VarcharFactory:
    """Factory for creating varchar expressions."""

    def __call__(self, column: str) -> VarcharExpression:
        return VarcharExpression.column(column)

    def literal(self, value: str) -> VarcharExpression:
        return VarcharExpression.literal(value)

    def raw(self, sql: str, *, dependencies: Iterable[str] = ()) -> VarcharExpression:
        return VarcharExpression.raw(sql, dependencies=dependencies)

    def coerce(self, operand: object) -> VarcharExpression:
        if isinstance(operand, VarcharExpression):
            return operand
        if isinstance(operand, str):
            return self.literal(operand)
        msg = "Unsupported operand for varchar expression"
        raise TypeError(msg)


class BooleanFactory:
    """Factory for creating boolean expressions."""

    def __call__(self, column: str) -> BooleanExpression:
        return BooleanExpression(_quote_identifier(column), dependencies=(column,))

    def literal(self, value: bool) -> BooleanExpression:
        return BooleanExpression("TRUE" if value else "FALSE")

    def raw(self, sql: str, *, dependencies: Iterable[str] = ()) -> BooleanExpression:
        return BooleanExpression(sql, dependencies=dependencies)

    def coerce(self, operand: object) -> BooleanExpression:
        if isinstance(operand, BooleanExpression):
            return operand
        if isinstance(operand, bool):
            return self.literal(operand)
        msg = "Boolean operands must be expression or bool"
        raise TypeError(msg)


class BlobFactory:
    """Factory for creating blob expressions."""

    def __call__(self, column: str) -> BlobExpression:
        return BlobExpression.column(column)

    def literal(self, value: bytes) -> BlobExpression:
        return BlobExpression.literal(value)

    def raw(self, sql: str, *, dependencies: Iterable[str] = ()) -> BlobExpression:
        return BlobExpression.raw(sql, dependencies=dependencies)

    def coerce(self, operand: object) -> BlobExpression:
        if isinstance(operand, BlobExpression):
            return operand
        if isinstance(operand, bytes):
            return self.literal(operand)
        msg = "Unsupported operand for blob expression"
        raise TypeError(msg)


class DuckTypeNamespace:
    """Container exposing typed expression factories and DuckDB functions."""

    def __init__(self) -> None:
        from .functions import DuckDBFunctionNamespace  # Local import to avoid cycle

        functions = DuckDBFunctionNamespace()
        self.Functions = functions
        self.Numeric = NumericFactory(functions)
        self.Varchar = VarcharFactory()
        self.Boolean = BooleanFactory()
        self.Blob = BlobFactory()


def _format_numeric(value: NumericOperand) -> str:
    if isinstance(value, bool):  # bool is subclass of int, exclude early
        raise TypeError("Boolean values are not valid numeric literals")
    if isinstance(value, Decimal):
        return format(value, "f")
    return repr(value)


ducktype = DuckTypeNamespace()
