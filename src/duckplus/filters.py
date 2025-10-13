"""Filter expression helpers for :class:`duckplus.DuckRel`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal as DecimalValue
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

from . import util
from . import ducktypes as ducktypes_module
from .ducktypes import DuckType, Unknown

DuckTypeMarker = TypeVar("DuckTypeMarker", bound=DuckType, covariant=True)
PythonType = TypeVar("PythonType", covariant=True)

IntegralDuck = TypeVar(
    "IntegralDuck",
    ducktypes_module.TinyInt,
    ducktypes_module.SmallInt,
    ducktypes_module.Integer,
    ducktypes_module.BigInt,
    ducktypes_module.UInteger,
    ducktypes_module.UBigInt,
    ducktypes_module.USmallInt,
    ducktypes_module.UTinyInt,
    ducktypes_module.HugeInt,
)

FloatingDuck = TypeVar(
    "FloatingDuck",
    ducktypes_module.Float,
    ducktypes_module.Double,
)

StringDuck = TypeVar(
    "StringDuck",
    ducktypes_module.StringLike,
    ducktypes_module.Varchar,
)

ColumnDuckType: TypeAlias = (
    type[ducktypes_module.Boolean]
    | type[ducktypes_module.TinyInt]
    | type[ducktypes_module.SmallInt]
    | type[ducktypes_module.Integer]
    | type[ducktypes_module.BigInt]
    | type[ducktypes_module.UInteger]
    | type[ducktypes_module.UBigInt]
    | type[ducktypes_module.USmallInt]
    | type[ducktypes_module.UTinyInt]
    | type[ducktypes_module.HugeInt]
    | type[ducktypes_module.Float]
    | type[ducktypes_module.Double]
    | type[ducktypes_module.Decimal]
    | type[ducktypes_module.Numeric]
    | type[ducktypes_module.StringLike]
    | type[ducktypes_module.Varchar]
    | type[ducktypes_module.Date]
    | type[ducktypes_module.Time]
    | type[ducktypes_module.Timestamp]
    | type[ducktypes_module.TimestampTz]
    | type[ducktypes_module.Interval]
    | type[ducktypes_module.Blob]
    | type[ducktypes_module.Json]
)

__all__ = [
    "ColumnExpression",
    "ColumnReference",
    "FilterExpression",
    "column",
    "col",
    "equals",
    "not_equals",
    "less_than",
    "less_than_or_equal",
    "greater_than",
    "greater_than_or_equal",
]


class ColumnExpression(Generic[DuckTypeMarker, PythonType]):
    """Reference to a column used when building structured expressions."""

    __slots__ = ("_name", "_duck_type")

    def __init__(
        self,
        name: str,
        *,
        duck_type: type[DuckTypeMarker] | None = None,
    ) -> None:
        if not isinstance(name, str):
            raise TypeError(
                "Column name must be provided as a string; "
                f"received {type(name).__name__}."
            )
        if not name:
            raise ValueError("Column name must not be empty.")
        self._name = name
        resolved_type: type[DuckTypeMarker]
        if duck_type is None:
            resolved_type = cast(type[DuckTypeMarker], Unknown)
        else:
            resolved_type = duck_type
        self._duck_type: type[DuckTypeMarker] = resolved_type

    @property
    def name(self) -> str:
        """Return the originally requested column name."""

        return self._name

    @property
    def duck_type(self) -> type[DuckTypeMarker]:
        """Return the declared DuckDB logical type for the column."""

        return self._duck_type

    @property
    def python_annotation(self) -> PythonType:
        """Return the Python type annotation inferred for the column."""

        return cast(PythonType, self._duck_type.python_annotation)

    def resolve(self, available_columns: Sequence[str]) -> str:
        """Return the canonical column name resolved against *available_columns*."""

        return util.resolve_columns([self._name], available_columns)[0]

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the quoted SQL identifier for use in scalar contexts."""

        resolved = self.resolve(available_columns)
        return util.quote_identifier(resolved)

    def render_for_aggregate(self, available_columns: Sequence[str]) -> str:
        """Return the quoted SQL identifier for use in aggregate contexts."""

        return self.render(available_columns)

    def _comparison(self, operator: str, other: "AnyColumnExpression" | Any) -> "FilterExpression":
        if isinstance(other, FilterExpression):
            raise TypeError("Cannot compare a column to a filter expression.")

        if isinstance(other, ColumnExpression):
            node: _Node = _ComparisonNode(self, operator, _ColumnOperand(other))
        else:
            coerced = util.coerce_scalar(other)
            node = _ComparisonNode(self, operator, _LiteralOperand(coerced))
        return FilterExpression(node)

    def __eq__(self, other: object) -> "FilterExpression":  # type: ignore[override]
        if isinstance(other, FilterExpression):
            raise TypeError("Cannot compare a column to a filter expression.")
        return self._comparison("=", other)

    def __ne__(self, other: object) -> "FilterExpression":  # type: ignore[override]
        if isinstance(other, FilterExpression):
            raise TypeError("Cannot compare a column to a filter expression.")
        return self._comparison("!=", other)

    def __lt__(self, other: Any) -> "FilterExpression":
        return self._comparison("<", other)

    def __le__(self, other: Any) -> "FilterExpression":
        return self._comparison("<=", other)

    def __gt__(self, other: Any) -> "FilterExpression":
        return self._comparison(">", other)

    def __ge__(self, other: Any) -> "FilterExpression":
        return self._comparison(">=", other)


AnyColumnExpression: TypeAlias = ColumnExpression[DuckType, Any]
ColumnReference: TypeAlias = AnyColumnExpression


@overload
def column(name: str, *, duck_type: None = None) -> ColumnExpression[Unknown, Any]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Boolean]
) -> ColumnExpression[ducktypes_module.Boolean, bool]:
    ...


@overload
def column(name: str, *, duck_type: type[IntegralDuck]) -> ColumnExpression[IntegralDuck, int]:
    ...


@overload
def column(name: str, *, duck_type: type[FloatingDuck]) -> ColumnExpression[FloatingDuck, float]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Decimal]
) -> ColumnExpression[ducktypes_module.Decimal, DecimalValue]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Numeric]
) -> ColumnExpression[ducktypes_module.Numeric, int | float | DecimalValue]:
    ...


@overload
def column(name: str, *, duck_type: type[StringDuck]) -> ColumnExpression[StringDuck, str]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Date]
) -> ColumnExpression[ducktypes_module.Date, date]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Time]
) -> ColumnExpression[ducktypes_module.Time, time]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Timestamp]
) -> ColumnExpression[ducktypes_module.Timestamp, datetime]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.TimestampTz]
) -> ColumnExpression[ducktypes_module.TimestampTz, datetime]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Interval]
) -> ColumnExpression[ducktypes_module.Interval, timedelta]:
    ...


@overload
def column(
    name: str, *, duck_type: type[ducktypes_module.Blob]
) -> ColumnExpression[ducktypes_module.Blob, bytes | bytearray | memoryview]:
    ...


@overload
def column(name: str, *, duck_type: type[ducktypes_module.Json]) -> ColumnExpression[ducktypes_module.Json, Any]:
    ...


def column(
    name: str,
    *,
    duck_type: ColumnDuckType | None = None,
) -> AnyColumnExpression:
    """Return a :class:`ColumnExpression` for *name* with optional typing.

    When *duck_type* references a :mod:`duckplus.ducktypes` marker the resulting
    expression participates in runtime and static validation for compatible
    aggregates and ordering clauses. Omitting *duck_type* preserves the prior
    untyped behavior.
    """

    resolved_type: type[DuckType]
    if duck_type is None:
        resolved_type = Unknown
    else:
        resolved_type = duck_type
    expression: AnyColumnExpression = ColumnExpression(name, duck_type=resolved_type)
    return expression


@overload
def col(name: str, *, duck_type: None = None) -> ColumnExpression[Unknown, Any]:
    ...


@overload
def col(
    name: str, *, duck_type: type[ducktypes_module.Boolean]
) -> ColumnExpression[ducktypes_module.Boolean, bool]:
    ...


@overload
def col(name: str, *, duck_type: type[IntegralDuck]) -> ColumnExpression[IntegralDuck, int]:
    ...


@overload
def col(name: str, *, duck_type: type[FloatingDuck]) -> ColumnExpression[FloatingDuck, float]:
    ...


@overload
def col(
    name: str, *, duck_type: type[ducktypes_module.Decimal]
) -> ColumnExpression[ducktypes_module.Decimal, DecimalValue]:
    ...


@overload
def col(
    name: str, *, duck_type: type[ducktypes_module.Numeric]
) -> ColumnExpression[ducktypes_module.Numeric, int | float | DecimalValue]:
    ...


@overload
def col(name: str, *, duck_type: type[StringDuck]) -> ColumnExpression[StringDuck, str]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Date]) -> ColumnExpression[ducktypes_module.Date, date]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Time]) -> ColumnExpression[ducktypes_module.Time, time]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Timestamp]) -> ColumnExpression[ducktypes_module.Timestamp, datetime]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.TimestampTz]) -> ColumnExpression[ducktypes_module.TimestampTz, datetime]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Interval]) -> ColumnExpression[ducktypes_module.Interval, timedelta]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Blob]) -> ColumnExpression[ducktypes_module.Blob, bytes | bytearray | memoryview]:
    ...


@overload
def col(name: str, *, duck_type: type[ducktypes_module.Json]) -> ColumnExpression[ducktypes_module.Json, Any]:
    ...


def col(
    name: str,
    *,
    duck_type: ColumnDuckType | None = None,
) -> AnyColumnExpression:
    """Alias for :func:`column`."""

    if duck_type is None:
        return column(name)
    return column(name, duck_type=duck_type)


class FilterExpression:
    """Structured filter expression that renders to SQL with validation."""

    __slots__ = ("_node",)

    def __init__(self, node: "_Node") -> None:
        self._node = node

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the SQL expression ensuring referenced columns exist."""

        resolver = _ColumnResolver(available_columns, self._node.columns())
        return self._node.render(resolver.lookup)

    def _columns(self) -> tuple[AnyColumnExpression, ...]:
        """Return the column references used within the expression."""

        return self._node.columns()

    def _render_with_resolver(
        self, resolver: Callable[[AnyColumnExpression], str]
    ) -> str:
        """Render the expression using *resolver* for column lookups."""

        return self._node.render(resolver)

    def __and__(self, other: "FilterExpression") -> "FilterExpression":
        if not isinstance(other, FilterExpression):
            raise TypeError("Filters can only be combined with other FilterExpression instances.")
        return FilterExpression(_CompoundNode("AND", self._node, other._node))

    def __rand__(self, other: "FilterExpression") -> "FilterExpression":
        if not isinstance(other, FilterExpression):
            raise TypeError("Filters can only be combined with other FilterExpression instances.")
        return other.__and__(self)

    def __or__(self, other: "FilterExpression") -> "FilterExpression":
        if not isinstance(other, FilterExpression):
            raise TypeError("Filters can only be combined with other FilterExpression instances.")
        return FilterExpression(_CompoundNode("OR", self._node, other._node))

    def __ror__(self, other: "FilterExpression") -> "FilterExpression":
        if not isinstance(other, FilterExpression):
            raise TypeError("Filters can only be combined with other FilterExpression instances.")
        return other.__or__(self)

    @classmethod
    def raw(cls, expression: str) -> "FilterExpression":
        """Return a filter expression using the provided SQL fragment."""

        if not isinstance(expression, str):
            raise TypeError(
                "Raw filter expressions must be strings; "
                f"received {type(expression).__name__}."
            )
        if not expression.strip():
            raise ValueError("Raw filter expression must not be empty.")
        return cls(_RawNode(expression))


def _combine_conditions(
    operator: str, conditions: Mapping[str, AnyColumnExpression | Any]
) -> FilterExpression:
    if not conditions:
        raise ValueError("At least one condition is required to build a filter.")

    expressions: Iterable[FilterExpression] = (
        column(name)._comparison(operator, value) for name, value in conditions.items()
    )

    iterator = iter(expressions)
    result = next(iterator)
    for expr in iterator:
        result = result & expr
    return result


def equals(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return an equality filter for the provided *conditions*."""

    return _combine_conditions("=", conditions)


def not_equals(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return a non-equality filter for the provided *conditions*."""

    return _combine_conditions("!=", conditions)


def less_than(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return a less-than filter for the provided *conditions*."""

    return _combine_conditions("<", conditions)


def less_than_or_equal(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return a less-than-or-equal filter for the provided *conditions*."""

    return _combine_conditions("<=", conditions)


def greater_than(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return a greater-than filter for the provided *conditions*."""

    return _combine_conditions(">", conditions)


def greater_than_or_equal(**conditions: AnyColumnExpression | Any) -> FilterExpression:
    """Return a greater-than-or-equal filter for the provided *conditions*."""

    return _combine_conditions(">=", conditions)


class _ColumnOperand:
    __slots__ = ("_column",)

    def __init__(self, column: AnyColumnExpression) -> None:
        self._column = column

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        return (self._column,)

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:
        return resolver(self._column)


class _LiteralOperand:
    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        return ()

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:  # noqa: ARG002
        return util.format_sql_literal(self._value)


class _Node:
    __slots__ = ()

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        raise NotImplementedError

    @property
    def precedence(self) -> int:
        raise NotImplementedError

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:
        raise NotImplementedError


@dataclass(slots=True)
class _ComparisonNode(_Node):
    left: AnyColumnExpression
    operator: str
    right: _ColumnOperand | _LiteralOperand

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        return (self.left, *self.right.columns())

    @property
    def precedence(self) -> int:
        return 3

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:
        left_sql = resolver(self.left)
        right_sql = self.right.render(resolver)
        return f"{left_sql} {self.operator} {right_sql}"


@dataclass(slots=True)
class _CompoundNode(_Node):
    operator: str
    left: _Node
    right: _Node

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        return (*self.left.columns(), *self.right.columns())

    @property
    def precedence(self) -> int:
        return 2 if self.operator == "AND" else 1

    def _render_child(
        self, child: _Node, resolver: Callable[[AnyColumnExpression], str]
    ) -> str:
        sql = child.render(resolver)
        if child.precedence < self.precedence:
            return f"({sql})"
        return sql

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:
        left_sql = self._render_child(self.left, resolver)
        right_sql = self._render_child(self.right, resolver)
        return f"{left_sql} {self.operator} {right_sql}"


@dataclass(slots=True)
class _RawNode(_Node):
    expression: str

    def columns(self) -> tuple[AnyColumnExpression, ...]:
        return ()

    @property
    def precedence(self) -> int:
        return 4

    def render(self, resolver: Callable[[AnyColumnExpression], str]) -> str:  # noqa: ARG002
        return self.expression


class _ColumnResolver:
    """Resolve requested column names against available relation metadata."""

    __slots__ = ("_mapping",)

    def __init__(
        self, available: Sequence[str], references: Sequence[AnyColumnExpression]
    ) -> None:
        mapping: dict[str, str] = {}
        requested: list[str] = []
        for reference in references:
            key = reference.name.casefold()
            if key in mapping:
                continue
            requested.append(reference.name)
            mapping[key] = ""

        if requested:
            resolved = util.resolve_columns(requested, available)
            for name, canonical in zip(requested, resolved, strict=True):
                mapping[name.casefold()] = canonical

        self._mapping = mapping

    def lookup(self, reference: AnyColumnExpression) -> str:
        canonical = self._mapping.get(reference.name.casefold())
        if canonical is None:
            raise KeyError(
                f"Column {reference.name!r} was not resolved; available columns were validated."
            )
        return util.quote_identifier(canonical)

