"""Filter expression helpers for :class:`duckplus.DuckRel`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from . import util

__all__ = [
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


class ColumnReference:
    """Reference to a column used when building filter expressions."""

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError(
                "Column name must be provided as a string; "
                f"received {type(name).__name__}."
            )
        if not name:
            raise ValueError("Column name must not be empty.")
        self._name = name

    @property
    def name(self) -> str:
        """Return the originally requested column name."""

        return self._name

    def _comparison(self, operator: str, other: ColumnReference | Any) -> "FilterExpression":
        if isinstance(other, FilterExpression):
            raise TypeError("Cannot compare a column to a filter expression.")

        if isinstance(other, ColumnReference):
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


def column(name: str) -> ColumnReference:
    """Return a :class:`ColumnReference` for *name*."""

    return ColumnReference(name)


def col(name: str) -> ColumnReference:
    """Alias for :func:`column`."""

    return column(name)


class FilterExpression:
    """Structured filter expression that renders to SQL with validation."""

    __slots__ = ("_node",)

    def __init__(self, node: "_Node") -> None:
        self._node = node

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the SQL expression ensuring referenced columns exist."""

        resolver = _ColumnResolver(available_columns, self._node.columns())
        return self._node.render(resolver.lookup)

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
    operator: str, conditions: Mapping[str, ColumnReference | Any]
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


def equals(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return an equality filter for the provided *conditions*."""

    return _combine_conditions("=", conditions)


def not_equals(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return a non-equality filter for the provided *conditions*."""

    return _combine_conditions("!=", conditions)


def less_than(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return a less-than filter for the provided *conditions*."""

    return _combine_conditions("<", conditions)


def less_than_or_equal(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return a less-than-or-equal filter for the provided *conditions*."""

    return _combine_conditions("<=", conditions)


def greater_than(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return a greater-than filter for the provided *conditions*."""

    return _combine_conditions(">", conditions)


def greater_than_or_equal(**conditions: ColumnReference | Any) -> FilterExpression:
    """Return a greater-than-or-equal filter for the provided *conditions*."""

    return _combine_conditions(">=", conditions)


class _ColumnOperand:
    __slots__ = ("_column",)

    def __init__(self, column: ColumnReference) -> None:
        self._column = column

    def columns(self) -> tuple[ColumnReference, ...]:
        return (self._column,)

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:
        return resolver(self._column)


class _LiteralOperand:
    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    def columns(self) -> tuple[ColumnReference, ...]:
        return ()

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:  # noqa: ARG002
        return util.format_sql_literal(self._value)


class _Node:
    __slots__ = ()

    def columns(self) -> tuple[ColumnReference, ...]:
        raise NotImplementedError

    @property
    def precedence(self) -> int:
        raise NotImplementedError

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:
        raise NotImplementedError


@dataclass(slots=True)
class _ComparisonNode(_Node):
    left: ColumnReference
    operator: str
    right: _ColumnOperand | _LiteralOperand

    def columns(self) -> tuple[ColumnReference, ...]:
        return (self.left, *self.right.columns())

    @property
    def precedence(self) -> int:
        return 3

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:
        left_sql = resolver(self.left)
        right_sql = self.right.render(resolver)
        return f"{left_sql} {self.operator} {right_sql}"


@dataclass(slots=True)
class _CompoundNode(_Node):
    operator: str
    left: _Node
    right: _Node

    def columns(self) -> tuple[ColumnReference, ...]:
        return (*self.left.columns(), *self.right.columns())

    @property
    def precedence(self) -> int:
        return 2 if self.operator == "AND" else 1

    def _render_child(
        self, child: _Node, resolver: Callable[[ColumnReference], str]
    ) -> str:
        sql = child.render(resolver)
        if child.precedence < self.precedence:
            return f"({sql})"
        return sql

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:
        left_sql = self._render_child(self.left, resolver)
        right_sql = self._render_child(self.right, resolver)
        return f"{left_sql} {self.operator} {right_sql}"


@dataclass(slots=True)
class _RawNode(_Node):
    expression: str

    def columns(self) -> tuple[ColumnReference, ...]:
        return ()

    @property
    def precedence(self) -> int:
        return 4

    def render(self, resolver: Callable[[ColumnReference], str]) -> str:  # noqa: ARG002
        return self.expression


class _ColumnResolver:
    """Resolve requested column names against available relation metadata."""

    __slots__ = ("_mapping",)

    def __init__(
        self, available: Sequence[str], references: Sequence[ColumnReference]
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

    def lookup(self, reference: ColumnReference) -> str:
        canonical = self._mapping.get(reference.name.casefold())
        if canonical is None:
            raise KeyError(
                f"Column {reference.name!r} was not resolved; available columns were validated."
            )
        return util.quote_identifier(canonical)

