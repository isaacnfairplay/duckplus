"""Dependency tracking utilities for typed expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ExpressionDependency:
    """Represents a dependency on a column or table."""

    column_name: str | None = None
    table_name: str | None = None

    def __post_init__(self) -> None:
        if self.column_name is None and self.table_name is None:
            msg = "Dependency must reference a column or table"
            raise ValueError(msg)
        if self.column_name is not None and not self.column_name:
            msg = "Column dependency name cannot be empty"
            raise ValueError(msg)
        if self.table_name is not None and not self.table_name:
            msg = "Table dependency name cannot be empty"
            raise ValueError(msg)

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "ExpressionDependency":
        """Create a column dependency optionally qualified with a table."""
        return cls(column_name=name, table_name=table)

    @classmethod
    def table(cls, name: str) -> "ExpressionDependency":
        """Create a dependency representing an entire table."""
        return cls(table_name=name)


DependencyLike = (
    ExpressionDependency
    | str
    | tuple[str]
    | tuple[str, str | None]
)


def _coerce_dependency(value: DependencyLike) -> ExpressionDependency:
    if isinstance(value, ExpressionDependency):
        return value
    if isinstance(value, str):
        return ExpressionDependency.column(value)
    if isinstance(value, tuple):
        if len(value) == 1:
            (table,) = value
            if not isinstance(table, str):
                msg = "Table dependency tuple must contain a string"
                raise TypeError(msg)
            return ExpressionDependency.table(table)
        if len(value) == 2:
            table, column = value
            if not isinstance(table, str):
                msg = "Table dependency must be a string"
                raise TypeError(msg)
            if column is None:
                return ExpressionDependency.table(table)
            if not isinstance(column, str):
                msg = "Column dependency must be a string or None"
                raise TypeError(msg)
            return ExpressionDependency.column(column, table=table)
    msg = (
        "Dependencies must be ExpressionDependency, column name string, "
        "or (table, column) tuple"
    )
    raise TypeError(msg)


def normalise_dependencies(
    dependencies: Iterable[DependencyLike],
) -> frozenset[ExpressionDependency]:
    """Convert arbitrary dependency inputs into a normalised frozenset."""
    if isinstance(dependencies, frozenset) and all(
        isinstance(value, ExpressionDependency) for value in dependencies
    ):
        return dependencies
    normalised: set[ExpressionDependency] = set()
    for dependency in dependencies:
        normalised.add(_coerce_dependency(dependency))
    return frozenset(normalised)
