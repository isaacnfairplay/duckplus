"""Core relational wrappers for Duck+."""

from __future__ import annotations

from dataclasses import dataclass, field

import duckdb

from .connect import DuckConnection


def _quote_identifier(identifier: str) -> str:
    """Return an identifier quoted for DuckDB."""

    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


@dataclass(frozen=True, slots=True)
class DuckRel:
    """Immutable wrapper around a :class:`duckdb.DuckDBPyRelation`."""

    _connection: DuckConnection
    _relation: duckdb.DuckDBPyRelation
    _columns: tuple[str, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Ensure columns are cached on instantiation to avoid reliance on DuckDB
        # defaults later. ``DuckDBPyRelation.columns`` already returns a list of
        # strings, so it can be stored as an immutable tuple.
        object.__setattr__(self, "_columns", tuple(self._relation.columns))

    @property
    def connection(self) -> DuckConnection:
        """Return the connection associated with the relation."""

        return self._connection

    @property
    def raw(self) -> duckdb.DuckDBPyRelation:
        """Expose the underlying :class:`duckdb.DuckDBPyRelation`."""

        return self._relation

    @property
    def columns(self) -> tuple[str, ...]:
        """Return the relation columns preserving their original case."""

        return self._columns

    @property
    def columns_lower(self) -> tuple[str, ...]:
        """Return the columns lower-cased in positional order."""

        return tuple(col.lower() for col in self._columns)

    @property
    def columns_lower_set(self) -> frozenset[str]:
        """Return a case-insensitive lookup set for the columns."""

        return frozenset(col.lower() for col in self._columns)

    def select(self, *columns: str) -> "DuckRel":
        """Project the relation onto a subset of columns."""

        if not columns:
            raise ValueError("select() requires at least one column")

        missing = [name for name in columns if name not in self._columns]
        if missing:
            missing_display = ", ".join(sorted(missing))
            raise KeyError(f"Columns not found: {missing_display}")

        projection = ", ".join(_quote_identifier(name) for name in columns)
        projected = self._relation.project(projection)
        return DuckRel(self._connection, projected)

    @classmethod
    def from_table(cls, connection: DuckConnection, table_name: str) -> "DuckRel":
        """Create a relation that references an existing DuckDB table."""

        relation = connection.raw.table(table_name)
        return cls(connection, relation)
