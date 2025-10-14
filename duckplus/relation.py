"""Utilities for working with DuckDB relations."""

from __future__ import annotations

from dataclasses import dataclass, field

import duckdb

from .duckcon import DuckCon


@dataclass(frozen=True)
class Relation:
    """Immutable wrapper around a DuckDB relation.

    The wrapper keeps track of the :class:`~duckplus.duckcon.DuckCon` that
    produced the relation together with cached metadata describing the
    relation's column names and DuckDB data types.
    """

    duckcon: DuckCon
    _relation: duckdb.DuckDBPyRelation
    _columns: tuple[str, ...] = field(init=False, repr=False)
    _types: tuple[str, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        columns = tuple(self._relation.columns)
        # DuckDB returns custom type objects in ``relation.types`` so we cast
        # them to their string representation for stable comparison.
        types = tuple(str(type_) for type_ in self._relation.types)
        object.__setattr__(self, "_columns", columns)
        object.__setattr__(self, "_types", types)

    @property
    def columns(self) -> tuple[str, ...]:
        """Return the column names of the wrapped relation."""

        return self._columns

    @property
    def types(self) -> tuple[str, ...]:
        """Return the DuckDB data types associated with the relation."""

        return self._types

    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        """Expose the underlying DuckDB relation."""

        return self._relation

    @classmethod
    def from_relation(cls, duckcon: DuckCon, relation: duckdb.DuckDBPyRelation) -> "Relation":
        """Create a :class:`Relation` from an existing DuckDB relation."""

        return cls(duckcon=duckcon, _relation=relation)

    @classmethod
    def from_sql(cls, duckcon: DuckCon, query: str) -> "Relation":
        """Create a relation from a SQL query executed on a managed connection."""

        connection = duckcon.connection
        relation = connection.sql(query)
        return cls.from_relation(duckcon, relation)
