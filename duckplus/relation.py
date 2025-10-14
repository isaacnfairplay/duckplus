"""Utilities for working with DuckDB relations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, overload

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

    @overload
    def transform(self, **replacements: str) -> "Relation":
        ...

    @overload
    def transform(
        self,
        **replacements: type[int] | type[float] | type[str] | type[bool] | type[bytes],
    ) -> "Relation":
        ...

    def transform(self, **replacements: object) -> "Relation":
        """Return a new relation with selected columns replaced.

        The helper issues a ``SELECT * REPLACE`` statement against the underlying
        DuckDB relation and validates that referenced columns exist. Replacement
        expressions can be provided directly as SQL snippets or as simple Python
        types (``int``, ``float``, ``str``, ``bool``, ``bytes``) which will be
        translated into DuckDB casts, e.g. ``relation.transform(total=int)``.
        """

        if not replacements:
            msg = "transform requires at least one replacement"
            raise ValueError(msg)

        if not self.duckcon.is_open:
            msg = (
                "DuckCon connection must be open to call transform. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        missing = sorted(set(replacements) - set(self.columns))
        if missing:
            msg = f"Columns do not exist on relation: {', '.join(missing)}"
            raise KeyError(msg)

        replace_clauses = []
        for column, value in replacements.items():
            expression = self._normalise_transform_value(column, value)
            alias = self._quote_identifier(column)
            replace_clauses.append(f"{expression} AS {alias}")

        select_list = f"* REPLACE ({', '.join(replace_clauses)})"

        try:
            relation = self._relation.project(select_list)
        except duckdb.BinderException as error:
            msg = "transform expression references unknown columns"
            raise ValueError(msg) from error

        return type(self).from_relation(self.duckcon, relation)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped = identifier.replace("\"", "\"\"")
        return f'"{escaped}"'

    @classmethod
    def _normalise_transform_value(cls, column: str, value: object) -> str:
        if isinstance(value, str):
            expression = value.strip()
            if not expression:
                msg = f"Replacement for column '{column}' cannot be empty"
                raise ValueError(msg)
            return expression

        if isinstance(value, type):
            duck_type = cls._python_type_to_duckdb(value)
            identifier = cls._quote_identifier(column)
            return f"{identifier}::{duck_type}"

        msg = (
            "transform replacements must be SQL strings or simple Python types "
            f"(got {type(value)!r})"
        )
        raise TypeError(msg)

    @staticmethod
    def _python_type_to_duckdb(python_type: type[object]) -> str:
        mapping: Mapping[type[object], str]
        mapping = {
            int: "INTEGER",
            float: "DOUBLE",
            str: "VARCHAR",
            bool: "BOOLEAN",
            bytes: "BLOB",
        }

        try:
            return mapping[python_type]
        except KeyError as error:
            msg = f"Unsupported cast target for transform: {python_type!r}"
            raise TypeError(msg) from error
