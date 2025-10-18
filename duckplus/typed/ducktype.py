"""Ergonomic aliases for the :mod:`duckplus.typed.expression` namespace."""

# pylint: disable=invalid-name

from __future__ import annotations

from .expression import DuckTypeNamespace, SelectStatementBuilder, ducktype as _ducktype

# Re-export the global namespace instance so importing from this module provides
# an intuitive entrypoint for ergonomic factory access.
ducktype: DuckTypeNamespace = _ducktype

# Factories exposed on the shared ``ducktype`` namespace. These aliases make it
# straightforward to import just the factories without interacting with the
# namespace container directly.
Numeric = ducktype.Numeric
Varchar = ducktype.Varchar
Boolean = ducktype.Boolean
Blob = ducktype.Blob
Generic = ducktype.Generic
Tinyint = ducktype.Tinyint
Smallint = ducktype.Smallint
Integer = ducktype.Integer
Utinyint = ducktype.Utinyint
Usmallint = ducktype.Usmallint
Uinteger = ducktype.Uinteger
Float = ducktype.Float
Double = ducktype.Double
Date = ducktype.Date
Datetime = ducktype.Datetime
Timestamp = ducktype.Timestamp


def select() -> SelectStatementBuilder:
    """Return a new ``SelectStatementBuilder`` via the shared ``ducktype`` namespace."""

    return ducktype.select()


__all__ = [
    "ducktype",
    "Numeric",
    "Varchar",
    "Boolean",
    "Blob",
    "Generic",
    "Tinyint",
    "Smallint",
    "Integer",
    "Utinyint",
    "Usmallint",
    "Uinteger",
    "Float",
    "Double",
    "Date",
    "Datetime",
    "Timestamp",
    "select",
]
