"""Ergonomic aliases for the :mod:`duckplus.static_typed.expression` namespace."""

# pylint: disable=invalid-name,wildcard-import,unused-wildcard-import

from __future__ import annotations

from .expression import DuckTypeNamespace, SelectStatementBuilder, ducktype as _ducktype
from .expressions.decimal import DECIMAL_FACTORY_NAMES as _DECIMAL_FACTORY_NAMES
from .expressions.decimal import *  # noqa: F401,F403 - re-export decimal factories

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
Timestamp = ducktype.Timestamp
Timestamp_s = ducktype.Timestamp_s
Timestamp_ms = ducktype.Timestamp_ms
Timestamp_us = ducktype.Timestamp_us
Timestamp_ns = ducktype.Timestamp_ns
Timestamp_tz = ducktype.Timestamp_tz


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
    "Timestamp",
    "Timestamp_s",
    "Timestamp_ms",
    "Timestamp_us",
    "Timestamp_ns",
    "Timestamp_tz",
    "select",
]

__all__.extend(_DECIMAL_FACTORY_NAMES)
