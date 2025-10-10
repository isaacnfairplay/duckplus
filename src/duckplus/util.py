"""Shared utility helpers for Duck+."""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from os import PathLike, fspath
from typing import Any, Literal, get_args

import re

try:  # pragma: no cover - optional dependency
    import numpy as _np  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - numpy isn't a project dependency
    _np = None


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUOTED_IDENTIFIER_RE = re.compile(r'^"(?:[^"]|"")*"$')


def ensure_identifier(name: str, *, allow_quoted: bool = False) -> str:
    """Validate that *name* is a valid DuckDB identifier.

    Parameters
    ----------
    name:
        Identifier to validate.
    allow_quoted:
        When ``True`` the identifier may be delimited by double quotes. Inner
        quotes must be doubled as per SQL rules.

    Returns
    -------
    str
        The original identifier when validation succeeds.

    Raises
    ------
    ValueError
        If the identifier is invalid.
    """

    if not isinstance(name, str):
        raise TypeError("Identifier must be a string")

    if _IDENTIFIER_RE.fullmatch(name):
        return name

    if allow_quoted and _QUOTED_IDENTIFIER_RE.fullmatch(name):
        # Ensure the identifier is properly quoted from both sides. The regular
        # expression already enforces doubling of any interior quotes.
        if name[0] == name[-1] == '"':
            return name

    raise ValueError(f"Invalid identifier: {name!r}")


def normalize_columns(columns: Sequence[str]) -> tuple[list[str], dict[str, int]]:
    """Return a stable column list along with a case-insensitive lookup map."""

    normalized = list(columns)
    lookup: dict[str, int] = {}

    for index, column in enumerate(normalized):
        if not isinstance(column, str):
            raise TypeError("Column names must be strings")

        key = column.casefold()
        if key in lookup:
            raise ValueError(f"Duplicate column name detected: {column!r}")
        lookup[key] = index

    return normalized, lookup


def resolve_columns(
    requested: Sequence[str],
    available: Sequence[str],
    *,
    missing_ok: bool = False,
) -> list[str]:
    """Resolve *requested* column names against *available* ones.

    Resolution is case-insensitive while preserving the stored column casing.
    When ``missing_ok`` is ``False`` (the default), missing columns raise a
    :class:`KeyError`.
    """

    normalized, lookup = normalize_columns(available)

    resolved: list[str] = []
    missing: list[str] = []

    for column in requested:
        if not isinstance(column, str):
            raise TypeError("Requested column names must be strings")

        key = column.casefold()
        index = lookup.get(key)
        if index is None:
            missing.append(column)
            if missing_ok:
                continue
        else:
            resolved.append(normalized[index])

    if missing and not missing_ok:
        missing_display = ", ".join(repr(name) for name in missing)
        raise KeyError(f"Columns not found: {missing_display}")

    return resolved


def coerce_scalar(value: Any) -> Any:
    """Coerce *value* into a DuckDB-friendly scalar."""

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, PathLike):
        return fspath(value)

    if _np is not None and isinstance(value, _np.generic):
        return value.item()

    return value
DuckDBType = Literal[
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
    "VARCHAR",
    "BLOB",
    "DATE",
    "TIME",
    "TIME_TZ",
    "TIMESTAMP",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
    "TIMESTAMP_TZ",
    "INTERVAL",
    "UUID",
    "JSON",
]
DUCKDB_TYPE_SET: frozenset[str] = frozenset(get_args(DuckDBType))

