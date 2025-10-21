"""Utility helpers used by typed expression primitives."""

from __future__ import annotations

from decimal import Decimal


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def quote_qualified_identifier(column: str, *, table: str | None = None) -> str:
    if table is None:
        return quote_identifier(column)
    return f"{quote_identifier(table)}.{quote_identifier(column)}"


def quote_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def format_numeric(value: int | float | Decimal) -> str:
    if isinstance(value, bool):  # bool is subclass of int, exclude early
        raise TypeError("Boolean values are not valid numeric literals")
    if isinstance(value, Decimal):
        return format(value, "f")
    return repr(value)
