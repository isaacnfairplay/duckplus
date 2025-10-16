"""Shared helpers for inserting relation data into tables."""

from __future__ import annotations

# pylint: disable=import-error

from collections.abc import Sequence
from uuid import uuid4

import duckdb  # type: ignore[import-not-found]

from .duckcon import DuckCon


def require_connection(duckcon: DuckCon, helper: str) -> duckdb.DuckDBPyConnection:
    """Return the active DuckDB connection for a helper."""

    if not duckcon.is_open:
        msg = (
            f"DuckCon connection must be open to call {helper}. "
            "Use DuckCon as a context manager."
        )
        raise RuntimeError(msg)
    return duckcon.connection


def quote_identifier(identifier: str) -> str:
    """Return a DuckDB-quoted identifier."""

    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def prepare_table_identifier(table: str, helper: str) -> str:
    """Validate and quote a potentially qualified table name."""

    if not isinstance(table, str):
        msg = f"{helper} table name must be a string"
        raise TypeError(msg)
    stripped = table.strip()
    if not stripped:
        msg = f"{helper} table name cannot be empty"
        raise ValueError(msg)
    parts = stripped.split(".")
    if any(not part for part in parts):
        msg = f"{helper} table name '{table}' is not a valid qualified identifier"
        raise ValueError(msg)
    return ".".join(quote_identifier(part) for part in parts)


def normalise_target_columns(
    target_columns: Sequence[str] | None, helper: str
) -> tuple[str, ...] | None:
    """Validate and deduplicate optional target columns."""

    if target_columns is None:
        return None
    if isinstance(target_columns, (str, bytes)):
        msg = f"{helper} target_columns must be a sequence of column names"
        raise TypeError(msg)

    normalised: list[str] = []
    seen: set[str] = set()
    for column in target_columns:
        if not isinstance(column, str):
            msg = f"{helper} target_columns must only contain strings"
            raise TypeError(msg)
        trimmed = column.strip()
        if not trimmed:
            msg = f"{helper} target column names cannot be empty"
            raise ValueError(msg)
        key = trimmed.casefold()
        if key in seen:
            msg = f"{helper} target column '{column}' specified multiple times"
            raise ValueError(msg)
        seen.add(key)
        normalised.append(trimmed)

    if not normalised:
        msg = f"{helper} target_columns must contain at least one column"
        raise ValueError(msg)
    return tuple(normalised)


def append_relation_data(
    connection: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    table: str,
    helper: str,
    *,
    target_columns: tuple[str, ...] | None,
    create: bool,
    overwrite: bool,
) -> None:  # pylint: disable=too-many-arguments
    """Write data from a relation into a DuckDB table."""

    table_identifier = prepare_table_identifier(table, helper)
    view_name = f"duckplus_{helper}_{uuid4().hex}"
    relation.create_view(view_name, replace=True)
    quoted_view = quote_identifier(view_name)

    try:
        if create:
            if target_columns is not None:
                msg = f"{helper} does not support target_columns when create=True"
                raise ValueError(msg)
            if overwrite:
                connection.execute(f"DROP TABLE IF EXISTS {table_identifier}")
            connection.execute(
                f"CREATE TABLE {table_identifier} AS SELECT * FROM {quoted_view}"
            )
        else:
            transaction_started = False
            try:
                if overwrite:
                    connection.execute("BEGIN")
                    transaction_started = True
                    connection.execute(f"DELETE FROM {table_identifier}")
                if target_columns is None:
                    connection.execute(
                        f"INSERT INTO {table_identifier} SELECT * FROM {quoted_view}"
                    )
                else:
                    columns_sql = ", ".join(
                        quote_identifier(column) for column in target_columns
                    )
                    connection.execute(
                        f"INSERT INTO {table_identifier} ({columns_sql}) "
                        f"SELECT {columns_sql} FROM {quoted_view}"
                    )
                if transaction_started:
                    connection.execute("COMMIT")
            except Exception:  # pragma: no cover - transactional rollback
                if transaction_started:
                    connection.execute("ROLLBACK")
                raise
    finally:
        connection.execute(f"DROP VIEW IF EXISTS {quoted_view}")
