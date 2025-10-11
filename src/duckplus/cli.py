"""Command line interface helpers for Duck+."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import TextIO

import duckdb

from .connect import DuckConnection, connect
from .core import DuckRel
from . import util

_DEFAULT_LIMIT = 20


class CLIError(RuntimeError):
    """Raised when command execution fails."""


@dataclass(slots=True)
class SQLCommand:
    """Parsed configuration for the ``sql`` subcommand."""

    statement: str
    limit: int | None


@dataclass(slots=True)
class SchemaCommand:
    """Parsed configuration for the ``schema`` subcommand."""

    target: str


@dataclass(slots=True)
class CLIConfig:
    """Complete configuration for CLI execution."""

    database: Path | None
    read_write: bool
    display_limit: int
    repl: bool
    command: SQLCommand | SchemaCommand | None


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``duckplus`` CLI."""

    parser = _build_parser()
    namespace = parser.parse_args(argv)
    config = _convert_namespace(namespace)

    try:
        _validate_config(config)
        if config.command is None and not config.repl:
            parser.error("A subcommand is required unless --repl is specified.")
        if config.repl and config.command is not None:
            parser.error("--repl cannot be combined with other subcommands.")

        read_only = False if config.database is None else not config.read_write

        with connect(database=config.database, read_only=read_only) as conn:
            if config.repl:
                repl(conn, limit=config.display_limit)
                return 0
            assert config.command is not None
            if isinstance(config.command, SQLCommand):
                limit = (
                    config.command.limit
                    if config.command.limit is not None
                    else config.display_limit
                )
                return _run_sql(conn, config.command.statement, limit=limit)
            if isinstance(config.command, SchemaCommand):
                return _run_schema(conn, config.command.target)
            raise CLIError("Unsupported command configuration.")
    except CLIError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except duckdb.Error as exc:
        location = (
            "in-memory database"
            if config.database is None
            else f"database at {config.database}"
        )
        print(f"DuckDB failed to open {location}: {exc}", file=sys.stderr)
        return 1


def repl(
    conn: DuckConnection,
    *,
    limit: int = _DEFAULT_LIMIT,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> None:
    """Read-eval-print loop for ad-hoc read-only queries."""

    if limit < 0:
        raise CLIError("REPL limit must be non-negative.")

    input_stream = stdin if stdin is not None else sys.stdin
    output_stream = stdout if stdout is not None else sys.stdout

    while True:
        output_stream.write("duckplus> ")
        output_stream.flush()
        line = input_stream.readline()
        if line == "":
            output_stream.write("\n")
            break
        statement = line.strip()
        if not statement:
            continue
        lowered = statement.casefold()
        if lowered in {"exit", "quit", ".exit", "\\q"}:
            break
        compiled = statement.rstrip(";")
        if not compiled:
            continue
        try:
            relation = DuckRel(conn.raw.sql(compiled))
        except duckdb.Error as exc:  # pragma: no cover - defensive
            output_stream.write(f"error: {exc}\n")
            continue
        _write_relation(relation, limit=limit, stream=output_stream)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="duckplus")
    parser.add_argument(
        "--database",
        type=Path,
        help="Optional path to a DuckDB database file. Defaults to in-memory storage.",
    )
    parser.add_argument(
        "--read-write",
        action="store_true",
        help="Allow write access to the database. Defaults to read-only mode.",
    )
    parser.add_argument(
        "--display-limit",
        type=int,
        default=_DEFAULT_LIMIT,
        help="Maximum number of rows to display for command output.",
    )
    parser.add_argument(
        "--repl",
        action="store_true",
        help="Start an interactive read-eval-print loop.",
    )
    subparsers = parser.add_subparsers(dest="command")

    sql_parser = subparsers.add_parser(
        "sql",
        help="Execute a SQL statement and print the resulting relation.",
    )
    sql_parser.add_argument("statement", help="DuckDB SQL statement to execute.")
    sql_parser.add_argument(
        "--limit",
        dest="sql_limit",
        type=int,
        help="Override the display limit for this SQL execution.",
    )

    schema_parser = subparsers.add_parser(
        "schema",
        help="Describe columns and types for a table or view.",
    )
    schema_parser.add_argument(
        "target",
        help="Table or view name to describe. Supports schema-qualified identifiers.",
    )

    return parser


def _convert_namespace(namespace: Namespace) -> CLIConfig:
    command: SQLCommand | SchemaCommand | None
    match namespace.command:
        case "sql":
            command = SQLCommand(namespace.statement, namespace.sql_limit)
        case "schema":
            command = SchemaCommand(namespace.target)
        case _:
            command = None

    return CLIConfig(
        database=namespace.database,
        read_write=bool(namespace.read_write),
        display_limit=namespace.display_limit,
        repl=bool(namespace.repl),
        command=command,
    )


def _validate_config(config: CLIConfig) -> None:
    if config.display_limit < 0:
        raise CLIError("--display-limit must be non-negative.")


def _run_sql(conn: DuckConnection, statement: str, *, limit: int) -> int:
    if limit < 0:
        raise CLIError("SQL limit must be non-negative.")

    try:
        relation = DuckRel(conn.raw.sql(statement))
    except duckdb.Error as exc:
        raise CLIError(f"DuckDB failed to execute SQL: {exc}") from exc

    _write_relation(relation, limit=limit, stream=sys.stdout)
    return 0


def _run_schema(conn: DuckConnection, target: str) -> int:
    try:
        identifier = _sanitize_target(target)
    except ValueError as exc:
        raise CLIError(str(exc)) from exc
    try:
        result = conn.raw.execute(f"PRAGMA table_info('{identifier}')")
    except duckdb.Error as exc:  # pragma: no cover - defensive
        message = str(exc)
        if "does not exist" in message.casefold():
            raise CLIError(
                f"No schema information found for target {target!r}; ensure the relation exists."
            ) from exc
        raise CLIError(f"DuckDB failed to describe schema: {exc}") from exc

    rows = result.fetchall()
    if not rows:
        raise CLIError(
            f"No schema information found for target {target!r}; ensure the relation exists."
        )

    headers = ["column", "type", "notnull", "default", "primary_key"]
    values = [
        [
            str(row[1]),
            str(row[2]),
            "yes" if row[3] else "no",
            "" if row[4] is None else str(row[4]),
            "yes" if row[5] else "no",
        ]
        for row in rows
    ]
    _write_table(headers, values, stream=sys.stdout)
    return 0


def _sanitize_target(target: str) -> str:
    if not target:
        raise ValueError("Schema target must be provided as a non-empty identifier.")
    parts = target.split(".")
    normalized: list[str] = []
    for part in parts:
        try:
            normalized.append(util.ensure_identifier(part))
        except ValueError:
            quoted = util.ensure_identifier(part, allow_quoted=True)
            if not (quoted and quoted[0] == '"' and quoted[-1] == '"'):
                raise
            normalized.append(quoted[1:-1].replace("\"\"", "\""))
    return ".".join(normalized)


def _write_relation(rel: DuckRel, *, limit: int, stream: TextIO) -> None:
    fetch = limit + 1 if limit >= 0 else 0
    limited = rel.limit(fetch) if limit >= 0 else rel
    table = limited.materialize().require_table()
    row_dicts = table.to_pylist()

    truncated = limit >= 0 and len(row_dicts) > limit
    display_rows = row_dicts[:limit] if limit >= 0 else row_dicts
    headers = rel.columns
    rows = [
        [
            "NULL" if row[column] is None else str(row[column])
            for column in headers
        ]
        for row in display_rows
    ]
    _write_table(headers, rows, stream=stream)
    if truncated:
        stream.write(f"\n… additional rows not shown (limit {limit}).\n")


def _write_table(headers: Sequence[str], rows: Sequence[Sequence[str]], *, stream: TextIO) -> None:
    header_list = list(headers)
    widths = [len(column) for column in header_list]
    rendered_rows: list[list[str]] = []
    for row in rows:
        rendered = [str(cell) for cell in row]
        rendered_rows.append(rendered)
        for index, cell in enumerate(rendered):
            if index < len(widths):
                widths[index] = max(widths[index], len(cell))

    header_line = " | ".join(
        column.ljust(widths[index]) for index, column in enumerate(header_list)
    )
    separator = "-+-".join("-" * width for width in widths)
    stream.write(f"{header_line}\n{separator}\n")

    if rendered_rows:
        for rendered in rendered_rows:
            line = " | ".join(
                rendered[index].ljust(widths[index])
                for index in range(len(header_list))
            )
            stream.write(f"{line}\n")
    else:
        stream.write("(no rows)\n")


__all__ = ["main", "repl"]

