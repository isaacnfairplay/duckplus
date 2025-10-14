"""Command line interface for Duck+."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Callable, Iterable, TextIO

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]

from .connect import DuckConnection, connect
from .core import DuckRel
from .relation.core import Relation
from .schema import AnyRow


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the Duck+ CLI."""

    parser = _build_parser()
    args = parser.parse_args(None if argv is None else list(argv))

    command: Callable[[argparse.Namespace, DuckConnection], int] | None = getattr(
        args, "handler", None
    )

    if command is None and not args.repl:
        parser.print_help()
        return 1

    read_only = bool(args.database)

    try:
        with connect(database=args.database, read_only=read_only) as conn:
            exit_code = 0
            if command is not None:
                exit_code = command(args, conn)
            if exit_code == 0 and args.repl:
                repl(conn)
            return exit_code
    except (duckdb.Error, OSError) as exc:  # pragma: no cover - defensive
        print(f"error: {exc}", file=sys.stderr)
        return 1


def repl(conn: DuckConnection) -> None:
    """Run a lightweight read-only REPL against *conn*."""

    stdin = sys.stdin
    stdout = sys.stdout
    stderr = sys.stderr

    while True:
        stdout.write("duckplus> ")
        stdout.flush()
        line = stdin.readline()
        if line == "":
            stdout.write("\n")
            break
        statement = _normalize_statement(line)
        if not statement:
            continue
        if statement in {".exit", ".quit", "\\q"}:
            break
        _run_statement(conn, statement, limit=20, stdout=stdout, stderr=stderr)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="duckplus", description="Duck+ CLI")
    parser.add_argument("--database", help="Optional DuckDB database path", default=None)
    parser.add_argument(
        "--repl",
        action="store_true",
        help="Start an interactive read-only REPL after executing the command.",
    )

    subparsers = parser.add_subparsers(dest="command")

    sql_parser = subparsers.add_parser("sql", help="Execute a read-only SQL query")
    sql_parser.add_argument("statement", help="SQL query to execute")
    sql_parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=20,
        help="Maximum number of rows to display (default: 20).",
    )
    sql_parser.set_defaults(handler=_handle_sql)

    schema_parser = subparsers.add_parser(
        "schema", help="Display column names and types for a query"
    )
    schema_parser.add_argument("statement", help="SQL query used to infer the schema")
    schema_parser.set_defaults(handler=_handle_schema)

    return parser


def _handle_sql(args: argparse.Namespace, conn: DuckConnection) -> int:
    statement = _normalize_statement(args.statement)
    if not statement:
        print("error: SQL statement cannot be empty", file=sys.stderr)
        return 1
    return _run_statement(
        conn,
        statement,
        limit=int(args.limit),
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def _handle_schema(args: argparse.Namespace, conn: DuckConnection) -> int:
    statement = _normalize_statement(args.statement)
    if not statement:
        print("error: SQL statement cannot be empty", file=sys.stderr)
        return 1
    try:
        relation: DuckRel[AnyRow] = Relation(conn.raw.sql(statement))
    except duckdb.Error as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema_table = pa.table(
        {"column": relation.columns, "type": relation.column_types}
    )
    output = _format_table(schema_table, ("column", "type"))
    sys.stdout.write(output)
    if not output.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _run_statement(
    conn: DuckConnection,
    statement: str,
    *,
    limit: int,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    try:
        relation: DuckRel[AnyRow] = Relation(conn.raw.sql(statement))
    except duckdb.Error as exc:
        stderr.write(f"error: {exc}\n")
        return 1

    try:
        preview, truncated = _preview_relation(relation, limit=limit)
    except duckdb.Error as exc:  # pragma: no cover - defensive
        stderr.write(f"error: {exc}\n")
        return 1

    output = _format_table(preview, relation.columns)
    stdout.write(output)
    if not output.endswith("\n"):
        stdout.write("\n")
    if truncated:
        stdout.write(f"... more rows available (showing first {limit})\n")
    return 0


def _preview_relation(rel: DuckRel[AnyRow], *, limit: int) -> tuple[pa.Table, bool]:
    if limit < 0:  # pragma: no cover - enforced by argparse
        raise ValueError("limit must be non-negative")
    preview_limit = 1 if limit == 0 else limit + 1
    limited = rel.limit(preview_limit)
    materialized = limited.materialize()
    table = materialized.require_table()
    truncated = table.num_rows > limit
    row_count = min(limit, table.num_rows)
    if limit == 0:
        row_count = 0
    return table.slice(0, row_count), truncated


def _format_table(table: pa.Table, columns: Iterable[str]) -> str:
    column_names = list(columns)
    if not column_names:
        return _format_empty_table(table)

    formatted_columns: list[list[str]] = []
    widths: list[int] = []
    for index, name in enumerate(column_names):
        column = table.column(index) if index < table.num_columns else pa.array([])
        values = [_format_value(value) for value in column.to_pylist()]
        width = max(len(name), *(len(value) for value in values)) if values else len(name)
        formatted_columns.append(values)
        widths.append(width)

    header = " | ".join(name.ljust(width) for name, width in zip(column_names, widths, strict=True))
    divider = "-+-".join("-" * width for width in widths)

    lines = [header, divider]

    for row_index in range(table.num_rows):
        line = " | ".join(
            formatted_columns[col_index][row_index].ljust(widths[col_index])
            for col_index in range(len(column_names))
        )
        lines.append(line)

    lines.append(_format_row_count(table.num_rows))
    return "\n".join(lines)


def _format_empty_table(table: pa.Table) -> str:
    lines = ["(no columns)"]
    lines.append(_format_row_count(table.num_rows))
    return "\n".join(lines)


def _format_row_count(count: int) -> str:
    suffix = "row" if count == 1 else "rows"
    return f"({count} {suffix})"


def _format_value(value: object) -> str:
    if value is None:
        return "NULL"
    return str(value)


def _normalize_statement(statement: str) -> str:
    normalized = statement.strip()
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    return normalized


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:  # pragma: no cover - argparse shows message
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("limit must be non-negative")
    return parsed


__all__ = ["main", "repl"]
