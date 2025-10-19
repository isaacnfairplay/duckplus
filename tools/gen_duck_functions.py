#!/usr/bin/env python3
"""CLI scaffold for generating DuckDB helper functions."""

from __future__ import annotations

import argparse
import sys
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

if TYPE_CHECKING:  # pragma: no cover - import used only for typing
    import duckdb


_SPACE_BEFORE_OPEN_PAREN_RE = re.compile(r"\s+\(")
_SPACE_AFTER_OPEN_PAREN_RE = re.compile(r"\(\s+")
_SPACE_BEFORE_CLOSE_PAREN_RE = re.compile(r"\s+\)")
_SPACE_BEFORE_OPEN_BRACKET_RE = re.compile(r"\s+\[")
_SPACE_AFTER_OPEN_BRACKET_RE = re.compile(r"\[\s+")
_SPACE_BEFORE_CLOSE_BRACKET_RE = re.compile(r"\s+\]")
_COMMA_WITH_OPTIONAL_SPACE_RE = re.compile(r"\s*,\s*")
_TYPE_FAMILY_BREAK_RE = re.compile(r"[\[(]")

_NUMERIC_BASE_RE = re.compile(
    r"^(?:UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|FLOAT|DOUBLE|REAL|DECIMAL|NUMERIC|BIT|BIGNUM)$"
)
_BLOB_BASES: frozenset[str] = frozenset({"BLOB", "BYTEA", "VARBINARY"})
_VARCHAR_BASES: frozenset[str] = frozenset({"VARCHAR", "STRING", "TEXT", "JSON", "UUID"})
_TEMPORAL_BASES: frozenset[str] = frozenset(
    {
        "DATE",
        "TIME",
        "TIME WITH TIME ZONE",
        "TIME_NS",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP_NS",
        "TIMESTAMP_MS",
        "TIMESTAMP_US",
        "TIMESTAMP_S",
        "TIMESTAMPTZ",
        "TIMETZ",
        "INTERVAL",
    }
)
_SEQUENCE_TYPES = (list, tuple)


@dataclass(frozen=True)
class DuckDBFunctionRecord:
    """Normalised representation of a DuckDB function definition."""

    schema_name: str
    function_name: str
    function_type: str
    return_type: str | None
    parameter_types: tuple[str | None, ...]
    parameters: tuple[str | None, ...]
    varargs: str | None
    description: str | None
    comment: str | None
    macro_definition: str | None
    family: str


def normalize_type(type_spec: str | None) -> str | None:
    """Normalise ``type_spec`` for consistent downstream processing."""

    if type_spec is None:
        return None

    text = type_spec.strip()
    if not text:
        return None

    in_single_quote = False
    in_double_quote = False
    pending_space = False
    components: list[str] = []

    for char in text:
        if char == "'" and not in_double_quote:
            if pending_space and components:
                components.append(" ")
            pending_space = False
            in_single_quote = not in_single_quote
            components.append(char)
            continue
        if char == '"' and not in_single_quote:
            if pending_space and components:
                components.append(" ")
            pending_space = False
            in_double_quote = not in_double_quote
            components.append(char)
            continue
        if in_single_quote or in_double_quote:
            if pending_space and components:
                components.append(" ")
            pending_space = False
            components.append(char)
            continue
        if char.isspace():
            pending_space = True
            continue
        if pending_space and components:
            components.append(" ")
        pending_space = False
        components.append(char.upper() if char.isalpha() else char)

    normalised = "".join(components).strip()
    if not normalised:
        return None

    normalised = _SPACE_BEFORE_OPEN_PAREN_RE.sub("(", normalised)
    normalised = _SPACE_AFTER_OPEN_PAREN_RE.sub("(", normalised)
    normalised = _SPACE_BEFORE_CLOSE_PAREN_RE.sub(")", normalised)
    normalised = _SPACE_BEFORE_OPEN_BRACKET_RE.sub("[", normalised)
    normalised = _SPACE_AFTER_OPEN_BRACKET_RE.sub("[", normalised)
    normalised = _SPACE_BEFORE_CLOSE_BRACKET_RE.sub("]", normalised)
    normalised = _COMMA_WITH_OPTIONAL_SPACE_RE.sub(", ", normalised)

    return normalised


def _root_type(type_spec: str) -> str:
    match = _TYPE_FAMILY_BREAK_RE.search(type_spec)
    if match:
        return type_spec[: match.start()].strip()
    return type_spec.strip()


def family_for_first_param(parameter_types: Sequence[str | None] | None) -> str:
    """Return a coarse family for the first parameter in ``parameter_types``."""

    if not parameter_types:
        return "generic"

    first = parameter_types[0]
    if first is None:
        return "generic"
    if isinstance(first, str):
        normalised_first = normalize_type(first)
    else:
        normalised_first = normalize_type(str(first))
    if not normalised_first:
        return "generic"

    base = _root_type(normalised_first)
    compact_base = base.replace(" ", "")

    if _NUMERIC_BASE_RE.match(compact_base):
        return "numeric"
    if base in _VARCHAR_BASES:
        return "varchar"
    if base in _BLOB_BASES:
        return "blob"
    if base in _TEMPORAL_BASES:
        return "temporal"
    if compact_base == "BOOLEAN":
        return "boolean"

    return "generic"


def get_functions(
    connection: duckdb.DuckDBPyConnection | None = None,
) -> list[DuckDBFunctionRecord]:
    """Load DuckDB function metadata and return normalised records."""

    import duckdb

    manage_connection = connection is None
    if connection is None:
        connection = duckdb.connect()

    query = """
        SELECT schema_name,
               function_name,
               function_type,
               return_type,
               parameters,
               parameter_types,
               varargs,
               description,
               comment,
               macro_definition
          FROM duckdb_functions()
         WHERE function_type IN ('scalar', 'aggregate', 'window')
    """

    try:
        result = connection.execute(query)
        records: Iterable[Mapping[str, object]]
        try:
            frame = result.fetch_df()
        except (ModuleNotFoundError, ImportError):  # pragma: no cover - pandas optional
            rows = result.fetchall()
            columns = (
                "schema_name",
                "function_name",
                "function_type",
                "return_type",
                "parameters",
                "parameter_types",
                "varargs",
                "description",
                "comment",
                "macro_definition",
            )
            records = (dict(zip(columns, row)) for row in rows)
        else:
            records = frame.to_dict(orient="records")
    finally:
        if manage_connection:
            connection.close()

    normalised_records: list[DuckDBFunctionRecord] = []
    for record in records:
        parameter_types_raw = record.get("parameter_types")
        parameters_raw = record.get("parameters")

        if isinstance(parameter_types_raw, _SEQUENCE_TYPES):
            normalised_parameter_types = tuple(
                normalize_type(value if isinstance(value, str) else str(value))
                if value is not None
                else None
                for value in parameter_types_raw
            )
        else:
            normalised_parameter_types = ()

        if isinstance(parameters_raw, _SEQUENCE_TYPES):
            normalised_parameters = tuple(
                value if isinstance(value, str) or value is None else str(value) for value in parameters_raw
            )
        else:
            normalised_parameters = ()

        normalised_records.append(
            DuckDBFunctionRecord(
                schema_name=str(record.get("schema_name")),
                function_name=str(record.get("function_name")),
                function_type=str(record.get("function_type")),
                return_type=normalize_type(record.get("return_type")),
                parameter_types=normalised_parameter_types,
                parameters=normalised_parameters,
                varargs=normalize_type(record.get("varargs")),
                description=(record.get("description") if isinstance(record.get("description"), str) else None),
                comment=(record.get("comment") if isinstance(record.get("comment"), str) else None),
                macro_definition=(
                    record.get("macro_definition") if isinstance(record.get("macro_definition"), str) else None
                ),
                family=family_for_first_param(normalised_parameter_types),
            )
        )

    return normalised_records


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the generator script."""
    parser = argparse.ArgumentParser(
        description="Generate helper functions backed by DuckDB."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate generated functions without writing changes.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entrypoint for the DuckDB function generation script."""
    args = parse_args(argv)

    try:
        import duckdb  # noqa: F401  # Import lazily to avoid unnecessary dependency load
    except Exception as exc:  # pragma: no cover - defensive against unexpected import errors
        print(f"Failed to import duckdb: {exc}", file=sys.stderr)
        return 1

    try:
        if args.check:
            # Placeholder for future validation logic.
            return 0

        # Placeholder for generation logic.
        return 0
    except Exception as exc:  # pragma: no cover - placeholder error handling
        print(f"gen_duck_functions failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
