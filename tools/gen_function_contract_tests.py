#!/usr/bin/env python3
"""Generate contract tests verifying DuckDB function coverage."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
import sys
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.gen_duck_functions import (
    DuckDBFunctionRecord,
    family_for_first_param,
    get_functions,
)


def _parse_letters(argument: str) -> list[str]:
    letters = []
    for chunk in argument.split(","):
        text = chunk.strip().lower()
        if not text:
            continue
        if not text.isalpha() or len(text) != 1:
            msg = f"bucket letters must be single alphabetic characters: {chunk!r}"
            raise argparse.ArgumentTypeError(msg)
        letters.append(text)
    if not letters:
        msg = "at least one bucket letter must be provided"
        raise argparse.ArgumentTypeError(msg)
    return letters


_NAMESPACE_BY_FAMILY = {
    "numeric": "Numeric",
    "boolean": "Boolean",
    "varchar": "Varchar",
    "blob": "Blob",
    "temporal": "Numeric",
    "generic": "Generic",
}

_NAMESPACE_PRIORITY = {
    "Generic": 99,
    "Numeric": 10,
    "Boolean": 10,
    "Varchar": 10,
    "Blob": 10,
}

_MANUAL_NAMESPACE_OVERRIDES = {
    "array_to_string": "Varchar",
    "array_to_string_comma_default": "Varchar",
}


def _family_for_return(record: DuckDBFunctionRecord) -> str:
    if record.return_type and record.return_type not in {"ANY", "UNKNOWN"}:
        return family_for_first_param((record.return_type,))
    return record.family


def _group_by_letter(
    records: Iterable[DuckDBFunctionRecord],
    *,
    letters: set[str],
) -> dict[str, dict[str, list[str]]]:
    assignments: dict[str, dict[str, str]] = defaultdict(dict)
    for record in records:
        function_name = record.function_name
        if not function_name:
            continue
        letter = function_name[0].lower()
        if letter not in letters:
            continue
        namespace = _MANUAL_NAMESPACE_OVERRIDES.get(function_name)
        if namespace is None:
            family = _family_for_return(record)
            namespace = _NAMESPACE_BY_FAMILY.get(family, "Generic")
        letter_assignments = assignments[letter]
        existing = letter_assignments.get(function_name)
        if existing is not None:
            existing_priority = _NAMESPACE_PRIORITY.get(existing, 50)
            new_priority = _NAMESPACE_PRIORITY.get(namespace, 50)
            if existing_priority <= new_priority:
                continue
        letter_assignments[function_name] = namespace

    grouped: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for letter, function_map in assignments.items():
        for function_name, namespace in function_map.items():
            grouped[namespace][letter].append(function_name)

    return {
        namespace: {
            letter: sorted(functions)
            for letter, functions in sorted(letter_map.items())
        }
        for namespace, letter_map in sorted(grouped.items())
    }


def _render_namespace_case(
    *,
    kind: str,
    prefix: str,
    accessor: str,
    grouped: dict[str, dict[str, list[str]]],
) -> list[str]:
    lines: list[str] = []
    for namespace_name, buckets in sorted(grouped.items()):
        for letter, function_names in buckets.items():
            identifier = f"{prefix}_{namespace_name.lower()}_{letter}"
            lines.append(f"@pytest.mark.parametrize('function_name', {function_names!r})")
            lines.append(
                f"def test_{identifier}(function_name: str) -> None:")
            lines.append(
                f"    namespace = getattr({accessor}, '{namespace_name}')")
            lines.append(
                f"    _assert_contract('{kind}', '{namespace_name}', namespace, function_name)")
            lines.append("")
    return lines


def _render_module(
    *,
    letters: list[str],
    scalar: dict[str, dict[str, list[str]]],
    aggregate: dict[str, dict[str, list[str]]],
    window: dict[str, dict[str, list[str]]],
) -> str:
    title = ", ".join(letters)
    lines = [
        '"""Auto-generated DuckDB function contract tests.'
        f" Buckets: {title}." '"""',
        "",
        "from __future__ import annotations",
        "",
        "import pytest",
        "",
        "from duckplus.static_typed import (",
        "    AGGREGATE_FUNCTIONS,",
        "    SCALAR_FUNCTIONS,",
        "    WINDOW_FUNCTIONS,",
        ")",
        "",
        "from tests.test_duckdb_contracts import _load_todo_section",
        "",
        "_PARITY_LEDGER = _load_todo_section('DUCK_FUNCTION_PARITY')",
        "",
        "def _assert_contract(kind: str, expected_namespace: str, namespace: object, function_name: str) -> None:",
        "    if hasattr(namespace, function_name):",
        "        return",
        "    mismatches = _PARITY_LEDGER.get(kind, {}).get(expected_namespace, {})",
        "    for actual_namespace, functions in mismatches.items():",
        "        if function_name in functions:",
        "            return",
        "    for namespace_name, namespace_mismatches in _PARITY_LEDGER.get(kind, {}).items():",
        "        for actual_namespace, functions in namespace_mismatches.items():",
        "            if function_name in functions:",
        "                return",
        "    pytest.fail(",
        "        f\"Missing {function_name!r} on namespace {expected_namespace} without TODO parity entry\"",
        "    )",
        "",
    ]

    lines.extend(
        _render_namespace_case(
            kind="scalar",
            prefix="scalar",
            accessor="SCALAR_FUNCTIONS",
            grouped=scalar,
        )
    )

    lines.extend(
        _render_namespace_case(
            kind="aggregate",
            prefix="aggregate",
            accessor="AGGREGATE_FUNCTIONS",
            grouped=aggregate,
        )
    )

    lines.extend(
        _render_namespace_case(
            kind="window",
            prefix="window",
            accessor="WINDOW_FUNCTIONS",
            grouped=window,
        )
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "letters",
        type=_parse_letters,
        help="Comma separated list of first-letter buckets to generate",
    )
    parser.add_argument(
        "--output-dir",
        default=Path(__file__).resolve().parents[1] / "tests" / "generated",
        type=Path,
        help="Target directory for generated tests",
    )
    args = parser.parse_args()

    letters = set(args.letters)
    records = get_functions()
    scalar_records: list[DuckDBFunctionRecord] = []
    aggregate_records: list[DuckDBFunctionRecord] = []
    window_records: list[DuckDBFunctionRecord] = []

    for record in records:
        if record.function_type == "scalar":
            scalar_records.append(record)
        elif record.function_type == "aggregate":
            aggregate_records.append(record)
        elif record.function_type == "window":
            window_records.append(record)

    scalar_grouped = _group_by_letter(scalar_records, letters=letters)
    aggregate_grouped = _group_by_letter(aggregate_records, letters=letters)
    window_grouped = _group_by_letter(window_records, letters=letters)

    module_text = _render_module(
        letters=args.letters,
        scalar=scalar_grouped,
        aggregate=aggregate_grouped,
        window=window_grouped,
    )

    letters_token = "".join(args.letters)
    output_path = args.output_dir / f"test_duck_functions_{letters_token}.py"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(module_text, encoding="utf8")


if __name__ == "__main__":
    main()
