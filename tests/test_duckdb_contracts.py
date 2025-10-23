from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import subprocess
import sys
from typing import Callable, Iterable

import duckdb
import pytest

from duckplus._duckdb_api_map import (
    CONNECTION_ATTRIBUTE_MAP,
    RELATION_ATTRIBUTE_MAP,
)
from duckplus.static_typed import expression as expression_module
from duckplus.static_typed._generated_function_namespaces import (
    AGGREGATE_FUNCTIONS,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
)
from tools.gen_duck_functions import get_functions, partition_functions

_REPO_ROOT = Path(__file__).resolve().parents[1]
_TODO_PATH = _REPO_ROOT / "TODO.md"


def _load_todo_section(marker: str) -> dict[str, object]:
    """Return the JSON payload stored under the given TODO ``marker``."""

    start_token = f"<!-- {marker}:BEGIN -->"
    end_token = f"<!-- {marker}:END -->"
    text = _TODO_PATH.read_text(encoding="utf8")

    start = text.find(start_token)
    end = text.find(end_token)
    if start == -1 or end == -1:
        pytest.skip(f"TODO markers for {marker} not found")
    start += len(start_token)

    payload = text[start:end].strip()
    if not payload:
        return {}

    return json.loads(payload)


def _collect_namespace_mismatches() -> dict[str, dict[str, dict[str, list[str]]]]:
    records = get_functions()
    scalar_map, aggregate_map, window_map = partition_functions(records)

    def locate(namespace_root: object, function_name: str) -> str | None:
        for namespace_name in dir(namespace_root):
            namespace = getattr(namespace_root, namespace_name)
            mapping = dict(type(namespace)._IDENTIFIER_FUNCTIONS)
            mapping.update(type(namespace)._SYMBOLIC_FUNCTIONS)
            if function_name in mapping:
                return namespace_name
        return None

    def build_mismatches(
        namespace_root: object,
        expected: dict[str, dict[str, list[object]]],
    ) -> dict[str, dict[str, list[str]]]:
        mismatches: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for expected_namespace, functions in expected.items():
            for function_name in functions:
                actual_namespace = locate(namespace_root, function_name)
                if actual_namespace is None:
                    mismatches[expected_namespace]["__missing__"].append(function_name)
                elif actual_namespace != expected_namespace:
                    mismatches[expected_namespace][actual_namespace].append(
                        function_name
                    )
        return {
            expected_namespace: {
                actual_namespace: sorted(functions)
                for actual_namespace, functions in sorted(actual_map.items())
            }
            for expected_namespace, actual_map in sorted(mismatches.items())
        }

    return {
        "scalar": build_mismatches(SCALAR_FUNCTIONS, scalar_map),
        "aggregate": build_mismatches(AGGREGATE_FUNCTIONS, aggregate_map),
        "window": build_mismatches(WINDOW_FUNCTIONS, window_map),
    }


def _normalise_attributes(namespace: Iterable[str]) -> list[str]:
    return [item for item in namespace if not item.startswith("_")]


def _collect_duckdb_attribute_gaps() -> dict[str, list[str]]:
    connection = duckdb.connect()
    relation = connection.sql("SELECT 1")

    relation_attributes = _normalise_attributes(dir(relation))
    connection_attributes = _normalise_attributes(dir(connection))

    missing_relation = sorted(
        attribute
        for attribute in relation_attributes
        if attribute not in RELATION_ATTRIBUTE_MAP
    )
    missing_connection = sorted(
        attribute
        for attribute in connection_attributes
        if attribute not in CONNECTION_ATTRIBUTE_MAP
    )

    return {
        "relation": missing_relation,
        "connection": missing_connection,
    }


@pytest.mark.parametrize(
    ("marker", "collector"),
    (
        ("DUCK_FUNCTION_PARITY", _collect_namespace_mismatches),
        ("DUCK_API_ATTRIBUTE_GAPS", _collect_duckdb_attribute_gaps),
    ),
)
def test_duck_catalog_contract(marker: str, collector: Callable[[], dict[str, object]]) -> None:
    """Ensure TODO ledger reflects the current DuckDB contract gaps."""

    expected = _load_todo_section(marker)
    observed = collector()

    assert observed == expected


def _gather_expression_attributes() -> dict[str, list[str]]:
    expression_types = {
        name: getattr(expression_module, name)
        for name in (
            "BlobExpression",
            "BooleanExpression",
            "GenericExpression",
            "NumericExpression",
            "TimestampExpression",
            "TimestampMillisecondsExpression",
            "TimestampMicrosecondsExpression",
            "TimestampNanosecondsExpression",
            "TimestampSecondsExpression",
            "TimestampWithTimezoneExpression",
            "VarcharExpression",
        )
    }

    attributes: dict[str, list[str]] = {}
    for name, expr_type in expression_types.items():
        members = [
            member
            for member in dir(expr_type)
            if not member.startswith("_")
            and member.isidentifier()
        ]
        attributes[name] = members
    return attributes


def _render_ty_contract_source(attributes: dict[str, list[str]]) -> str:
    lines = ["from duckplus import static_typed as st", ""]
    for name, members in sorted(attributes.items()):
        parameter = name[0].lower()
        lines.append(f"def ensure_{name.lower()}({parameter}: st.{name}) -> None:")
        if not members:
            lines.append("    pass")
            lines.append("")
            continue
        for member in members:
            lines.append(f"    _ = {parameter}.{member}")
        lines.append("")
    return "\n".join(lines)


def _run_ty_check(source: str) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "-m", "ty", "check"]
    return subprocess.run(
        command,
        input=source,
        text=True,
        capture_output=True,
        check=False,
    )


def _parse_ty_missing_attributes(output: str) -> list[str]:
    missing: list[str] = []
    for line in output.splitlines():
        if "has no attribute" in line:
            parts = line.split("has no attribute", maxsplit=1)
            if len(parts) != 2:
                continue
            attribute = parts[1].strip().strip("'\"").strip()
            missing.append(attribute)
    return sorted(set(missing))


def test_ty_dir_contract() -> None:
    pytest.importorskip("ty")
    attributes = _gather_expression_attributes()
    source = _render_ty_contract_source(attributes)
    result = _run_ty_check(source)
    missing = _parse_ty_missing_attributes(result.stderr + result.stdout)

    expected = _load_todo_section("TY_DIR_MISMATCHES")
    assert missing == expected.get("missing", [])

