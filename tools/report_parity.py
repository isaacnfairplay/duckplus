#!/usr/bin/env python3
"""Report DuckPlus parity coverage against DuckDB's public API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import sys

import duckdb

if str(_REPO_ROOT := Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.gen_duck_functions import get_functions, partition_functions

_TODO_PATH = _REPO_ROOT / "TODO.md"


def _load_todo_section(marker: str) -> dict[str, object]:
    start_token = f"<!-- {marker}:BEGIN -->"
    end_token = f"<!-- {marker}:END -->"
    text = _TODO_PATH.read_text(encoding="utf8")

    start = text.find(start_token)
    end = text.find(end_token)
    if start == -1 or end == -1:
        raise RuntimeError(f"TODO markers for {marker} not found")
    start += len(start_token)

    payload = text[start:end].strip()
    if not payload:
        return {}
    return json.loads(payload)


def _count_entries(mapping: dict[str, object]) -> int:
    total = 0
    for value in mapping.values():
        if isinstance(value, dict):
            total += _count_entries(value)
        elif isinstance(value, list):
            total += len(value)
        else:  # pragma: no cover - defensive against malformed ledgers
            raise TypeError(f"Unexpected ledger payload: {value!r}")
    return total


def _normalise_attributes(namespace: list[str]) -> list[str]:
    return [item for item in namespace if not item.startswith("_")]


@dataclass(frozen=True)
class ParityReport:
    category: str
    implemented: int
    total: int

    @property
    def missing(self) -> int:
        return self.total - self.implemented

    @property
    def coverage(self) -> float:
        if self.total == 0:
            return 100.0
        return (self.implemented / self.total) * 100.0

    def render(self) -> str:
        return (
            f"{self.category}: {self.implemented}/{self.total} "
            f"implemented ({self.coverage:.1f}% coverage)"
        )


def _collect_api_reports() -> list[ParityReport]:
    ledger = _load_todo_section("DUCK_API_ATTRIBUTE_GAPS")

    connection = duckdb.connect()
    relation = connection.sql("SELECT 1")

    connection_attributes = _normalise_attributes(dir(connection))
    relation_attributes = _normalise_attributes(dir(relation))

    connection_missing = len(ledger.get("connection", []))
    relation_missing = len(ledger.get("relation", []))

    connection_total = len(connection_attributes)
    relation_total = len(relation_attributes)

    return [
        ParityReport(
            category="Connection attributes",
            implemented=connection_total - connection_missing,
            total=connection_total,
        ),
        ParityReport(
            category="Relation attributes",
            implemented=relation_total - relation_missing,
            total=relation_total,
        ),
    ]


def _collect_function_reports() -> list[ParityReport]:
    ledger = _load_todo_section("DUCK_FUNCTION_PARITY")
    records = get_functions()
    scalar_map, aggregate_map, window_map = partition_functions(records)

    reports: list[ParityReport] = []
    for category, mapping in (
        ("Scalar functions", scalar_map),
        ("Aggregate functions", aggregate_map),
        ("Window functions", window_map),
    ):
        total = sum(len(family) for namespace in mapping.values() for family in namespace.values())
        missing = _count_entries(ledger.get(category.split()[0].lower(), {}))
        reports.append(
            ParityReport(category=category, implemented=total - missing, total=total)
        )

    overall_total = sum(report.total for report in reports)
    overall_missing = _count_entries(ledger)
    reports.append(
        ParityReport(
            category="Overall function coverage",
            implemented=overall_total - overall_missing,
            total=overall_total,
        )
    )
    return reports


def main() -> None:
    reports = _collect_api_reports() + _collect_function_reports()
    for report in reports:
        print(report.render())


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
