"""Tests for relation materialization metrics."""

from __future__ import annotations

import json
from typing import Iterable, Mapping

from duckplus.core.relation import Relation


def _expected_size(columns: Iterable[str], rows: Iterable[Mapping[str, object]]) -> int:
    return sum(
        len(
            json.dumps({column: row.get(column) for column in columns}, ensure_ascii=False).encode(
                "utf-8"
            )
        )
        for row in rows
    )


def test_relation_metrics_cache() -> None:
    rows = (
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    )
    relation = Relation(("id", "name"), rows, materialized=False)
    expected_size = _expected_size(relation.columns, rows)
    assert relation.row_count == 2
    assert relation.estimated_size_bytes == expected_size

    materialized = relation.materialize()
    assert materialized is not relation
    assert materialized.row_count == relation.row_count
    assert materialized.estimated_size_bytes == relation.estimated_size_bytes


def test_materialize_returns_self_when_already_materialized() -> None:
    relation = Relation(("a",), ({"a": 1},))
    assert relation.materialize() is relation
