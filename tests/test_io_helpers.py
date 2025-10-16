from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from duckplus import DuckCon, io as io_helpers


def _write_parquet(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 1 AS value, 'a' AS label UNION ALL SELECT 2, 'b') "
            "TO ? (FORMAT 'parquet')",
            [str(path)],
        )
    finally:
        connection.close()


def _write_json(path: Path) -> None:
    rows = [
        {"value": 1, "label": "alpha"},
        {"value": 2, "label": "beta"},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")


def test_read_csv_returns_relation(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("value,other\n1,foo\n2,bar\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(manager, csv_path)

        assert relation.columns == ("value", "other")
        assert relation.relation.fetchall() == [(1, "foo"), (2, "bar")]


def test_read_csv_requires_open_connection(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("value\n1\n", encoding="utf-8")

    manager = DuckCon()

    with pytest.raises(RuntimeError, match="DuckCon connection must be open"):
        io_helpers.read_csv(manager, csv_path)


def test_read_csv_allows_explicit_schema(tmp_path: Path) -> None:
    csv_path = tmp_path / "schema.csv"
    csv_path.write_text("1,foo\n2,bar\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(
            manager,
            csv_path,
            header=False,
            columns={"value": "INTEGER", "label": "VARCHAR"},
        )

        assert relation.columns == ("value", "label")
        assert relation.types == ("INTEGER", "VARCHAR")
        assert relation.relation.fetchall() == [(1, "foo"), (2, "bar")]


def test_read_parquet_returns_relation(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    _write_parquet(parquet_path)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_parquet(manager, parquet_path, file_row_number=True)

        assert relation.columns[:2] == ("value", "label")
        assert relation.relation.fetchall() == [
            (1, "a", 0),
            (2, "b", 1),
        ]


def test_read_json_returns_relation(tmp_path: Path) -> None:
    json_path = tmp_path / "data.json"
    _write_json(json_path)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_json(manager, json_path)

        assert relation.columns == ("value", "label")
        assert relation.relation.fetchall() == [
            (1, "alpha"),
            (2, "beta"),
        ]
