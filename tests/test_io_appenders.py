from __future__ import annotations

import json
from pathlib import Path

import pytest

from duckplus import DuckCon
from duckplus import io


def _write_file(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")


def test_append_csv_appends_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    _write_file(csv_path, "id,value\n1,a\n2,b\n")

    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER, value VARCHAR)")
        io.append_csv(manager, "data", csv_path)

        rows = connection.sql("SELECT * FROM data ORDER BY id").fetchall()

    assert rows == [(1, "a"), (2, "b")]


def test_append_csv_overwrite_replaces_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    _write_file(csv_path, "id,value\n2,b\n")

    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER, value VARCHAR)")
        connection.execute("INSERT INTO data VALUES (1, 'a')")

        io.append_csv(manager, "data", csv_path, overwrite=True)

        rows = connection.sql("SELECT * FROM data ORDER BY id").fetchall()

    assert rows == [(2, "b")]


def test_append_csv_target_columns_respects_defaults(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    _write_file(csv_path, "id,value\n1,a\n")

    manager = DuckCon()
    with manager as connection:
        connection.execute(
            "CREATE TABLE data("
            "id INTEGER,"
            " value VARCHAR,"
            " created TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")"
        )

        io.append_csv(manager, "data", csv_path, target_columns=("id", "value"))

        rows = connection.sql(
            "SELECT id, value, created IS NOT NULL FROM data ORDER BY id"
        ).fetchall()

    assert rows == [(1, "a", True)]


def test_append_csv_accepts_path_sequence(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    _write_file(first, "1,a\n")
    _write_file(second, "2,b\n")

    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER, value VARCHAR)")
        io.append_csv(
            manager,
            "data",
            (first, second),
            header=False,
            names=("id", "value"),
        )

        rows = connection.sql("SELECT * FROM data ORDER BY id").fetchall()

    assert rows == [(1, "a"), (2, "b")]


def test_append_ndjson_creates_table(tmp_path: Path) -> None:
    ndjson_path = tmp_path / "events.ndjson"
    with ndjson_path.open("w", encoding="utf-8") as handle:
        json.dump({"id": 1, "value": "a"}, handle)
        handle.write("\n")
        json.dump({"id": 2, "value": "b"}, handle)
        handle.write("\n")

    manager = DuckCon()
    with manager as connection:
        io.append_ndjson(manager, "events", ndjson_path, create=True, overwrite=True)

        rows = connection.sql("SELECT * FROM events ORDER BY id").fetchall()

    assert rows == [(1, "a"), (2, "b")]


def test_append_ndjson_accepts_path_sequence(tmp_path: Path) -> None:
    first = tmp_path / "first.ndjson"
    second = tmp_path / "second.ndjson"
    with first.open("w", encoding="utf-8") as handle:
        json.dump({"id": 1, "value": "a"}, handle)
        handle.write("\n")
    with second.open("w", encoding="utf-8") as handle:
        json.dump({"id": 2, "value": "b"}, handle)
        handle.write("\n")

    manager = DuckCon()
    with manager as connection:
        io.append_ndjson(
            manager,
            "events",
            [first, second],
            create=True,
            overwrite=True,
        )

        rows = connection.sql("SELECT * FROM events ORDER BY id").fetchall()

    assert rows == [(1, "a"), (2, "b")]


def test_append_helpers_require_open_connection(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    _write_file(csv_path, "id\n1\n")

    ndjson_path = tmp_path / "events.ndjson"
    with ndjson_path.open("w", encoding="utf-8") as handle:
        json.dump({"id": 1}, handle)
        handle.write("\n")

    manager = DuckCon()

    with pytest.raises(RuntimeError):
        io.append_csv(manager, "data", csv_path)

    with pytest.raises(RuntimeError):
        io.append_ndjson(manager, "events", ndjson_path)
