from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from duckplus import (
    DuckRel,
    DuckTable,
    append_csv,
    append_parquet,
    append_ndjson,
    connect,
    write_csv,
    write_parquet,
)


def _relation_rows(rel: DuckRel) -> list[tuple[object, ...]]:
    return [tuple(row) for row in rel.relation.fetchall()]


def test_read_parquet_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id, 'Alpha' AS label"))
        write_parquet(rel, path)
        loaded = conn.read_parquet(path)
        assert loaded.columns == ["id", "label"]
        assert _relation_rows(loaded) == [(1, "Alpha")]
        table = pq.read_table(path)
        assert table.num_rows == 1


def test_read_parquet_accepts_sequence(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 42 AS value"))
        write_parquet(rel, path)
        loaded = conn.read_parquet([path])
        assert _relation_rows(loaded) == [(42,)]


def test_read_parquet_validates_paths(tmp_path: Path) -> None:
    with connect() as conn:
        with pytest.raises(ValueError):
            conn.read_parquet([])  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            conn.read_parquet(123)  # type: ignore[arg-type]


def test_read_parquet_runtime_error_includes_details(tmp_path: Path) -> None:
    missing = tmp_path / "missing.parquet"
    with connect() as conn:
        with pytest.raises(RuntimeError) as excinfo:
            conn.read_parquet(missing)
        message = str(excinfo.value)
        assert "DuckDB failed to read Parquet data" in message
        assert "DuckDB error:" in message


def test_read_csv_with_options(tmp_path: Path) -> None:
    csv_path = tmp_path / "values.csv"
    csv_path.write_text("id|name\n1|Alice\n2|Bob\n", encoding="utf-8")
    with connect() as conn:
        rel = conn.read_csv(
            csv_path,
            delimiter="|",
            encoding="utf-8",
            header=True,
            auto_detect=False,
            columns={"id": "INTEGER", "name": "VARCHAR"},
        )
        assert rel.columns == ["id", "name"]
        assert _relation_rows(rel) == [(1, "Alice"), (2, "Bob")]


def test_read_csv_rejects_invalid_delimiter(tmp_path: Path) -> None:
    csv_path = tmp_path / "values.csv"
    csv_path.write_text("id,name\n1,A\n", encoding="utf-8")
    with connect() as conn:
        with pytest.raises(TypeError):
            conn.read_csv(csv_path, delimiter="")


def test_read_csv_rejects_invalid_header_type(tmp_path: Path) -> None:
    csv_path = tmp_path / "values.csv"
    csv_path.write_text("id,name\n1,A\n", encoding="utf-8")
    with connect() as conn:
        with pytest.raises(TypeError) as excinfo:
            conn.read_csv(csv_path, header="yes")  # type: ignore[arg-type]
        assert "header must be a boolean or integer value" in str(excinfo.value)


def test_read_csv_rejects_unknown_column_type(tmp_path: Path) -> None:
    csv_path = tmp_path / "values.csv"
    csv_path.write_text("id,name\n1,A\n", encoding="utf-8")
    with connect() as conn:
        with pytest.raises(ValueError) as excinfo:
            conn.read_csv(csv_path, columns={"id": "BAD"})  # type: ignore[arg-type]
        assert "Unsupported DuckDB type" in str(excinfo.value)


def test_read_csv_runtime_error_includes_details(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    with connect() as conn:
        with pytest.raises(RuntimeError) as excinfo:
            conn.read_csv(missing)
        message = str(excinfo.value)
        assert "DuckDB failed to read CSV data" in message
        assert "DuckDB error:" in message


def test_read_json_supports_newline_format(tmp_path: Path) -> None:
    json_path = tmp_path / "rows.json"
    json_path.write_text("\n".join([json.dumps({"id": 1}), json.dumps({"id": 2})]), encoding="utf-8")
    with connect() as conn:
        rel = conn.read_json(json_path, format="newline_delimited")
        assert rel.columns == ["id"]
        assert _relation_rows(rel) == [(1,), (2,)]


def test_write_csv_round_trip(tmp_path: Path) -> None:
    csv_path = tmp_path / "output.csv"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id, 'Alpha' AS label"))
        write_csv(rel, csv_path, delimiter=",")
        loaded = conn.read_csv(csv_path)
        assert _relation_rows(loaded) == [(1, "Alpha")]
        text = csv_path.read_text(encoding="utf-8")
        assert text.startswith("id,label\n")


def test_write_csv_rejects_empty_encoding(tmp_path: Path) -> None:
    csv_path = tmp_path / "output.csv"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id"))
        with pytest.raises(TypeError) as excinfo:
            write_csv(rel, csv_path, encoding="")
        assert "encoding must be provided as a non-empty string" in str(excinfo.value)


def test_write_parquet_rejects_invalid_compression(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id"))
        with pytest.raises(ValueError):
            write_parquet(rel, path, compression="invalid")  # type: ignore[arg-type]


def test_write_parquet_runtime_error_includes_details(tmp_path: Path) -> None:
    target_file = tmp_path / "out.parquet"
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id"))
        with pytest.raises(RuntimeError) as excinfo:
            write_parquet(rel, target_file, partition_by=["missing"])
        message = str(excinfo.value)
        assert "DuckDB failed to write Parquet data" in message
        assert "DuckDB error:" in message


def test_append_csv_appends_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,value\n1,Alice\n2,Bob\n", encoding="utf-8")
    with connect() as conn:
        conn.raw.execute("CREATE TABLE target(id INTEGER, value VARCHAR)")
        table = DuckTable(conn, "target")
        append_csv(table, csv_path)
        result = conn.raw.sql("SELECT * FROM target ORDER BY id").fetchall()
        assert result == [(1, "Alice"), (2, "Bob")]


def test_append_csv_forwards_options(tmp_path: Path) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id|value\n3|Carol\n", encoding="utf-8")
    with connect() as conn:
        conn.raw.execute("CREATE TABLE target(id INTEGER, value VARCHAR)")
        table = DuckTable(conn, "target")
        append_csv(
            table,
            csv_path,
            delimiter="|",
            auto_detect=False,
            columns={"id": "INTEGER", "value": "VARCHAR"},
        )
        result = conn.raw.sql("SELECT * FROM target ORDER BY id").fetchall()
        assert result == [(3, "Carol")]


def test_append_parquet_appends_rows(tmp_path: Path) -> None:
    parquet_path = tmp_path / "rows.parquet"
    with connect() as conn:
        seed = DuckRel(conn.raw.sql("SELECT 5 AS id, 'Delta' AS label"))
        write_parquet(seed, parquet_path)
        conn.raw.execute("CREATE TABLE target(id INTEGER, label VARCHAR)")
        table = DuckTable(conn, "target")
        append_parquet(table, parquet_path)
        result = conn.raw.sql("SELECT * FROM target").fetchall()
        assert result == [(5, "Delta")]


def test_append_ndjson_defaults_to_newline(tmp_path: Path) -> None:
    json_path = tmp_path / "rows.json"
    json_path.write_text("\n".join([json.dumps({"id": 1, "name": "Alice"}), json.dumps({"id": 2, "name": "Bob"})]), encoding="utf-8")
    with connect() as conn:
        conn.raw.execute("CREATE TABLE target(id INTEGER, name VARCHAR)")
        table = DuckTable(conn, "target")
        append_ndjson(table, json_path)
        result = conn.raw.sql("SELECT * FROM target ORDER BY id").fetchall()
        assert result == [(1, "Alice"), (2, "Bob")]


def test_append_ndjson_rejects_invalid_compression(tmp_path: Path) -> None:
    json_path = tmp_path / "rows.json"
    json_path.write_text("{}\n", encoding="utf-8")
    with connect() as conn:
        conn.raw.execute("CREATE TABLE target(id INTEGER)")
        table = DuckTable(conn, "target")
        with pytest.raises(ValueError):
            append_ndjson(table, json_path, compression="bad")  # type: ignore[arg-type]
