from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from duckplus import DuckCon, io as io_helpers


def _write_parquet(path: Path) -> None:
    connection = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        connection.execute(
            "COPY (SELECT 1 AS value, 'a' AS label UNION ALL SELECT 2, 'b') "
            f"TO '{escaped}' (FORMAT 'parquet')"
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


def test_read_csv_accepts_delim_alias(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("value;other\n1;foo\n2;bar\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(
            manager,
            csv_path,
            delim=";",
            quotechar="'",
            auto_detect=True,
        )

        assert relation.relation.fetchall() == [(1, "foo"), (2, "bar")]


def test_read_csv_rejects_conflicting_delimiters(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("value\n1\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        with pytest.raises(ValueError, match="Both 'delimiter' and alias 'delim'"):
            io_helpers.read_csv(
                manager,
                csv_path,
                delimiter=",",
                delim=";",
            )


def test_read_csv_filename_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("value\n1\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(manager, csv_path, filename=True)

        rows = relation.relation.fetchall()
        assert rows == [(1, str(csv_path))]


def test_read_csv_supports_dtype_alias(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("1\n2\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(
            manager,
            csv_path,
            header=False,
            names=["value"],
            dtype={"value": "INTEGER"},
        )

        assert relation.types == ("INTEGER",)
        assert relation.relation.fetchall() == [(1,), (2,)]


def test_read_csv_accepts_path_sequence(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text("1\n2\n", encoding="utf-8")
    second.write_text("3\n4\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(
            manager,
            [first, second],
            header=False,
            names=["value"],
        )

        assert relation.relation.fetchall() == [(1,), (2,), (3,), (4,)]


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


def test_read_parquet_supports_keyword_passthrough(tmp_path: Path) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_parquet(first)
    _write_parquet(second)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_parquet(
            manager,
            [first, second],
            binary_as_string=False,
            file_row_number=True,
            filename=True,
            hive_partitioning=False,
            union_by_name=True,
            compression="snappy",
        )

        assert relation.columns[-2:] == ("file_row_number", "filename")
        rows = relation.relation.fetchall()
        assert rows[:2] == [(1, "a", 0, str(first)), (2, "b", 1, str(first))]
        assert rows[2:] == [(1, "a", 0, str(second)), (2, "b", 1, str(second))]


def test_read_parquet_directory_adds_partition_column(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    _write_parquet(dataset / "0.parquet")
    _write_parquet(dataset / "prefix_1.parquet")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_parquet(
            manager,
            dataset,
            directory=True,
            partition_id_column="partition_id",
        )

        assert "partition_id" in relation.columns
        assert "filename" in relation.columns

        partition_index = relation.columns.index("partition_id")
        partitions = {row[partition_index] for row in relation.relation.fetchall()}
        assert partitions == {"0", "prefix_1"}


def test_read_parquet_directory_rejects_partition_collision(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    values = duckdb.sql(
        "SELECT 1 AS partition, 'a' AS label UNION ALL SELECT 2, 'b'"
    )
    values.write_parquet(str(dataset / "data.parquet"), overwrite=True)

    manager = DuckCon()
    with manager:
        with pytest.raises(ValueError, match="collides"):
            io_helpers.read_parquet(
                manager,
                dataset,
                directory=True,
                partition_id_column="partition",
            )


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


def test_read_json_accepts_path_sequence(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    _write_json(first)
    _write_json(second)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_json(manager, (first, second))

        rows = relation.relation.fetchall()
        assert len(rows) == 4
        assert rows.count((1, "alpha")) == 2
        assert rows.count((2, "beta")) == 2


def test_read_json_allows_explicit_columns(tmp_path: Path) -> None:
    json_path = tmp_path / "data.json"
    _write_json(json_path)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_json(
            manager,
            json_path,
            columns={"value": "INTEGER", "label": "VARCHAR"},
            maximum_object_size=1024,
            union_by_name=False,
        )

        assert relation.types == ("INTEGER", "VARCHAR")
        assert relation.relation.fetchall() == [
            (1, "alpha"),
            (2, "beta"),
        ]


def test_read_csv_allows_keyword_invocation(tmp_path: Path) -> None:
    csv_path = tmp_path / "keyword.csv"
    csv_path.write_text("value\n1\n", encoding="utf-8")

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_csv(
            duckcon=manager,
            source=csv_path,
            header=True,
        )

        assert relation.relation.fetchall() == [(1,)]


def test_read_parquet_allows_keyword_invocation(tmp_path: Path) -> None:
    parquet_path = tmp_path / "keyword.parquet"
    _write_parquet(parquet_path)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_parquet(
            duckcon=manager,
            source=parquet_path,
            file_row_number=True,
        )

        assert relation.relation.fetchall() == [(1, "a", 0), (2, "b", 1)]


def test_read_json_allows_keyword_invocation(tmp_path: Path) -> None:
    json_path = tmp_path / "keyword.json"
    _write_json(json_path)

    manager = DuckCon()
    with manager:
        relation = io_helpers.read_json(
            duckcon=manager,
            source=json_path,
            columns={"value": "INTEGER", "label": "VARCHAR"},
        )

        assert relation.relation.fetchall() == [
            (1, "alpha"),
            (2, "beta"),
        ]
