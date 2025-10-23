from __future__ import annotations

from pathlib import Path

import pytest

from duckplus.core.connection import Connection
from duckplus.core.exceptions import AppendError
from duckplus.core.relation import Relation
from duckplus.io import AppendDedupe, Partition, Rollover


def _make_relation(rows: list[dict[str, object]]) -> Relation:
    conn = Connection()
    return conn.from_rows(["id", "name"], rows)


def test_csv_append_dedupe(tmp_path: Path) -> None:
    base = tmp_path / "people.csv"
    initial = _make_relation([{ "id": 1, "name": "Ada" }])
    initial.write_csv(base)

    new_rows = _make_relation([{ "id": 1, "name": "Ada" }, { "id": 2, "name": "Bob" }])
    new_rows.append_csv(base, dedupe=AppendDedupe(mode="anti_join", keys=["id"]))

    content = base.read_text(encoding="utf-8").splitlines()
    assert content == ["id,name", "1,Ada", "2,Bob"]


def test_csv_append_header_guard(tmp_path: Path) -> None:
    base = tmp_path / "people.csv"
    relation = _make_relation([{ "id": 1, "name": "Ada" }])
    relation.write_csv(base, delimiter=",")
    with pytest.raises(AppendError):
        relation.append_csv(base, delimiter="\t")


def test_csv_append_respects_custom_delimiter(tmp_path: Path) -> None:
    base = tmp_path / "people.csv"
    initial = _make_relation([{ "id": 1, "name": "Ada" }])
    initial.write_csv(base, delimiter=";", quoting="\"")

    follow_up = _make_relation([{ "id": 2, "name": "Bea" }])
    follow_up.append_csv(base, delimiter=";", quoting="\"")

    content = base.read_text(encoding="utf-8").splitlines()
    assert content == ["id;name", "1;Ada", "2;Bea"]


def test_csv_partition_and_rollover(tmp_path: Path) -> None:
    base = tmp_path / "out"
    rows = _make_relation(
        [
            {"id": 1, "name": "Ada"},
            {"id": 2, "name": "Bob"},
        ]
    )
    written = rows.append_csv(
        base,
        partition=Partition(scheme="by_column", column="id"),
        rollover=Rollover(max_rows=1),
    )
    assert len(written) == 2
    for path in written:
        assert path.parent.parent == base
        assert path.exists()


def test_parquet_directory_append(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    rows = _make_relation([{"id": 1, "name": "Ada"}])
    target_dir = tmp_path / "warehouse"
    written = rows.append_parquet(target_dir)
    assert written
    for file_path in written:
        assert file_path.parent == target_dir
        assert file_path.suffix == ".parquet"


def test_parquet_single_file_force(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    rows = _make_relation([{"id": 1, "name": "Ada"}])
    file_path = tmp_path / "single.parquet"
    with pytest.raises(AppendError):
        rows.append_parquet(file_path)
    with pytest.warns(UserWarning):
        rows.append_parquet(file_path, force_single_file=True)
    assert file_path.exists()

