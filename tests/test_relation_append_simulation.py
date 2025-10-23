from __future__ import annotations

from pathlib import Path

from duckplus.core.connection import Connection
from duckplus.core.relation import Relation, _write_parquet_meta
from duckplus.io import AppendDedupe, Rollover


def _relation(rows: list[dict[str, object]]) -> Relation:
    conn = Connection()
    return conn.from_rows(["id", "name"], rows)


def test_simulate_append_csv_basic(tmp_path: Path) -> None:
    relation = _relation([
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Bob"},
    ])

    plan = relation.simulate_append_csv(tmp_path / "people.csv")

    assert plan.backend == "csv"
    assert plan.total_rows == 2
    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert action.path == tmp_path / "people.csv"
    assert action.rows_to_append == 2
    assert action.existing_rows == 0
    assert action.dedupe_applied is False
    assert action.rollover_applied is False
    assert action.will_create is True
    assert not action.path.exists()


def test_simulate_append_csv_rollover_and_dedupe(tmp_path: Path) -> None:
    base = tmp_path / "out"
    base.mkdir()
    target = base / "part.csv"
    target.write_text("id,name\n1,Ada\n", encoding="utf-8")

    relation = _relation([
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Bea"},
    ])

    plan = relation.simulate_append_csv(
        base,
        dedupe=AppendDedupe(mode="anti_join", keys=["id"]),
        rollover=Rollover(max_rows=1),
    )

    assert plan.backend == "csv"
    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert action.path.name == "part_1.csv"
    assert action.rollover_applied is True
    assert action.rows_to_append == 1
    assert action.dedupe_applied is True


def test_simulate_append_parquet_without_pyarrow(monkeypatch, tmp_path: Path) -> None:
    # Guard against accidental pyarrow imports.
    def _fail_import() -> None:
        raise AssertionError("pyarrow import should not be attempted during simulation")

    monkeypatch.setattr("duckplus.core.relation._import_pyarrow", _fail_import)

    base = tmp_path / "warehouse"
    base.mkdir()
    existing = base / "part.parquet"
    existing.touch()
    _write_parquet_meta(existing, 2)

    relation = _relation([{"id": 3, "name": "Cara"}])

    plan = relation.simulate_append_parquet(
        base,
        dedupe=AppendDedupe(mode="anti_join", keys=["id"]),
        rollover=Rollover(max_rows=2),
    )

    assert plan.backend == "parquet"
    assert plan.warnings == ()
    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert action.path.name == "part_1.parquet"
    assert action.rollover_applied is True
    assert action.rows_to_append == 1
    assert action.existing_rows == 0
    assert action.dedupe_applied is False
    assert "pyarrow" in plan.notes[0]


def test_simulate_parquet_force_single_file(tmp_path: Path) -> None:
    relation = _relation([{"id": 1, "name": "Ada"}])

    plan = relation.simulate_append_parquet(
        tmp_path / "single.parquet",
        force_single_file=True,
    )

    assert plan.warnings
    assert "Single-file Parquet append" in plan.warnings[0]
    assert plan.actions[0].path.name == "part.parquet"
