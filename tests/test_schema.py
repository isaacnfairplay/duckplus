from pathlib import Path

import pytest

from duckplus import DuckCon, Relation
from duckplus.schema import ColumnTypeDrift, diff_files, diff_relations


def _make_relation(manager: DuckCon, query: str) -> Relation:
    with manager as connection:
        relation = connection.sql(query)
        return Relation.from_relation(manager, relation)


def test_diff_relations_reports_missing_and_extra_columns() -> None:
    manager = DuckCon()

    baseline = _make_relation(
        manager,
        "SELECT 1::INTEGER AS id, 'alpha'::VARCHAR AS label",
    )
    candidate = _make_relation(
        manager,
        "SELECT 1::INTEGER AS id, 2::INTEGER AS amount",
    )

    diff = diff_relations(
        baseline,
        candidate,
        baseline_label="expected",
        candidate_label="actual",
        warn=False,
    )

    assert diff.missing_from_candidate == ("label",)
    assert diff.unexpected_in_candidate == ("amount",)
    assert diff.type_drift == ()
    assert not diff.is_match


def test_diff_relations_warn_on_type_drift() -> None:
    manager = DuckCon()

    baseline = _make_relation(
        manager,
        "SELECT 1::INTEGER AS value",
    )
    candidate = _make_relation(
        manager,
        "SELECT '1'::VARCHAR AS value",
    )

    with pytest.warns(UserWarning) as recorded:
        diff = diff_relations(
            baseline,
            candidate,
            baseline_label="baseline",
            candidate_label="candidate",
        )

    assert diff.type_drift == (
        ColumnTypeDrift(column="value", expected_type="INTEGER", observed_type="VARCHAR"),
    )
    assert diff.missing_from_candidate == ()
    assert diff.unexpected_in_candidate == ()
    assert not diff.is_match
    assert "value: INTEGER -> VARCHAR" in str(recorded[0].message)


def test_diff_files_compares_csv_sources(tmp_path: Path) -> None:
    manager = DuckCon()

    baseline_dir = tmp_path / "schema_baseline"
    baseline_dir.mkdir()
    baseline_path = baseline_dir / "baseline.csv"
    baseline_path.write_text("id,label\n1,alpha\n", encoding="utf-8")
    candidate_dir = tmp_path / "schema_candidate"
    candidate_dir.mkdir()
    candidate_path = candidate_dir / "candidate.csv"
    candidate_path.write_text("id,amount\n1,5\n", encoding="utf-8")

    with manager:
        diff = diff_files(
            manager,
            baseline_path,
            candidate_path,
            file_format="csv",
            warn=False,
        )

    assert diff.missing_from_candidate == ("label",)
    assert diff.unexpected_in_candidate == ("amount",)
    assert diff.type_drift == ()
