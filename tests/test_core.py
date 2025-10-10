from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from pathlib import Path

from duckplus.core import DuckRel
from duckplus.materialize import ParquetMaterializeStrategy


def table_rows(table: pa.Table) -> list[tuple[object, ...]]:
    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]
    if not columns:
        return [tuple() for _ in range(table.num_rows)]
    return list(zip(*columns, strict=True))


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def sample_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    base = connection.sql(
        """
        SELECT *
        FROM (VALUES
            (1, 'Alpha', 10),
            (2, 'Beta', 5),
            (3, 'Gamma', 8)
        ) AS t(id, Name, score)
        """
    )
    return DuckRel(base)


def test_columns_metadata_preserves_case(sample_rel: DuckRel) -> None:
    assert sample_rel.columns == ["id", "Name", "score"]
    assert sample_rel.columns_lower == ["id", "name", "score"]
    assert sample_rel.columns_lower_set == frozenset({"id", "name", "score"})


def test_column_types_metadata(sample_rel: DuckRel) -> None:
    assert sample_rel.column_types == ["INTEGER", "VARCHAR", "INTEGER"]


def test_project_columns_case_insensitive(sample_rel: DuckRel) -> None:
    selected = sample_rel.project_columns("name", "ID")
    assert selected.columns == ["Name", "id"]
    assert selected.column_types == ["VARCHAR", "INTEGER"]
    assert table_rows(selected.materialize().require_table()) == [
        ("Alpha", 1),
        ("Beta", 2),
        ("Gamma", 3),
    ]


def test_project_columns_missing_ok_returns_original(sample_rel: DuckRel) -> None:
    rel = sample_rel.project_columns("missing", missing_ok=True)
    assert rel is sample_rel


def test_project_allows_computed_columns(sample_rel: DuckRel) -> None:
    projected = sample_rel.project({"id": '"id"', "label": 'upper("Name")', "score": '"score"'})
    assert projected.columns == ["id", "label", "score"]
    assert projected.column_types == ["INTEGER", "VARCHAR", "INTEGER"]
    assert table_rows(projected.materialize().require_table())[0] == (1, "ALPHA", 10)


def test_filter_supports_parameters(sample_rel: DuckRel) -> None:
    filtered = sample_rel.filter('"id" = ? AND "Name" = ?', 2, "Beta")
    assert table_rows(filtered.materialize().require_table()) == [(2, "Beta", 5)]


@pytest.mark.parametrize(
    "values, error",
    [
        ((), ValueError),
        ((1,), ValueError),
    ],
)
def test_filter_parameter_validation(values: tuple[object, ...], error: type[Exception], sample_rel: DuckRel) -> None:
    with pytest.raises(error):
        sample_rel.filter('"id" = ? AND "Name" = ?', *values)


def test_inner_join_defaults_to_shared_keys(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'L1'),
                (2, 'L2')
            ) AS t(id, left_val)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'R1'),
                (3, 'R3')
            ) AS t(ID, right_val)
            """
        )
    )

    joined = left.inner_join(right)
    assert joined.columns == ["id", "left_val", "right_val"]
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1")]


def test_left_join_with_missing_rows(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L1'), (2, 'L2')) AS t(id, left_val)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R1')) AS t(id, right_val)"))

    joined = left.left_join(right)
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1"), (2, "L2", None)]


def test_join_raises_on_non_key_column_collision(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)")
    )
    right = DuckRel(
        connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id, value)")
    )

    with pytest.raises(ValueError):
        left.inner_join(right, on=["id"])


def test_explicit_join_requires_matching_right_columns(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id_right, value)"))

    with pytest.raises(KeyError):
        left.inner_join(right, on=["id"])


def test_join_missing_shared_keys_raises(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT 1 AS a"))
    right = DuckRel(connection.sql("SELECT 1 AS b"))
    with pytest.raises(ValueError):
        left.inner_join(right)


def test_semi_and_anti_join(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (2), (3)) AS t(id)"))

    semi = left.semi_join(right)
    anti = left.anti_join(right)

    assert table_rows(semi.materialize().require_table()) == [(2,), (3,)]
    assert table_rows(anti.materialize().require_table()) == [(1,)]


def test_cast_columns_updates_types(sample_rel: DuckRel) -> None:
    casted = sample_rel.cast_columns(id="UTINYINT")
    assert casted.column_types == ["UTINYINT", "VARCHAR", "INTEGER"]
    assert table_rows(casted.materialize().require_table())[0] == (1, "Alpha", 10)


def test_cast_columns_requires_targets(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.cast_columns()


def test_cast_columns_rejects_unknown_type(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.cast_columns({"id": "UNKNOWN"})  # type: ignore[arg-type]


def test_try_cast_columns_handles_invalid_values(sample_rel: DuckRel) -> None:
    casted = sample_rel.try_cast_columns({"Name": "UTINYINT"})
    assert casted.column_types == ["INTEGER", "UTINYINT", "INTEGER"]
    assert table_rows(casted.materialize().require_table())[0] == (1, None, 10)


def test_order_by_and_limit(sample_rel: DuckRel) -> None:
    ordered = sample_rel.order_by(score="desc").limit(2)
    assert table_rows(ordered.materialize().require_table()) == [(1, "Alpha", 10), (3, "Gamma", 8)]


def test_order_by_rejects_invalid_direction(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        sample_rel.order_by(score="up")


def test_materialize_returns_arrow_table(sample_rel: DuckRel) -> None:
    result = sample_rel.materialize()
    table = result.require_table()
    assert isinstance(table, pa.Table)
    assert result.relation is None
    assert result.path is None


def test_materialize_into_new_connection(sample_rel: DuckRel) -> None:
    other = duckdb.connect()
    try:
        result = sample_rel.materialize(into=other)
        relation = result.require_relation()
        rows = table_rows(relation.materialize().require_table())
        assert rows == table_rows(sample_rel.materialize().require_table())
    finally:
        other.close()


def test_materialize_parquet_strategy(sample_rel: DuckRel, tmp_path: Path) -> None:
    path = tmp_path / "dataset.parquet"
    strategy = ParquetMaterializeStrategy(path)
    result = sample_rel.materialize(strategy=strategy)
    table = result.require_table()
    assert result.path == path
    assert path.exists()
    other = duckdb.connect()
    try:
        moved = sample_rel.materialize(strategy=strategy, into=other)
        relation = moved.require_relation()
        assert table_rows(relation.materialize().require_table()) == table_rows(table)
    finally:
        other.close()
