from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from datetime import date, datetime
from pathlib import Path

from duckplus.core import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    DuckRel,
    ExpressionPredicate,
    JoinProjection,
    JoinSpec,
)
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


def test_natural_inner_defaults_to_shared_keys(connection: duckdb.DuckDBPyConnection) -> None:
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

    joined = left.natural_inner(right)
    assert joined.columns == ["id", "left_val", "right_val"]
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1")]


def test_natural_left_with_missing_rows(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L1'), (2, 'L2')) AS t(id, left_val)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R1')) AS t(id, right_val)"))

    joined = left.natural_left(right)
    assert joined.column_types == ["INTEGER", "VARCHAR", "VARCHAR"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L1", "R1"), (2, "L2", None)]


def test_natural_join_with_alias(connection: duckdb.DuckDBPyConnection) -> None:
    orders = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 100),
                (2, 200)
            ) AS t(order_id, customer_ref)
            """
        )
    )
    customers = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (100, 'Alice'),
                (300, 'Charlie')
            ) AS t(id, name)
            """
        )
    )

    joined = orders.natural_inner(customers, customer_ref="id")
    assert joined.columns == ["order_id", "customer_ref", "name"]
    assert table_rows(joined.materialize().require_table()) == [(1, 100, "Alice")]


def test_join_missing_shared_keys_raises(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT 1 AS a"))
    right = DuckRel(connection.sql("SELECT 1 AS b"))
    with pytest.raises(ValueError):
        left.natural_inner(right)


def test_explicit_join_requires_matching_right_columns(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id_right, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    with pytest.raises(KeyError):
        left.inner_join(right, spec)


def test_join_projection_raises_on_collision_without_suffix(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    with pytest.raises(ValueError):
        left.inner_join(right, spec)


def test_join_projection_suffix_override(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'L')) AS t(id, value)"))
    right = DuckRel(connection.sql("SELECT * FROM (VALUES (1, 'R')) AS t(id, value)"))

    spec = JoinSpec(equal_keys=[("id", "id")])
    joined = left.inner_join(
        right, spec, project=JoinProjection(allow_collisions=True, suffixes=("_left", "_right"))
    )
    assert joined.columns == ["id", "value_left", "value_right"]
    assert table_rows(joined.materialize().require_table()) == [(1, "L", "R")]


def test_join_spec_rejects_invalid_key_sequence() -> None:
    with pytest.raises(TypeError):
        JoinSpec(equal_keys="id")  # type: ignore[arg-type]


def test_join_spec_rejects_non_predicate_entries() -> None:
    with pytest.raises(TypeError):
        JoinSpec(
            equal_keys=[("id", "id")],
            predicates=("unsupported",),  # type: ignore[arg-type]
        )


def test_explicit_join_with_predicate(connection: duckdb.DuckDBPyConnection) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, DATE '2024-01-01'),
                (2, DATE '2024-02-01')
            ) AS t(order_id, order_date)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, DATE '2023-12-15', 'A'),
                (1, DATE '2024-01-15', 'B')
            ) AS t(customer_id, customer_since, tier)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("order_id", "customer_id")],
        predicates=[ColumnPredicate("order_date", ">=", "customer_since")],
    )
    joined = left.left_outer(right, spec, project=JoinProjection(allow_collisions=True))
    assert joined.columns == ["order_id", "order_date", "customer_since", "tier"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, date(2024, 1, 1), date(2023, 12, 15), "A"),
        (2, date(2024, 2, 1), None, None),
    ]


def test_explicit_join_with_expression_predicate(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'keep'),
                (2, 'also_keep')
            ) AS t(id, label)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'primary', TRUE),
                (1, 'secondary', FALSE),
                (2, 'primary', TRUE)
            ) AS t(id, status, is_primary)
            """
        )
    )

    spec = JoinSpec(
        equal_keys=[("id", "id")],
        predicates=[ExpressionPredicate('r."is_primary"')],
    )
    joined = left.inner_join(
        right,
        spec,
        project=JoinProjection(allow_collisions=True, suffixes=("_left", "_right")),
    )

    assert joined.columns == ["id", "label", "status", "is_primary"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, "keep", "primary", True),
        (2, "also_keep", "primary", True),
    ]


def test_natural_asof_backward(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:03', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('B', TIMESTAMP '2024-01-01 00:00:04', 15)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:01', 9),
                ('A', TIMESTAMP '2024-01-01 00:00:05', 11),
                ('B', TIMESTAMP '2024-01-01 00:00:02', 14)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    joined = trades.natural_asof(quotes, order=AsofOrder(left="trade_ts", right="quote_ts"))
    assert joined.columns == ["symbol", "trade_ts", "price", "quote_ts", "quote_price"]
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 3), 10, datetime(2024, 1, 1, 0, 0, 1), 9),
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 5), 11),
        ("B", datetime(2024, 1, 1, 0, 0, 4), 15, datetime(2024, 1, 1, 0, 0, 2), 14),
    ]


def test_explicit_right_join_keeps_unmatched_rows(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    left = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'left')
            ) AS t(order_ref, left_val)
            """
        )
    )
    right = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'match'),
                (2, 'orphan')
            ) AS t(order_ref, right_val)
            """
        )
    )

    spec = JoinSpec(equal_keys=[("order_ref", "order_ref")])
    joined = left.left_right(right, spec).order_by(right_val="asc")

    assert joined.columns == ["order_ref", "left_val", "right_val"]
    assert table_rows(joined.materialize().require_table()) == [
        (1, "left", "match"),
        (None, None, "orphan"),
    ]


def test_natural_asof_forward(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:03', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('B', TIMESTAMP '2024-01-01 00:00:04', 15)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:04', 13),
                ('A', TIMESTAMP '2024-01-01 00:00:07', 15),
                ('B', TIMESTAMP '2024-01-01 00:00:06', 18)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    joined = trades.natural_asof(
        quotes,
        order=AsofOrder(left="trade_ts", right="quote_ts"),
        direction="forward",
    )
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 3), 10, datetime(2024, 1, 1, 0, 0, 4), 13),
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 7), 15),
        ("B", datetime(2024, 1, 1, 0, 0, 4), 15, datetime(2024, 1, 1, 0, 0, 6), 18),
    ]


def test_asof_join_nearest_with_tolerance(connection: duckdb.DuckDBPyConnection) -> None:
    trades = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:06', 12),
                ('A', TIMESTAMP '2024-01-01 00:00:09', 14)
            ) AS t(symbol, trade_ts, price)
            """
        )
    )
    quotes = DuckRel(
        connection.sql(
            """
            SELECT *
            FROM (VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:04', 10),
                ('A', TIMESTAMP '2024-01-01 00:00:08', 13),
                ('A', TIMESTAMP '2024-01-01 00:00:15', 16)
            ) AS t(symbol, quote_ts, quote_price)
            """
        )
    )

    spec = AsofSpec(
        equal_keys=[("symbol", "symbol")],
        order=AsofOrder(left="trade_ts", right="quote_ts"),
        direction="nearest",
        tolerance="3 seconds",
    )
    joined = trades.asof_join(quotes, spec)
    assert table_rows(joined.materialize().require_table()) == [
        ("A", datetime(2024, 1, 1, 0, 0, 6), 12, datetime(2024, 1, 1, 0, 0, 4), 10),
        ("A", datetime(2024, 1, 1, 0, 0, 9), 14, datetime(2024, 1, 1, 0, 0, 8), 13),
    ]


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
