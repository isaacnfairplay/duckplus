"""Tests for the DuckRel wrapper."""

import pytest

from duckplus import DuckConnection, DuckRel, connect


def _prepare_simple_table(conn: DuckConnection) -> None:
    conn.raw.execute("DROP TABLE IF EXISTS data")
    conn.raw.execute("CREATE TABLE data (id INTEGER, name TEXT)")
    conn.raw.execute("INSERT INTO data VALUES (1, 'alpha'), (2, 'beta')")


def test_from_table_preserves_columns() -> None:
    with connect() as conn:
        _prepare_simple_table(conn)
        rel = DuckRel.from_table(conn, "data")
        assert rel.columns == ("id", "name")
        assert rel.columns_lower == ("id", "name")
        assert rel.columns_lower_set == {"id", "name"}


def test_select_projects_without_mutating_source() -> None:
    with connect() as conn:
        _prepare_simple_table(conn)
        rel = DuckRel.from_table(conn, "data")
        projected = rel.select("id")

        assert rel.columns == ("id", "name")
        assert projected.columns == ("id",)


def test_select_raises_on_missing_column() -> None:
    with connect() as conn:
        _prepare_simple_table(conn)
        rel = DuckRel.from_table(conn, "data")

        with pytest.raises(KeyError, match="missing"):
            rel.select("missing")
