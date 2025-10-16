from __future__ import annotations

import pytest

from duckplus import DuckCon, Relation


def test_table_insert_appends_rows() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER, value VARCHAR)")
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT * FROM (VALUES (1::INTEGER, 'a'::VARCHAR), (2::INTEGER, 'b'::VARCHAR))"
                " AS data(id, value)"
            ),
        )

        manager.table("data").insert(relation)
        rows = connection.sql("SELECT * FROM data ORDER BY id").fetchall()

    assert rows == [(1, "a"), (2, "b")]


def test_table_insert_overwrite_replaces_rows() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER, value VARCHAR)")
        connection.execute("INSERT INTO data VALUES (1, 'old')")
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT * FROM (VALUES (2::INTEGER, 'b'::VARCHAR)) AS data(id, value)"
            ),
        )

        manager.table("data").insert(relation, overwrite=True)
        rows = connection.sql("SELECT * FROM data ORDER BY id").fetchall()

    assert rows == [(2, "b")]


def test_table_insert_create_populates_new_table() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE SCHEMA table_api")
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT * FROM (VALUES (1::INTEGER, 'a'::VARCHAR)) AS data(id, value)"
            ),
        )

        manager.table("table_api.data").insert(relation, create=True, overwrite=True)
        rows = connection.sql("SELECT * FROM table_api.data").fetchall()

    assert rows == [(1, "a")]


def test_table_insert_requires_shared_duckcon() -> None:
    left = DuckCon()
    right = DuckCon()
    with left as left_connection:
        left_connection.execute("CREATE TABLE data(id INTEGER)")
        with right as right_connection:
            relation = Relation.from_relation(
                right,
                right_connection.sql("SELECT 1::INTEGER AS id"),
            )

            table = left.table("data")
            with pytest.raises(ValueError, match="different DuckCon"):
                table.insert(relation)


def test_table_insert_requires_open_connection() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER)")
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id"),
        )

    table = manager.table("data")
    with pytest.raises(RuntimeError, match="must be open"):
        table.insert(relation)


def test_table_insert_target_columns_respect_defaults() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute(
            "CREATE TABLE data(id INTEGER, value VARCHAR, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        relation = Relation.from_relation(
            manager,
            connection.sql(
                "SELECT * FROM (VALUES (1::INTEGER, 'a'::VARCHAR, CURRENT_TIMESTAMP))"
                " AS data(id, value, created)"
            ),
        )

        manager.table("data").insert(relation, target_columns=("id", "value"))
        rows = connection.sql(
            "SELECT id, value, created IS NOT NULL FROM data ORDER BY id"
        ).fetchall()

    assert rows == [(1, "a", True)]


def test_table_insert_relation_supports_raw_relations() -> None:
    manager = DuckCon()
    with manager as connection:
        connection.execute("CREATE TABLE data(id INTEGER)")
        relation = connection.sql("SELECT 1::INTEGER AS id")

        manager.table("data").insert_relation(relation)
        rows = connection.sql("SELECT * FROM data").fetchall()

    assert rows == [(1,)]


def test_table_insert_create_rejects_target_columns() -> None:
    manager = DuckCon()
    with manager as connection:
        relation = Relation.from_relation(
            manager,
            connection.sql("SELECT 1::INTEGER AS id"),
        )

        table = manager.table("data")
        with pytest.raises(ValueError, match="does not support target_columns"):
            table.insert(relation, create=True, target_columns=("id",))
