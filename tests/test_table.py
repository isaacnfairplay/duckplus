from __future__ import annotations

import pytest

from duckplus import DuckConnection, DuckRel, DuckTable, connect


@pytest.fixture()
def connection() -> DuckConnection:
    with connect() as conn:
        yield conn


def materialized_rows(conn: DuckConnection, table: str) -> list[tuple[object, ...]]:
    rows = conn.raw.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    return [tuple(row) for row in rows]


def test_append_aligns_columns_by_name(connection: DuckConnection) -> None:
    connection.raw.execute("CREATE TABLE target(id INTEGER, value VARCHAR)")
    rel = DuckRel(
        connection.raw.sql(
            "SELECT value, id FROM (VALUES ('a', 1), ('b', 2)) AS t(value, id)"
        )
    )
    table = DuckTable(connection, "target")
    table.append(rel)

    assert materialized_rows(connection, "target") == [(1, "a"), (2, "b")]


def test_append_by_position_requires_matching_counts(connection: DuckConnection) -> None:
    connection.raw.execute("CREATE TABLE numbers(id INTEGER)")
    rel = DuckRel(connection.raw.sql("SELECT 1 AS value, 2 AS extra"))
    table = DuckTable(connection, "numbers")
    with pytest.raises(ValueError):
        table.append(rel, by_name=False)


def test_insert_antijoin_inserts_missing_rows(connection: DuckConnection) -> None:
    connection.raw.execute("CREATE TABLE items(id INTEGER, label VARCHAR)")
    connection.raw.execute("INSERT INTO items VALUES (1, 'one'), (2, 'two')")

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES (2, 'two'), (3, 'three'), (4, 'four')
            ) AS t(id, label)
            """
        )
    )
    table = DuckTable(connection, "items")
    inserted = table.insert_antijoin(rel, keys=["id"])

    assert inserted == 2
    assert materialized_rows(connection, "items") == [
        (1, "one"),
        (2, "two"),
        (3, "three"),
        (4, "four"),
    ]


def test_insert_by_continuous_id_respects_threshold(connection: DuckConnection) -> None:
    connection.raw.execute("CREATE TABLE events(id INTEGER, payload VARCHAR)")
    connection.raw.execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')
            ) AS t(id, payload)
            """
        )
    )
    table = DuckTable(connection, "events")
    inserted = table.insert_by_continuous_id(rel, id_column="id")
    assert inserted == 2

    rel2 = DuckRel(
        connection.raw.sql(
            "SELECT * FROM (VALUES (5, 'e'), (6, 'f')) AS t(id, payload)"
        )
    )
    inserted_again = table.insert_by_continuous_id(rel2, id_column="id", inclusive=True)
    assert inserted_again == 1

    assert materialized_rows(connection, "events") == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
        (6, "f"),
    ]
