from __future__ import annotations

from typing import Callable

import pytest

from duckplus.core.connection import Connection
from duckplus.core.relation import Relation
from duckplus.table import Table


def _relation_builder(conn: Connection, *, rows: list[dict[str, object]]) -> Callable[[], Relation]:
    def _build() -> Relation:
        return conn.from_rows(["id"], rows)

    return _build


def test_table_insert_create_and_overwrite() -> None:
    conn = Connection()
    relation = conn.from_rows(["id"], [{"id": 1}, {"id": 2}])
    table = Table("people")
    table.insert(relation, create=True)
    assert list(table) == list(relation)

    new_relation = conn.from_rows(["id"], [{"id": 3}])
    table.insert(new_relation, overwrite=True)
    assert list(table) == list(new_relation)


def test_table_insert_requires_schema() -> None:
    conn = Connection()
    relation = conn.from_rows(["id"], [{"id": 1}])
    table = Table("people")
    with pytest.raises(ValueError):
        table.insert(relation)

    table.insert(relation, create=True)
    mismatch = conn.from_rows(["id", "name"], [{"id": 1, "name": "Ada"}])
    with pytest.raises(ValueError):
        table.insert(mismatch)


def test_table_insert_accepts_callable_for_create() -> None:
    conn = Connection()
    builder = _relation_builder(conn, rows=[{"id": 1}])
    table = Table("people")
    table.insert(builder, create=True)
    assert list(table) == [{"id": 1}]


def test_table_insert_accepts_callable_for_overwrite() -> None:
    conn = Connection()
    initial = conn.from_rows(["id"], [{"id": 1}])
    table = Table("people")
    table.insert(initial, create=True)

    builder = _relation_builder(conn, rows=[{"id": 2}, {"id": 3}])
    table.insert(builder, overwrite=True)
    assert list(table) == [{"id": 2}, {"id": 3}]


def test_table_insert_callable_requires_relation() -> None:
    conn = Connection()
    table = Table("people")

    def wrong_builder() -> object:
        return {"id": 1}

    with pytest.raises(TypeError):
        table.insert(wrong_builder, create=True)

