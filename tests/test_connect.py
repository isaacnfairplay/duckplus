from __future__ import annotations

from importlib import import_module
from typing import Any, Sequence

import duckplus
import duckplus.io  # noqa: F401  # ensure submodule is available for patching
import pytest

import duckdb

connect_mod = import_module("duckplus.connect")


def test_connection_read_parquet_filters_none(monkeypatch: pytest.MonkeyPatch) -> None:
    with duckplus.connect() as conn:
        captured: dict[str, object] = {}

        def fake_read_parquet(connection: duckplus.DuckConnection, paths: object, **kwargs: object) -> duckplus.DuckRel:
            captured["paths"] = paths
            captured["kwargs"] = kwargs
            return duckplus.DuckRel(conn.raw.sql("SELECT 1 AS marker"))

        monkeypatch.setattr(duckplus.io, "read_parquet", fake_read_parquet)

        conn.read_parquet("/tmp/input.parquet", union_by_name=True)

        assert captured["paths"] == "/tmp/input.parquet"
        assert captured["kwargs"] == {"union_by_name": True}


def test_connect_executes_simple_query() -> None:
    with duckplus.connect() as conn:
        result = conn.raw.execute("SELECT 42").fetchone()

    assert result == (42,)


def test_connection_from_pandas_roundtrip() -> None:
    pd = pytest.importorskip("pandas")

    with duckplus.connect() as conn:
        frame = pd.DataFrame({"id": [1, 2], "name": ["alpha", "beta"]})
        rel = conn.from_pandas(frame)

        assert rel.columns == ["id", "name"]
        assert rel.column_types == ["BIGINT", "VARCHAR"]
        assert rel.materialize().require_table().to_pylist() == [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]


def test_connection_from_polars_roundtrip() -> None:
    pl = pytest.importorskip("polars")

    with duckplus.connect() as conn:
        frame = pl.DataFrame({"value": [10, 20]})
        rel = conn.from_polars(frame)

        assert rel.columns == ["value"]
        assert rel.column_types == ["BIGINT"]
        assert rel.materialize().require_table().to_pylist() == [
            {"value": 10},
            {"value": 20},
        ]


def test_connection_table_helper_returns_ducktable() -> None:
    with duckplus.connect() as conn:
        conn.raw.execute("CREATE TABLE target(id INTEGER)")
        table = conn.table("target")

        assert table.name == "target"

        table.append(duckplus.DuckRel(conn.raw.sql("SELECT 1 AS id")))
        rows = conn.raw.execute("SELECT * FROM target").fetchall()

    assert rows == [(1,)]


def test_connect_applies_configuration(monkeypatch) -> None:
    captured_config: dict[str, object] = {}

    real_connect = connect_mod.duckdb.connect

    def capture_connect(*args: Any, **kwargs: Any):
        captured_config.update(kwargs)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(connect_mod.duckdb, "connect", capture_connect)

    with duckplus.connect(config={"Threads": 1}):
        pass

    assert captured_config["config"] == {"Threads": "1"}


def test_connect_module_exposes_odbc_strategies() -> None:
    assert connect_mod.MySQLStrategy is duckplus.MySQLStrategy
    assert connect_mod.PostgresStrategy is duckplus.PostgresStrategy


def test_load_extensions_validates_names() -> None:
    class StubConnection:
        def __init__(self) -> None:
            self.loaded: list[str] = []

        @property
        def raw(self) -> StubConnection:
            return self

        def load_extension(self, name: str) -> None:
            self.loaded.append(name)

    conn = StubConnection()

    connect_mod.load_extensions(conn, ["fts5"])

    assert conn.loaded == ["fts5"]

    with pytest.raises(ValueError):
        connect_mod.load_extensions(conn, ["invalid name"])  # spaces not allowed


class StubConnection:
    def __init__(self) -> None:
        self.loaded: list[str] = []
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self._duckdb_conn = duckdb.connect()

    @property
    def raw(self) -> StubConnection:
        return self

    def load_extension(self, name: str) -> None:
        self.loaded.append(name)

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None:
        self.statements.append((sql, tuple(() if params is None else tuple(params))))

    def sql(
        self,
        sql: str,
        parameters: Sequence[object] | None = None,
        *,
        params: Sequence[object] | None = None,
    ):
        if params is not None:
            bound = tuple(params)
        elif parameters is not None:
            bound = tuple(parameters)
        else:
            bound = tuple()
        self.queries.append((sql, bound))
        return self._duckdb_conn.sql("SELECT 1 AS sentinel")


def test_attach_nanodbc_loads_extension_and_attaches() -> None:
    conn = StubConnection()

    connect_mod.attach_nanodbc(
        conn,
        alias="remote",
        connection_string="DSN=example;UID=user;PWD=pass",
    )

    assert conn.loaded == ["nanodbc"]
    assert conn.statements == [
        (
            "ATTACH ? AS remote (TYPE ODBC, READ_ONLY)",
            ("DSN=example;UID=user;PWD=pass",),
        )
    ]


def test_attach_nanodbc_optional_write_access() -> None:
    conn = StubConnection()

    connect_mod.attach_nanodbc(
        conn,
        alias="rw_target",
        connection_string="Driver=SQLite;Database=:memory:",
        read_only=False,
        load_extension=False,
    )

    assert conn.loaded == []
    assert conn.statements == [
        (
            "ATTACH ? AS rw_target (TYPE ODBC, READ_ONLY=FALSE)",
            ("Driver=SQLite;Database=:memory:",),
        )
    ]


def test_attach_nanodbc_validates_inputs() -> None:
    conn = StubConnection()

    with pytest.raises(ValueError):
        connect_mod.attach_nanodbc(conn, alias="remote", connection_string="")

    with pytest.raises(ValueError):
        connect_mod.attach_nanodbc(conn, alias="remote schema", connection_string="DSN=x")

    with pytest.raises(TypeError):
        connect_mod.attach_nanodbc(conn, alias="remote", connection_string=123)  # type: ignore[arg-type]


def test_query_nanodbc_loads_extension_and_executes_query() -> None:
    conn = StubConnection()

    rel = connect_mod.query_nanodbc(
        conn,
        connection_string="DSN=warehouse",  # remote DSN
        query="SELECT * FROM remote_table WHERE flag = 1",
    )

    assert conn.loaded == ["nanodbc"]
    assert conn.queries == [
        (
            "SELECT * FROM odbc_query(?, ?)",
            ("DSN=warehouse", "SELECT * FROM remote_table WHERE flag = 1"),
        )
    ]
    assert isinstance(rel, duckplus.DuckRel)
    assert rel.columns == ["sentinel"]


def test_query_nanodbc_optional_extension_loading() -> None:
    conn = StubConnection()

    connect_mod.query_nanodbc(
        conn,
        connection_string="DSN=warehouse",
        query="SELECT 1",
        load_extension=False,
    )

    assert conn.loaded == []


def test_query_nanodbc_validates_inputs() -> None:
    conn = StubConnection()

    with pytest.raises(ValueError):
        connect_mod.query_nanodbc(
            conn,
            connection_string="DSN=warehouse",
            query=" ",
        )

    with pytest.raises(TypeError):
        connect_mod.query_nanodbc(
            conn,
            connection_string=object(),  # type: ignore[arg-type]
            query="SELECT 1",
        )
