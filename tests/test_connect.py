from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Sequence
import os
import subprocess
import sys
import textwrap

import duckplus
import duckplus.io  # noqa: F401  # ensure submodule is available for patching
import pytest

import duckdb

connect_mod = import_module("duckplus.connect")


def test_connection_read_parquet_delegates_to_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    with duckplus.connect() as conn:
        captured: dict[str, object] = {}

        class DummyReader:
            def __init__(self, relation: duckplus.DuckRel) -> None:
                self._relation = relation

            def run(self) -> duckplus.DuckRel:
                return self._relation

        def fake_for_connection(
            cls: type[object],
            connection: duckplus.DuckConnection,
            paths: object,
            **kwargs: object,
        ) -> DummyReader:
            captured["cls"] = cls
            captured["connection"] = connection
            captured["paths"] = paths
            captured["kwargs"] = kwargs
            relation = duckplus.DuckRel(connection.raw.sql("SELECT 1 AS marker"))
            captured["relation"] = relation
            return DummyReader(relation)

        monkeypatch.setattr(
            duckplus.io._ParquetReader,
            "from_connection",
            classmethod(fake_for_connection),
        )

        rel = conn.read_parquet("/tmp/input.parquet", union_by_name=True)

        assert rel is captured["relation"]
        assert captured["paths"] == "/tmp/input.parquet"
        assert set(captured["kwargs"]) == {
            "binary_as_string",
            "file_row_number",
            "filename",
            "hive_partitioning",
            "union_by_name",
            "can_have_nan",
            "compression",
            "parquet_version",
            "debug_use_openssl",
            "explicit_cardinality",
        }
        assert captured["kwargs"]["union_by_name"] is True
        assert captured["kwargs"]["binary_as_string"] is None


def test_connection_read_csv_delegates_to_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    with duckplus.connect() as conn:
        captured: dict[str, object] = {}

        class DummyReader:
            def __init__(self, relation: duckplus.DuckRel) -> None:
                self._relation = relation

            def run(self) -> duckplus.DuckRel:
                return self._relation

        def fake_for_connection(
            cls: type[object],
            connection: duckplus.DuckConnection,
            paths: object,
            **kwargs: object,
        ) -> DummyReader:
            captured["paths"] = paths
            captured["kwargs"] = kwargs
            relation = duckplus.DuckRel(connection.raw.sql("SELECT 2 AS marker"))
            captured["relation"] = relation
            return DummyReader(relation)

        monkeypatch.setattr(
            duckplus.io._CSVReader,
            "from_connection",
            classmethod(fake_for_connection),
        )

        rel = conn.read_csv(
            "/tmp/input.csv",
            encoding="latin-1",
            header=False,
            delimiter="|",
        )

        assert rel is captured["relation"]
        assert captured["paths"] == "/tmp/input.csv"
        expected_keys = {
            "encoding",
            "header",
            "delimiter",
            "quote",
            "escape",
            "nullstr",
            "sample_size",
            "auto_detect",
            "ignore_errors",
            "dateformat",
            "timestampformat",
            "decimal_separator",
            "columns",
            "all_varchar",
            "parallel",
            "allow_quoted_nulls",
            "null_padding",
            "normalize_names",
            "union_by_name",
            "filename",
            "hive_partitioning",
            "hive_types_autocast",
            "hive_types",
            "files_to_sniff",
            "compression",
            "thousands",
        }
        assert set(captured["kwargs"]) == expected_keys
        assert captured["kwargs"]["encoding"] == "latin-1"
        assert captured["kwargs"]["header"] is False
        assert captured["kwargs"]["delimiter"] == "|"
        assert captured["kwargs"]["compression"] is None


def test_connection_read_json_delegates_to_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    with duckplus.connect() as conn:
        captured: dict[str, object] = {}

        class DummyReader:
            def __init__(self, relation: duckplus.DuckRel) -> None:
                self._relation = relation

            def run(self) -> duckplus.DuckRel:
                return self._relation

        def fake_for_connection(
            cls: type[object],
            connection: duckplus.DuckConnection,
            paths: object,
            **kwargs: object,
        ) -> DummyReader:
            captured["paths"] = paths
            captured["kwargs"] = kwargs
            relation = duckplus.DuckRel(connection.raw.sql("SELECT 3 AS marker"))
            captured["relation"] = relation
            return DummyReader(relation)

        monkeypatch.setattr(
            duckplus.io._JSONReader,
            "from_connection",
            classmethod(fake_for_connection),
        )

        rel = conn.read_json(
            "/tmp/input.json",
            records="records",
            auto_detect=False,
        )

        assert rel is captured["relation"]
        assert captured["paths"] == "/tmp/input.json"
        expected_keys = {
            "columns",
            "sample_size",
            "maximum_depth",
            "records",
            "format",
            "dateformat",
            "timestampformat",
            "compression",
            "maximum_object_size",
            "ignore_errors",
            "convert_strings_to_integers",
            "field_appearance_threshold",
            "map_inference_threshold",
            "maximum_sample_files",
            "filename",
            "hive_partitioning",
            "union_by_name",
            "hive_types",
            "hive_types_autocast",
            "auto_detect",
        }
        assert set(captured["kwargs"]) == expected_keys
        assert captured["kwargs"]["records"] == "records"
        assert captured["kwargs"]["auto_detect"] is False
        assert captured["kwargs"]["compression"] is None


def test_connection_read_helpers_preserve_mypy_signatures(tmp_path: Path) -> None:
    snippet = textwrap.dedent(
        """
        from duckplus import DuckConnection, connect


        def check(conn: DuckConnection) -> None:
            conn.read_parquet("data.parquet", union_by_name=True)
            conn.read_csv("data.csv", encoding="utf-8", header=True, delimiter=",")
            conn.read_json("data.json", records="records")


        def misuse(conn: DuckConnection) -> None:
            conn.read_parquet("data.parquet", binary_as_string="oops")


        def main() -> None:
            with connect() as conn:
                check(conn)
                misuse(conn)
        """
    )

    script = tmp_path / "signature_check.py"
    script.write_text(snippet)

    project_root = Path(__file__).resolve().parent.parent
    config = project_root / "pyproject.toml"
    src_dir = project_root / "src"
    env = os.environ.copy()
    existing_path = env.get("PYTHONPATH", "")
    new_path = str(src_dir)
    if existing_path:
        new_path = f"{new_path}{os.pathsep}{existing_path}"
    env["PYTHONPATH"] = new_path

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file",
            str(config),
            str(script),
        ],
        capture_output=True,
        text=True,
        env=env,
    )

    assert proc.returncode == 1
    assert "error: Argument \"binary_as_string\"" in proc.stdout


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
