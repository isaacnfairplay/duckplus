from collections.abc import Iterable, Mapping, Sequence
import inspect
import pickle
from pathlib import Path
from typing import Any

import duckdb
import pytest

from duckplus import DuckCon, Relation, io as io_helpers
from duckplus.duckcon import ExtensionInfo


def test_duckcon_context_opens_and_closes_connection() -> None:
    manager = DuckCon()
    assert not manager.is_open

    with manager as connection:
        assert manager.is_open
        result = connection.execute("SELECT 1").fetchone()
        assert result == (1,)

    assert not manager.is_open
    with pytest.raises(RuntimeError):
        _ = manager.connection


def test_duckcon_helper_extension_point() -> None:
    manager = DuckCon()

    def echo_helper(conn: duckdb.DuckDBPyConnection, value: int) -> int:
        return conn.execute("SELECT ?", [value]).fetchone()[0]

    manager.register_helper("echo", echo_helper)

    assert DuckCon.echo is not None

    with manager:
        assert manager.apply_helper("echo", 42) == 42

    with pytest.raises(KeyError):
        manager.apply_helper("missing")

    manager.register_helper("echo", echo_helper, overwrite=True)
    with manager:
        assert manager.apply_helper("echo", 7) == 7


def test_duckcon_registers_default_io_helpers(tmp_path: Path) -> None:
    csv_path = tmp_path / "auto.csv"
    csv_path.write_text("value\n1\n2\n", encoding="utf-8")

    manager = DuckCon()

    assert DuckCon.read_csv is io_helpers.read_csv

    with manager:
        via_method = manager.read_csv(csv_path)
        via_registry = manager.apply_helper("read_csv", csv_path)
        rows = via_method.relation.fetchall()
        registry_rows = via_registry.relation.fetchall()

    assert rows == [(1,), (2,)]
    assert rows == registry_rows
    assert via_method.columns == via_registry.columns

    with pytest.raises(RuntimeError, match="connection must be open"):
        manager.read_csv(csv_path)


def test_duckcon_helpers_support_introspection() -> None:
    helper = DuckCon.__dict__["read_csv"]

    assert helper is io_helpers.read_csv
    assert helper.__module__ == "duckplus.io"
    assert inspect.getdoc(helper) and "Load a CSV file" in inspect.getdoc(helper)

    signature = inspect.signature(helper)
    assert list(signature.parameters)[:2] == ["duckcon", "source"]

    assert inspect.getdoc(DuckCon.read_csv) == inspect.getdoc(helper)
    assert pickle.loads(pickle.dumps(helper)) is helper
def test_duckcon_registers_read_odbc_and_excel_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = DuckCon()

    query_calls: list[tuple[str, str, tuple[Any, ...] | None]] = []
    table_calls: list[tuple[str, str]] = []
    excel_calls: list[tuple[str, str]] = []

    query_sentinel = object()
    table_sentinel = object()
    excel_sentinel = object()

    def fake_from_odbc_query(
        cls: type[Relation],
        duckcon: DuckCon,
        connection_string: str,
        query: str,
        *,
        parameters: Iterable[Any] | None = None,
    ) -> object:
        assert cls is Relation
        assert duckcon is manager
        query_calls.append(
            (
                connection_string,
                query,
                tuple(parameters) if parameters is not None else None,
            )
        )
        return query_sentinel

    def fake_from_odbc_table(
        cls: type[Relation],
        duckcon: DuckCon,
        connection_string: str,
        table: str,
    ) -> object:
        assert cls is Relation
        assert duckcon is manager
        table_calls.append((connection_string, table))
        return table_sentinel

    def fake_from_excel(
        cls: type[Relation],
        duckcon: DuckCon,
        source: str,
        *,
        sheet: str | int | None = None,
        header: bool | None = None,
        skip: int | None = None,
        skiprows: int | None = None,
        limit: int | None = None,
        names: Sequence[str] | None = None,
        dtype: Mapping[str, str] | Sequence[str] | None = None,
        all_varchar: bool | None = None,
    ) -> object:
        assert cls is Relation
        assert duckcon is manager
        excel_calls.append((source, str(sheet)))
        return excel_sentinel

    monkeypatch.setattr(
        Relation,
        "from_odbc_query",
        classmethod(fake_from_odbc_query),
    )
    monkeypatch.setattr(
        Relation,
        "from_odbc_table",
        classmethod(fake_from_odbc_table),
    )
    monkeypatch.setattr(
        Relation,
        "from_excel",
        classmethod(fake_from_excel),
    )

    with manager:
        assert (
            manager.read_odbc_query("conn", "SELECT 1", parameters=(1,))
            is query_sentinel
        )
        assert (
            manager.apply_helper("read_odbc_query", "conn", "SELECT 1", parameters=(1,))
            is query_sentinel
        )
        assert manager.read_odbc_table("conn", "schema.table") is table_sentinel
        assert (
            manager.apply_helper("read_odbc_table", "conn", "schema.table")
            is table_sentinel
        )
        assert manager.read_excel("workbook.xlsx", sheet="Sheet1") is excel_sentinel
        assert (
            manager.apply_helper("read_excel", "workbook.xlsx", sheet="Sheet1")
            is excel_sentinel
        )

    expected_query = ("conn", "SELECT 1", (1,))
    expected_table = ("conn", "schema.table")
    expected_excel = ("workbook.xlsx", "Sheet1")

    assert query_calls == [expected_query, expected_query]
    assert table_calls == [expected_table, expected_table]
    assert excel_calls == [expected_excel, expected_excel]


def test_extra_extensions_loads_on_enter(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def install_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        calls.append(("install", name))

    def load_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        calls.append(("load", name))

    monkeypatch.setattr(DuckCon, "_install_via_duckdb_extensions", lambda self, name: False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "install_extension", install_extension)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "load_extension", load_extension)

    manager = DuckCon(extra_extensions=("nanodbc",))

    with manager:
        pass

    assert ("install", "nano_odbc") in calls
    assert ("load", "nano_odbc") in calls


def test_extra_extension_failure_recommends_parameter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def load_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        raise duckdb.IOException("offline")

    monkeypatch.setattr(DuckCon, "_install_via_duckdb_extensions", lambda self, name: False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "load_extension", load_extension)

    manager = DuckCon(extra_extensions=("nanodbc",))

    with pytest.raises(RuntimeError, match="extra_extensions"):
        with manager:
            pass


def test_extra_extensions_loads_excel(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def install_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        calls.append(("install", name))

    def load_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        calls.append(("load", name))

    monkeypatch.setattr(DuckCon, "_install_via_duckdb_extensions", lambda self, name: False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "install_extension", install_extension)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "load_extension", load_extension)

    manager = DuckCon(extra_extensions=("excel",))

    with manager:
        pass

    assert ("install", "excel") in calls
    assert ("load", "excel") in calls


def test_extra_extensions_excel_failure_recommends_parameter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def load_extension(_connection: duckdb.DuckDBPyConnection, name: str) -> None:
        raise duckdb.IOException("offline")

    monkeypatch.setattr(DuckCon, "_install_via_duckdb_extensions", lambda self, name: False)
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "load_extension", load_extension)

    manager = DuckCon(extra_extensions=("excel",))

    with pytest.raises(RuntimeError, match="extra_extensions"):
        with manager:
            pass


def test_extensions_requires_open_connection() -> None:
    manager = DuckCon()

    with pytest.raises(RuntimeError, match="open"):
        manager.extensions()


def test_extensions_returns_metadata() -> None:
    manager = DuckCon()

    with manager:
        infos = manager.extensions()

    assert infos
    assert all(isinstance(info, ExtensionInfo) for info in infos)
    assert any(info.name for info in infos)


def test_load_nano_odbc_emits_deprecation(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = DuckCon()
    monkeypatch.setattr(DuckCon, "_load_nano_odbc", lambda self, install=True: None)

    with manager:
        with pytest.warns(DeprecationWarning):
            manager.load_nano_odbc(install=False)
