import duckdb
import pytest

from duckplus import DuckCon
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

    with manager:
        assert manager.apply_helper("echo", 42) == 42

    with pytest.raises(KeyError):
        manager.apply_helper("missing")

    manager.register_helper("echo", echo_helper, overwrite=True)
    with manager:
        assert manager.apply_helper("echo", 7) == 7


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
