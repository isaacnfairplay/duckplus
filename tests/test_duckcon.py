import duckdb
import pytest

from duckplus import DuckCon


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
