from __future__ import annotations

from importlib import import_module
from typing import Any

import duckplus
import pytest

connect_mod = import_module("duckplus.connect")


def test_connect_executes_simple_query() -> None:
    with duckplus.connect() as conn:
        result = conn.raw.execute("SELECT 42").fetchone()

    assert result == (42,)


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
