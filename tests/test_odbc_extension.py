"""Integration tests for the nano-ODBC extension helpers."""

from __future__ import annotations

import os

import pytest

from duckplus import DuckCon, Relation


_CONNECTION_ENV = "DUCKPLUS_TEST_ODBC_CONNECTION"
_QUERY_ENV = "DUCKPLUS_TEST_ODBC_QUERY"
_TABLE_ENV = "DUCKPLUS_TEST_ODBC_TABLE"


def _require_connection_string() -> str:
    connection_string = os.environ.get(_CONNECTION_ENV)
    if not connection_string:
        pytest.skip(
            "Set DUCKPLUS_TEST_ODBC_CONNECTION to run nano-ODBC integration tests.",
        )
    return connection_string

@pytest.mark.integration
def test_relation_from_odbc_query_executes() -> None:
    connection_string = _require_connection_string()
    query = os.environ.get(_QUERY_ENV)
    if not query:
        pytest.skip("Set DUCKPLUS_TEST_ODBC_QUERY to validate ODBC queries.")

    manager = DuckCon(extra_extensions=("nanodbc",))
    try:
        with manager:
            infos = manager.extensions()
            if not any(info.name == "nano_odbc" and info.loaded for info in infos):
                pytest.skip("nano-ODBC extension unavailable on this machine.")
            relation = Relation.from_odbc_query(manager, connection_string, query)
            rows = relation.relation.fetchall()
    except RuntimeError as exc:
        if "nano-ODBC extension" in str(exc):
            pytest.skip(f"nano-ODBC extension unavailable: {exc}")
        raise

    assert isinstance(rows, list)


@pytest.mark.integration
def test_relation_from_odbc_table_executes() -> None:
    connection_string = _require_connection_string()
    table = os.environ.get(_TABLE_ENV)
    if not table:
        pytest.skip("Set DUCKPLUS_TEST_ODBC_TABLE to validate table scans.")

    manager = DuckCon(extra_extensions=("nanodbc",))
    try:
        with manager:
            infos = manager.extensions()
            if not any(info.name == "nano_odbc" and info.loaded for info in infos):
                pytest.skip("nano-ODBC extension unavailable on this machine.")
            relation = Relation.from_odbc_table(manager, connection_string, table)
            rows = relation.relation.fetchall()
    except RuntimeError as exc:
        if "nano-ODBC extension" in str(exc):
            pytest.skip(f"nano-ODBC extension unavailable: {exc}")
        raise

    assert isinstance(rows, list)
