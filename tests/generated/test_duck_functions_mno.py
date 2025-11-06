"""Auto-generated DuckDB function contract tests. Buckets: m, n, o."""

from __future__ import annotations

import pytest

from duckplus.static_typed import (
    AGGREGATE_FUNCTIONS,
    SCALAR_FUNCTIONS,
    WINDOW_FUNCTIONS,
)

from tests.test_duckdb_contracts import _load_todo_section

_PARITY_LEDGER = _load_todo_section('DUCK_FUNCTION_PARITY')

def _assert_contract(kind: str, expected_namespace: str, namespace: object, function_name: str) -> None:
    if hasattr(namespace, function_name):
        return
    mismatches = _PARITY_LEDGER.get(kind, {}).get(expected_namespace, {})
    for actual_namespace, functions in mismatches.items():
        if function_name in functions:
            return
    for namespace_name, namespace_mismatches in _PARITY_LEDGER.get(kind, {}).items():
        for actual_namespace, functions in namespace_mismatches.items():
            if function_name in functions:
                return
    pytest.fail(
        f"Missing {function_name!r} on namespace {expected_namespace} without TODO parity entry"
    )

@pytest.mark.parametrize('function_name', ['map_contains'])
def test_scalar_boolean_m(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['not_ilike_escape', 'not_like_escape'])
def test_scalar_boolean_n(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['map', 'map_concat', 'map_contains_entry', 'map_contains_value', 'map_entries', 'map_extract', 'map_extract_value', 'map_from_entries', 'map_keys', 'map_to_pg_oid', 'map_values', 'md5_number_lower', 'md5_number_upper'])
def test_scalar_generic_m(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['nullif'])
def test_scalar_generic_n(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['obj_description'])
def test_scalar_generic_o(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['make_date', 'make_time', 'make_timestamp', 'make_timestamp_ms', 'make_timestamp_ns', 'make_timestamptz', 'md5_number', 'microsecond', 'millennium', 'millisecond', 'minute', 'mismatches', 'mod', 'month', 'multiply'])
def test_scalar_numeric_m(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['nanosecond', 'nextafter', 'nextval', 'normalized_interval', 'now'])
def test_scalar_numeric_n(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['octet_length', 'ord'])
def test_scalar_numeric_o(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['md5', 'monthname'])
def test_scalar_varchar_m(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['nfc_normalize'])
def test_scalar_varchar_n(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['max', 'median', 'min', 'mode'])
def test_aggregate_generic_m(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Generic')
    _assert_contract('aggregate', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['mad', 'max_by', 'mean', 'min_by'])
def test_aggregate_numeric_m(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
