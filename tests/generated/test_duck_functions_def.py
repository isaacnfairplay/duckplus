"""Auto-generated DuckDB function contract tests. Buckets: d, e, f."""

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

@pytest.mark.parametrize('function_name', ['encode'])
def test_scalar_blob_e(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Blob')
    _assert_contract('scalar', 'Blob', namespace, function_name)

@pytest.mark.parametrize('function_name', ['from_base64', 'from_binary', 'from_hex'])
def test_scalar_blob_f(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Blob')
    _assert_contract('scalar', 'Blob', namespace, function_name)

@pytest.mark.parametrize('function_name', ['ends_with'])
def test_scalar_boolean_e(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['date_add'])
def test_scalar_generic_d(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['element_at', 'enum_code', 'equi_width_bins', 'error'])
def test_scalar_generic_e(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['fdiv', 'filter', 'finalize', 'flatten', 'fmod', 'format_pg_type', 'format_type'])
def test_scalar_generic_f(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['damerau_levenshtein', 'date_diff', 'date_part', 'date_sub', 'date_trunc', 'datediff', 'datepart', 'datesub', 'datetrunc', 'day', 'dayofmonth', 'dayofweek', 'dayofyear', 'decade', 'degrees', 'divide'])
def test_scalar_numeric_d(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['editdist3', 'epoch', 'epoch_ms', 'epoch_ns', 'epoch_us', 'era', 'even', 'exp'])
def test_scalar_numeric_e(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['factorial', 'floor'])
def test_scalar_numeric_f(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['dayname', 'decode'])
def test_scalar_varchar_d(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['enum_first', 'enum_last', 'enum_range', 'enum_range_boundary'])
def test_scalar_varchar_e(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['format', 'formatReadableDecimalSize', 'formatReadableSize', 'format_bytes', 'from_json', 'from_json_strict'])
def test_scalar_varchar_f(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['entropy'])
def test_aggregate_numeric_e(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['favg', 'first', 'fsum'])
def test_aggregate_numeric_f(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
