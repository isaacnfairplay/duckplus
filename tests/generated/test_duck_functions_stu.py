"""Auto-generated DuckDB function contract tests. Buckets: s, t, u."""

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

@pytest.mark.parametrize('function_name', ['unbin', 'unhex'])
def test_scalar_blob_u(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Blob')
    _assert_contract('scalar', 'Blob', namespace, function_name)

@pytest.mark.parametrize('function_name', ['signbit', 'starts_with', 'struct_contains', 'struct_has', 'suffix'])
def test_scalar_boolean_s(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['session_user', 'setseed', 'shobj_description', 'split_part', 'struct_concat', 'struct_extract', 'struct_extract_at', 'struct_insert', 'struct_pack', 'struct_update'])
def test_scalar_generic_s(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['union_extract', 'union_tag', 'union_value', 'unpivot_list', 'user'])
def test_scalar_generic_u(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['second', 'set_bit', 'sign', 'sin', 'sinh', 'sqrt', 'strlen', 'strpos', 'strptime', 'struct_indexof', 'struct_position', 'subtract'])
def test_scalar_numeric_s(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['tan', 'tanh', 'time_bucket', 'timetz_byte_comparable', 'timezone', 'timezone_hour', 'timezone_minute', 'to_centuries', 'to_days', 'to_decades', 'to_hours', 'to_microseconds', 'to_millennia', 'to_milliseconds', 'to_minutes', 'to_months', 'to_quarters', 'to_seconds', 'to_timestamp', 'to_weeks', 'to_years', 'today', 'transaction_timestamp', 'trunc', 'try_strptime', 'txid_current'])
def test_scalar_numeric_t(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['unicode', 'uuid_extract_timestamp', 'uuid_extract_version'])
def test_scalar_numeric_u(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['sha1', 'sha256', 'split', 'stats', 'str_split', 'str_split_regex', 'strftime', 'string_split', 'string_split_regex', 'string_to_array', 'strip_accents', 'substr', 'substring', 'substring_grapheme'])
def test_scalar_varchar_s(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['to_base', 'to_base64', 'to_binary', 'to_hex', 'to_json', 'translate', 'trim', 'typeof'])
def test_scalar_varchar_t(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['ucase', 'upper', 'url_decode', 'url_encode', 'uuid', 'uuidv4', 'uuidv7'])
def test_scalar_varchar_u(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['sem', 'skewness', 'stddev', 'stddev_pop', 'stddev_samp', 'sum', 'sum_no_overflow', 'sumkahan'])
def test_aggregate_numeric_s(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['string_agg'])
def test_aggregate_varchar_s(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Varchar')
    _assert_contract('aggregate', 'Varchar', namespace, function_name)
