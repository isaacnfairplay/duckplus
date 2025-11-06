"""Auto-generated DuckDB function contract tests. Buckets: c."""

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

@pytest.mark.parametrize('function_name', ['create_sort_key'])
def test_scalar_blob_c(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Blob')
    _assert_contract('scalar', 'Blob', namespace, function_name)

@pytest.mark.parametrize('function_name', ['can_cast_implicitly', 'contains'])
def test_scalar_boolean_c(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['cast_to_type', 'col_description', 'combine', 'concat', 'constant_or_null', 'current_catalog', 'current_role', 'current_user'])
def test_scalar_generic_c(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['cardinality', 'cbrt', 'ceil', 'ceiling', 'century', 'char_length', 'character_length', 'cos', 'cosh', 'cot', 'current_connection_id', 'current_date', 'current_localtime', 'current_localtimestamp', 'current_query_id', 'current_transaction_id', 'currval'])
def test_scalar_numeric_c(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['chr', 'concat_ws', 'current_database', 'current_query', 'current_schema', 'current_schemas', 'current_setting'])
def test_scalar_varchar_c(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['corr', 'count', 'count_if', 'count_star', 'countif', 'covar_pop', 'covar_samp'])
def test_aggregate_numeric_c(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
