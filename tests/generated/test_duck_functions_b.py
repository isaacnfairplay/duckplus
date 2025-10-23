"""Auto-generated DuckDB function contract tests. Buckets: b."""

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

@pytest.mark.parametrize('function_name', ['bit_count', 'bit_length', 'bit_position', 'bitstring'])
def test_scalar_numeric_b(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['bar', 'base64', 'bin'])
def test_scalar_varchar_b(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['bool_and', 'bool_or'])
def test_aggregate_boolean_b(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Boolean')
    _assert_contract('aggregate', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['bit_and', 'bit_or', 'bit_xor', 'bitstring_agg'])
def test_aggregate_numeric_b(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
