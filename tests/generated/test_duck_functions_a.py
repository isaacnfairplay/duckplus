"""Auto-generated DuckDB function contract tests. Buckets: a."""

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

@pytest.mark.parametrize('function_name', ['array_contains', 'array_has', 'array_has_all', 'array_has_any'])
def test_scalar_boolean_a(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['aggregate', 'apply', 'array_aggr', 'array_aggregate', 'array_append', 'array_apply', 'array_cat', 'array_concat', 'array_distinct', 'array_filter', 'array_grade_up', 'array_intersect', 'array_pop_back', 'array_pop_front', 'array_prepend', 'array_push_back', 'array_push_front', 'array_reduce', 'array_resize', 'array_reverse', 'array_reverse_sort', 'array_select', 'array_slice', 'array_sort', 'array_transform', 'array_value', 'array_where', 'array_zip'])
def test_scalar_generic_a(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['abs', 'acos', 'acosh', 'add', 'age', 'array_cosine_distance', 'array_cosine_similarity', 'array_cross_product', 'array_distance', 'array_dot_product', 'array_indexof', 'array_inner_product', 'array_length', 'array_negative_dot_product', 'array_negative_inner_product', 'array_position', 'array_unique', 'ascii', 'asin', 'asinh', 'atan', 'atan2', 'atanh'])
def test_scalar_numeric_a(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['alias', 'array_extract', 'array_to_json', 'array_to_string', 'array_to_string_comma_default'])
def test_scalar_varchar_a(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['approx_top_k', 'array_agg'])
def test_aggregate_generic_a(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Generic')
    _assert_contract('aggregate', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['any_value', 'approx_count_distinct', 'approx_quantile', 'arbitrary', 'arg_max', 'arg_max_null', 'arg_min', 'arg_min_null', 'argmax', 'argmin', 'avg'])
def test_aggregate_numeric_a(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
