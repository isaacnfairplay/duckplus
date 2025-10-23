"""Auto-generated DuckDB function contract tests. Buckets: j, k, l."""

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

@pytest.mark.parametrize('function_name', ['json_contains', 'json_exists', 'json_valid'])
def test_scalar_boolean_j(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['like_escape', 'list_contains', 'list_has', 'list_has_all', 'list_has_any'])
def test_scalar_boolean_l(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['json', 'json_group_array', 'json_group_object', 'json_group_structure'])
def test_scalar_generic_j(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['least', 'list_aggr', 'list_aggregate', 'list_any_value', 'list_append', 'list_apply', 'list_approx_count_distinct', 'list_avg', 'list_bit_and', 'list_bit_or', 'list_bit_xor', 'list_bool_and', 'list_bool_or', 'list_cat', 'list_concat', 'list_count', 'list_distinct', 'list_entropy', 'list_filter', 'list_first', 'list_grade_up', 'list_histogram', 'list_intersect', 'list_kurtosis', 'list_kurtosis_pop', 'list_last', 'list_mad', 'list_max', 'list_median', 'list_min', 'list_mode', 'list_pack', 'list_prepend', 'list_product', 'list_reduce', 'list_resize', 'list_reverse', 'list_reverse_sort', 'list_select', 'list_sem', 'list_skewness', 'list_slice', 'list_sort', 'list_stddev_pop', 'list_stddev_samp', 'list_string_agg', 'list_sum', 'list_transform', 'list_value', 'list_var_pop', 'list_var_samp', 'list_where', 'list_zip'])
def test_scalar_generic_l(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['jaccard', 'jaro_similarity', 'jaro_winkler_similarity', 'json_array_length', 'julian'])
def test_scalar_numeric_j(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['last_day', 'lcm', 'least_common_multiple', 'len', 'length', 'length_grapheme', 'levenshtein', 'lgamma', 'list_cosine_distance', 'list_cosine_similarity', 'list_distance', 'list_dot_product', 'list_indexof', 'list_inner_product', 'list_negative_dot_product', 'list_negative_inner_product', 'list_position', 'list_unique', 'ln', 'log', 'log10', 'log2'])
def test_scalar_numeric_l(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['json_array', 'json_deserialize_sql', 'json_extract', 'json_extract_path', 'json_extract_path_text', 'json_extract_string', 'json_keys', 'json_merge_patch', 'json_object', 'json_pretty', 'json_quote', 'json_serialize_plan', 'json_serialize_sql', 'json_structure', 'json_transform', 'json_transform_strict', 'json_type', 'json_value'])
def test_scalar_varchar_j(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['lcase', 'left', 'left_grapheme', 'list_element', 'list_extract', 'lower', 'lpad', 'ltrim'])
def test_scalar_varchar_l(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['list'])
def test_aggregate_generic_l(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Generic')
    _assert_contract('aggregate', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['kahan_sum', 'kurtosis', 'kurtosis_pop'])
def test_aggregate_numeric_k(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['last'])
def test_aggregate_numeric_l(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['listagg'])
def test_aggregate_varchar_l(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Varchar')
    _assert_contract('aggregate', 'Varchar', namespace, function_name)
