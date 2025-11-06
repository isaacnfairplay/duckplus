"""Auto-generated DuckDB function contract tests. Buckets: p, q, r."""

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

@pytest.mark.parametrize('function_name', ['prefix'])
def test_scalar_boolean_p(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['regexp_full_match', 'regexp_matches'])
def test_scalar_boolean_r(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['pg_collation_is_visible', 'pg_conf_load_time', 'pg_conversion_is_visible', 'pg_function_is_visible', 'pg_get_constraintdef', 'pg_get_expr', 'pg_get_viewdef', 'pg_has_role', 'pg_is_other_temp_schema', 'pg_my_temp_schema', 'pg_opclass_is_visible', 'pg_operator_is_visible', 'pg_opfamily_is_visible', 'pg_postmaster_start_time', 'pg_size_pretty', 'pg_table_is_visible', 'pg_ts_config_is_visible', 'pg_ts_dict_is_visible', 'pg_ts_parser_is_visible', 'pg_ts_template_is_visible', 'pg_type_is_visible', 'pg_typeof'])
def test_scalar_generic_p(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['reduce', 'regexp_split_to_table', 'remap_struct', 'replace_type', 'round_even', 'roundbankers', 'row'])
def test_scalar_generic_r(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['pi', 'position', 'pow', 'power'])
def test_scalar_numeric_p(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['quarter'])
def test_scalar_numeric_q(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['radians', 'random', 'range', 'round'])
def test_scalar_numeric_r(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['parse_dirname', 'parse_dirpath', 'parse_duckdb_log_message', 'parse_filename', 'parse_path', 'printf'])
def test_scalar_varchar_p(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['regexp_escape', 'regexp_extract', 'regexp_extract_all', 'regexp_replace', 'regexp_split_to_array', 'repeat', 'replace', 'reverse', 'right', 'right_grapheme', 'row_to_json', 'rpad', 'rtrim'])
def test_scalar_varchar_r(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['quantile', 'quantile_disc'])
def test_aggregate_generic_q(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Generic')
    _assert_contract('aggregate', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['product'])
def test_aggregate_numeric_p(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['quantile_cont'])
def test_aggregate_numeric_q(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'reservoir_quantile'])
def test_aggregate_numeric_r(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Numeric')
    _assert_contract('aggregate', 'Numeric', namespace, function_name)
