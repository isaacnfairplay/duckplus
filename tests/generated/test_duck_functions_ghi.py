"""Auto-generated DuckDB function contract tests. Buckets: g, h, i."""

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

@pytest.mark.parametrize('function_name', ['ilike_escape', 'in_search_path', 'is_histogram_other_bin', 'isfinite', 'isinf', 'isnan'])
def test_scalar_boolean_i(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Boolean')
    _assert_contract('scalar', 'Boolean', namespace, function_name)

@pytest.mark.parametrize('function_name', ['generate_subscripts', 'geomean', 'geometric_mean', 'get_block_size', 'grade_up', 'greatest'])
def test_scalar_generic_g(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['has_any_column_privilege', 'has_column_privilege', 'has_database_privilege', 'has_foreign_data_wrapper_privilege', 'has_function_privilege', 'has_language_privilege', 'has_schema_privilege', 'has_sequence_privilege', 'has_server_privilege', 'has_table_privilege', 'has_tablespace_privilege'])
def test_scalar_generic_h(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['inet_client_addr', 'inet_client_port', 'inet_server_addr', 'inet_server_port'])
def test_scalar_generic_i(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Generic')
    _assert_contract('scalar', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['gamma', 'gcd', 'generate_series', 'get_bit', 'get_current_time', 'get_current_timestamp', 'greatest_common_divisor'])
def test_scalar_numeric_g(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['hamming', 'hash', 'hour'])
def test_scalar_numeric_h(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['instr', 'isodow', 'isoyear'])
def test_scalar_numeric_i(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Numeric')
    _assert_contract('scalar', 'Numeric', namespace, function_name)

@pytest.mark.parametrize('function_name', ['gen_random_uuid', 'getvariable'])
def test_scalar_varchar_g(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['hex'])
def test_scalar_varchar_h(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['icu_collate_af', 'icu_collate_am', 'icu_collate_ar', 'icu_collate_ar_sa', 'icu_collate_as', 'icu_collate_az', 'icu_collate_be', 'icu_collate_bg', 'icu_collate_bn', 'icu_collate_bo', 'icu_collate_br', 'icu_collate_bs', 'icu_collate_ca', 'icu_collate_ceb', 'icu_collate_chr', 'icu_collate_cs', 'icu_collate_cy', 'icu_collate_da', 'icu_collate_de', 'icu_collate_de_at', 'icu_collate_dsb', 'icu_collate_dz', 'icu_collate_ee', 'icu_collate_el', 'icu_collate_en', 'icu_collate_en_us', 'icu_collate_eo', 'icu_collate_es', 'icu_collate_et', 'icu_collate_fa', 'icu_collate_fa_af', 'icu_collate_ff', 'icu_collate_fi', 'icu_collate_fil', 'icu_collate_fo', 'icu_collate_fr', 'icu_collate_fr_ca', 'icu_collate_fy', 'icu_collate_ga', 'icu_collate_gl', 'icu_collate_gu', 'icu_collate_ha', 'icu_collate_haw', 'icu_collate_he', 'icu_collate_he_il', 'icu_collate_hi', 'icu_collate_hr', 'icu_collate_hsb', 'icu_collate_hu', 'icu_collate_hy', 'icu_collate_id', 'icu_collate_id_id', 'icu_collate_ig', 'icu_collate_is', 'icu_collate_it', 'icu_collate_ja', 'icu_collate_ka', 'icu_collate_kk', 'icu_collate_kl', 'icu_collate_km', 'icu_collate_kn', 'icu_collate_ko', 'icu_collate_kok', 'icu_collate_ku', 'icu_collate_ky', 'icu_collate_lb', 'icu_collate_lkt', 'icu_collate_ln', 'icu_collate_lo', 'icu_collate_lt', 'icu_collate_lv', 'icu_collate_mk', 'icu_collate_ml', 'icu_collate_mn', 'icu_collate_mr', 'icu_collate_ms', 'icu_collate_mt', 'icu_collate_my', 'icu_collate_nb', 'icu_collate_nb_no', 'icu_collate_ne', 'icu_collate_nl', 'icu_collate_nn', 'icu_collate_noaccent', 'icu_collate_om', 'icu_collate_or', 'icu_collate_pa', 'icu_collate_pa_in', 'icu_collate_pl', 'icu_collate_ps', 'icu_collate_pt', 'icu_collate_ro', 'icu_collate_ru', 'icu_collate_sa', 'icu_collate_se', 'icu_collate_si', 'icu_collate_sk', 'icu_collate_sl', 'icu_collate_smn', 'icu_collate_sq', 'icu_collate_sr', 'icu_collate_sr_ba', 'icu_collate_sr_me', 'icu_collate_sr_rs', 'icu_collate_sv', 'icu_collate_sw', 'icu_collate_ta', 'icu_collate_te', 'icu_collate_th', 'icu_collate_tk', 'icu_collate_to', 'icu_collate_tr', 'icu_collate_ug', 'icu_collate_uk', 'icu_collate_ur', 'icu_collate_uz', 'icu_collate_vi', 'icu_collate_wae', 'icu_collate_wo', 'icu_collate_xh', 'icu_collate_yi', 'icu_collate_yo', 'icu_collate_yue', 'icu_collate_yue_cn', 'icu_collate_zh', 'icu_collate_zh_cn', 'icu_collate_zh_hk', 'icu_collate_zh_mo', 'icu_collate_zh_sg', 'icu_collate_zh_tw', 'icu_collate_zu', 'icu_sort_key'])
def test_scalar_varchar_i(function_name: str) -> None:
    namespace = getattr(SCALAR_FUNCTIONS, 'Varchar')
    _assert_contract('scalar', 'Varchar', namespace, function_name)

@pytest.mark.parametrize('function_name', ['histogram', 'histogram_exact'])
def test_aggregate_generic_h(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Generic')
    _assert_contract('aggregate', 'Generic', namespace, function_name)

@pytest.mark.parametrize('function_name', ['group_concat'])
def test_aggregate_varchar_g(function_name: str) -> None:
    namespace = getattr(AGGREGATE_FUNCTIONS, 'Varchar')
    _assert_contract('aggregate', 'Varchar', namespace, function_name)
