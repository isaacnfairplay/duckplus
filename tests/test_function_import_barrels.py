"""Ensure function import barrels expose helpers and side effects."""

from __future__ import annotations

from importlib import import_module


def test_top_level_functions_barrel_reexports_helpers() -> None:
    import duckplus.functions as functions

    assert functions.approx_count_distinct.__module__ == (
        "duckplus.functions.aggregate.approximation"
    )
    assert functions.histogram_filter.__module__ == (
        "duckplus.functions.aggregate.approximation"
    )
    assert functions.arg_max.__module__ == "duckplus.functions.aggregate.arg_extrema"
    assert functions.arg_min_null_filter.__module__ == (
        "duckplus.functions.aggregate.arg_extrema"
    )
    assert functions.max_by.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    assert functions.max_by_filter.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    assert functions.min_by.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    assert functions.min_by_filter.__module__ == (
        "duckplus.functions.aggregate.extremum_by_value"
    )
    assert functions.max.__module__ == "duckplus.functions.aggregate.extrema"
    assert functions.max_filter.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    assert functions.min.__module__ == "duckplus.functions.aggregate.extrema"
    assert functions.min_filter.__module__ == (
        "duckplus.functions.aggregate.extrema"
    )
    assert functions.bool_and.__module__ == "duckplus.functions.aggregate.boolean"
    assert functions.bool_or_filter.__module__ == (
        "duckplus.functions.aggregate.boolean"
    )
    assert functions.bit_and.__module__ == "duckplus.functions.aggregate.bitwise"
    assert functions.bit_or_filter.__module__ == (
        "duckplus.functions.aggregate.bitwise"
    )
    assert functions.bit_xor.__module__ == "duckplus.functions.aggregate.bitwise"
    assert functions.bitstring_agg.__module__ == (
        "duckplus.functions.aggregate.bitstring"
    )
    assert functions.bitstring_agg_filter.__module__ == (
        "duckplus.functions.aggregate.bitstring"
    )
    assert functions.count.__module__ == "duckplus.functions.aggregate.counting"
    assert functions.count_star_filter.__module__ == (
        "duckplus.functions.aggregate.counting"
    )
    assert functions.any_value.__module__ == "duckplus.functions.aggregate.generic"
    assert functions.any_value_filter.__module__ == (
        "duckplus.functions.aggregate.generic"
    )
    assert functions.list.__module__ == "duckplus.functions.aggregate.list"
    assert functions.list_filter.__module__ == "duckplus.functions.aggregate.list"
    assert functions.map.__module__ == "duckplus.functions.aggregate.map"
    assert functions.median.__module__ == "duckplus.functions.aggregate.median"
    assert functions.median_filter.__module__ == (
        "duckplus.functions.aggregate.median"
    )
    assert functions.mode.__module__ == "duckplus.functions.aggregate.mode"
    assert functions.mode_filter.__module__ == "duckplus.functions.aggregate.mode"
    assert functions.quantile.__module__ == "duckplus.functions.aggregate.quantiles"
    assert functions.quantile_filter.__module__ == (
        "duckplus.functions.aggregate.quantiles"
    )
    assert functions.quantile_cont.__module__ == (
        "duckplus.functions.aggregate.quantiles"
    )
    assert functions.quantile_cont_filter.__module__ == (
        "duckplus.functions.aggregate.quantiles"
    )
    assert functions.quantile_disc.__module__ == (
        "duckplus.functions.aggregate.quantiles"
    )
    assert functions.quantile_disc_filter.__module__ == (
        "duckplus.functions.aggregate.quantiles"
    )
    assert functions.first.__module__ == "duckplus.functions.aggregate.ordering"
    assert functions.first_filter.__module__ == (
        "duckplus.functions.aggregate.ordering"
    )
    assert functions.avg.__module__ == "duckplus.functions.aggregate.averages"
    assert functions.avg_filter.__module__ == (
        "duckplus.functions.aggregate.averages"
    )
    assert functions.mean.__module__ == "duckplus.functions.aggregate.averages"
    assert functions.mean_filter.__module__ == (
        "duckplus.functions.aggregate.averages"
    )
    assert functions.sum.__module__ == "duckplus.functions.aggregate.summation"
    assert functions.sum_filter.__module__ == (
        "duckplus.functions.aggregate.summation"
    )
    assert functions.product.__module__ == (
        "duckplus.functions.aggregate.summation"
    )
    assert functions.product_filter.__module__ == (
        "duckplus.functions.aggregate.summation"
    )
    assert functions.string_agg.__module__ == "duckplus.functions.aggregate.string"
    assert functions.string_agg_filter.__module__ == (
        "duckplus.functions.aggregate.string"
    )
    assert functions.split_part.__module__ == (
        "duckplus.functions.scalar.string"
    )
    assert functions.array_to_string.__module__ == (
        "duckplus.functions.scalar.string"
    )
    assert functions.array_to_string_comma_default.__module__ == (
        "duckplus.functions.scalar.string"
    )
    assert functions.array_append.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_intersect.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_pop_back.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_pop_front.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_prepend.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_push_back.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_push_front.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.array_reverse.__module__ == (
        "duckplus.functions.scalar.list"
    )
    assert functions.current_catalog.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_database.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_query.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_role.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_schema.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_schemas.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.current_user.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.session_user.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.pg_get_constraintdef.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.pg_get_viewdef.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.pg_size_pretty.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.pg_typeof.__module__ == (
        "duckplus.functions.scalar.system"
    )
    assert functions.has_any_column_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_column_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_database_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_foreign_data_wrapper_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_function_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_language_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_schema_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_sequence_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_server_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_table_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.has_tablespace_privilege.__module__ == (
        "duckplus.functions.scalar.postgres_privilege"
    )
    assert functions.pg_collation_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_conversion_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_function_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_has_role.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_opclass_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_operator_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_opfamily_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_table_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_ts_config_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_ts_dict_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_ts_parser_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_ts_template_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.pg_type_is_visible.__module__ == (
        "duckplus.functions.scalar.postgres_visibility"
    )
    assert functions.skewness.__module__ == "duckplus.functions.aggregate.statistics"
    assert functions.skewness_filter.__module__ == (
        "duckplus.functions.aggregate.statistics"
    )
    assert functions.covar_pop.__module__ == (
        "duckplus.functions.aggregate.regression"
    )
    assert functions.regr_slope_filter.__module__ == (
        "duckplus.functions.aggregate.regression"
    )

    assert "duckplus.functions.aggregate" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.approximation" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.arg_extrema" in functions.SIDE_EFFECT_MODULES
    assert (
        "duckplus.functions.aggregate.extremum_by_value"
        in functions.SIDE_EFFECT_MODULES
    )
    assert "duckplus.functions.aggregate.extrema" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.boolean" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.bitwise" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.bitstring" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.counting" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.generic" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.list" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.map" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.median" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.mode" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.quantiles" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.ordering" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.summation" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.string" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.statistics" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.averages" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.regression" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.scalar" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.scalar.string" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.scalar.list" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.scalar.system" in functions.SIDE_EFFECT_MODULES
    assert (
        "duckplus.functions.scalar.postgres_privilege"
        in functions.SIDE_EFFECT_MODULES
    )
    assert (
        "duckplus.functions.scalar.postgres_visibility"
        in functions.SIDE_EFFECT_MODULES
    )

    for module_name in functions.SIDE_EFFECT_MODULES:
        module = import_module(module_name)
        assert module.__name__ == module_name


def test_aggregate_barrel_reexports_helpers() -> None:
    import duckplus.functions.aggregate as aggregate

    assert aggregate.approximation.__name__ == "duckplus.functions.aggregate.approximation"
    assert aggregate.approx_count_distinct is aggregate.approximation.approx_count_distinct
    assert aggregate.approx_top_k_filter is aggregate.approximation.approx_top_k_filter
    assert aggregate.mode_module.__name__ == "duckplus.functions.aggregate.mode"

    assert aggregate.SIDE_EFFECT_MODULES == (
        "duckplus.functions.aggregate.approximation",
        "duckplus.functions.aggregate.arg_extrema",
        "duckplus.functions.aggregate.extremum_by_value",
        "duckplus.functions.aggregate.extrema",
        "duckplus.functions.aggregate.boolean",
        "duckplus.functions.aggregate.bitwise",
        "duckplus.functions.aggregate.bitstring",
        "duckplus.functions.aggregate.counting",
        "duckplus.functions.aggregate.generic",
        "duckplus.functions.aggregate.list",
        "duckplus.functions.aggregate.map",
        "duckplus.functions.aggregate.median",
        "duckplus.functions.aggregate.mode",
        "duckplus.functions.aggregate.quantiles",
        "duckplus.functions.aggregate.ordering",
        "duckplus.functions.aggregate.summation",
        "duckplus.functions.aggregate.string",
        "duckplus.functions.aggregate.statistics",
        "duckplus.functions.aggregate.averages",
        "duckplus.functions.aggregate.regression",
    )

    for module_name in aggregate.SIDE_EFFECT_MODULES:
        module = import_module(module_name)
        assert module in (
            aggregate.approximation,
            aggregate.arg_extrema,
            aggregate.extremum_by_value,
            aggregate.extrema_module,
            aggregate.boolean,
            aggregate.bitwise,
            aggregate.bitstring_module,
            aggregate.counting,
            aggregate.generic,
            aggregate.list_module,
            aggregate.map_module,
            aggregate.median_module,
            aggregate.mode_module,
            aggregate.quantiles_module,
            aggregate.ordering,
            aggregate.summation,
            aggregate.string_module,
            aggregate.statistics,
            aggregate.averages_module,
            aggregate.regression_module,
        )

def test_scalar_barrel_reexports_helpers() -> None:
    import duckplus.functions.scalar as scalar

    assert scalar.string_module.__name__ == "duckplus.functions.scalar.string"
    assert scalar.list_module.__name__ == "duckplus.functions.scalar.list"
    assert (
        scalar.postgres_privilege_module.__name__
        == "duckplus.functions.scalar.postgres_privilege"
    )
    assert (
        scalar.postgres_visibility_module.__name__
        == "duckplus.functions.scalar.postgres_visibility"
    )
    assert scalar.split_part is scalar.string.split_part
    assert scalar.array_to_string is scalar.string.array_to_string
    assert (
        scalar.array_to_string_comma_default
        is scalar.string.array_to_string_comma_default
    )
    assert scalar.array_append is scalar.list.array_append
    assert scalar.array_intersect is scalar.list.array_intersect
    assert scalar.array_pop_back is scalar.list.array_pop_back
    assert scalar.array_pop_front is scalar.list.array_pop_front
    assert scalar.array_prepend is scalar.list.array_prepend
    assert scalar.array_push_back is scalar.list.array_push_back
    assert scalar.array_push_front is scalar.list.array_push_front
    assert scalar.array_reverse is scalar.list.array_reverse
    assert (
        scalar.has_any_column_privilege
        is scalar.postgres_privilege.has_any_column_privilege
    )
    assert (
        scalar.has_tablespace_privilege
        is scalar.postgres_privilege.has_tablespace_privilege
    )
    assert (
        scalar.pg_collation_is_visible
        is scalar.postgres_visibility.pg_collation_is_visible
    )
    assert (
        scalar.pg_conversion_is_visible
        is scalar.postgres_visibility.pg_conversion_is_visible
    )
    assert (
        scalar.pg_function_is_visible
        is scalar.postgres_visibility.pg_function_is_visible
    )
    assert (
        scalar.pg_has_role is scalar.postgres_visibility.pg_has_role
    )
    assert (
        scalar.pg_opclass_is_visible
        is scalar.postgres_visibility.pg_opclass_is_visible
    )
    assert (
        scalar.pg_operator_is_visible
        is scalar.postgres_visibility.pg_operator_is_visible
    )
    assert (
        scalar.pg_opfamily_is_visible
        is scalar.postgres_visibility.pg_opfamily_is_visible
    )
    assert (
        scalar.pg_table_is_visible
        is scalar.postgres_visibility.pg_table_is_visible
    )
    assert (
        scalar.pg_ts_config_is_visible
        is scalar.postgres_visibility.pg_ts_config_is_visible
    )
    assert (
        scalar.pg_ts_dict_is_visible
        is scalar.postgres_visibility.pg_ts_dict_is_visible
    )
    assert (
        scalar.pg_ts_parser_is_visible
        is scalar.postgres_visibility.pg_ts_parser_is_visible
    )
    assert (
        scalar.pg_ts_template_is_visible
        is scalar.postgres_visibility.pg_ts_template_is_visible
    )
    assert (
        scalar.pg_type_is_visible
        is scalar.postgres_visibility.pg_type_is_visible
    )

    assert scalar.SIDE_EFFECT_MODULES == (
        "duckplus.functions.scalar.string",
        "duckplus.functions.scalar.list",
        "duckplus.functions.scalar.system",
        "duckplus.functions.scalar.postgres_privilege",
        "duckplus.functions.scalar.postgres_visibility",
    )

    for module_name in scalar.SIDE_EFFECT_MODULES:
        module = import_module(module_name)
        assert module in (
            scalar.string_module,
            scalar.list_module,
            scalar.system_module,
            scalar.postgres_privilege_module,
            scalar.postgres_visibility_module,
        )
