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
