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

    assert "duckplus.functions.aggregate" in functions.SIDE_EFFECT_MODULES
    assert "duckplus.functions.aggregate.approximation" in functions.SIDE_EFFECT_MODULES

    for module_name in functions.SIDE_EFFECT_MODULES:
        module = import_module(module_name)
        assert module.__name__ == module_name


def test_aggregate_barrel_reexports_helpers() -> None:
    import duckplus.functions.aggregate as aggregate

    assert aggregate.approximation.__name__ == "duckplus.functions.aggregate.approximation"
    assert aggregate.approx_count_distinct is aggregate.approximation.approx_count_distinct
    assert aggregate.approx_top_k_filter is aggregate.approximation.approx_top_k_filter

    assert aggregate.SIDE_EFFECT_MODULES == ("duckplus.functions.aggregate.approximation",)

    for module_name in aggregate.SIDE_EFFECT_MODULES:
        module = import_module(module_name)
        assert module is aggregate.approximation
