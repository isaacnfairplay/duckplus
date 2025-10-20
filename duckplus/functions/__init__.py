"""Domain-organised DuckDB function helpers."""

from __future__ import annotations

from . import aggregate
from .aggregate import (
    SIDE_EFFECT_MODULES as _AGGREGATE_SIDE_EFFECT_MODULES,
    approx_count_distinct,
    approx_count_distinct_filter,
    approx_quantile_generic,
    approx_quantile_generic_filter,
    approx_quantile_numeric,
    approx_quantile_numeric_filter,
    approx_top_k,
    approx_top_k_filter,
    histogram,
    histogram_exact,
    histogram_exact_filter,
    histogram_filter,
)

SIDE_EFFECT_MODULES: tuple[str, ...] = (
    "duckplus.functions.aggregate",
    *_AGGREGATE_SIDE_EFFECT_MODULES,
)

__all__ = [
    "aggregate",
    "approx_count_distinct",
    "approx_count_distinct_filter",
    "approx_quantile_generic",
    "approx_quantile_generic_filter",
    "approx_quantile_numeric",
    "approx_quantile_numeric_filter",
    "approx_top_k",
    "approx_top_k_filter",
    "histogram",
    "histogram_exact",
    "histogram_exact_filter",
    "histogram_filter",
    "SIDE_EFFECT_MODULES",
]
