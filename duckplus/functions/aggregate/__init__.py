"""Aggregate DuckDB function helpers organised by domain."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

_SIDE_EFFECT_MODULES: tuple[str, ...] = ("duckplus.functions.aggregate.approximation",)

# Import modules with registration side effects explicitly so tests can assert
# the dependency surface while keeping the helpers introspectable.
approximation: ModuleType = import_module(_SIDE_EFFECT_MODULES[0])

from .approximation import (  # noqa: E402  # Imported after side-effect module load.
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

SIDE_EFFECT_MODULES: tuple[str, ...] = _SIDE_EFFECT_MODULES

__all__ = [
    "approximation",
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
