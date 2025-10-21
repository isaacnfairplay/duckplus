"""Aggregate DuckDB function helpers organised by domain."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

_SIDE_EFFECT_MODULES: tuple[str, ...] = (
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

# Import modules with registration side effects explicitly so tests can assert
# the dependency surface while keeping the helpers introspectable.
approximation: ModuleType = import_module(_SIDE_EFFECT_MODULES[0])
arg_extrema: ModuleType = import_module(_SIDE_EFFECT_MODULES[1])
extremum_by_value: ModuleType = import_module(_SIDE_EFFECT_MODULES[2])
extrema_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[3])
boolean: ModuleType = import_module(_SIDE_EFFECT_MODULES[4])
bitwise: ModuleType = import_module(_SIDE_EFFECT_MODULES[5])
bitstring_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[6])
counting: ModuleType = import_module(_SIDE_EFFECT_MODULES[7])
generic: ModuleType = import_module(_SIDE_EFFECT_MODULES[8])
list_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[9])
map_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[10])
median_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[11])
mode_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[12])
quantiles_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[13])
ordering: ModuleType = import_module(_SIDE_EFFECT_MODULES[14])
summation: ModuleType = import_module(_SIDE_EFFECT_MODULES[15])
string_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[16])
statistics: ModuleType = import_module(_SIDE_EFFECT_MODULES[17])
averages_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[18])
regression_module: ModuleType = import_module(_SIDE_EFFECT_MODULES[19])

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
from .arg_extrema import (  # noqa: E402  # Imported after side-effect module load.
    arg_max,
    arg_max_filter,
    arg_max_null,
    arg_max_null_filter,
    arg_min,
    arg_min_filter,
    arg_min_null,
    arg_min_null_filter,
)
from .extremum_by_value import (  # noqa: E402  # Imported after side-effect module load.
    max_by,
    max_by_filter,
    min_by,
    min_by_filter,
)
from .extrema import (  # noqa: E402  # Imported after side-effect module load.
    max,
    max_filter,
    min,
    min_filter,
)
from .boolean import (  # noqa: E402  # Imported after side-effect module load.
    bool_and,
    bool_and_filter,
    bool_or,
    bool_or_filter,
)
from .bitwise import (  # noqa: E402  # Imported after side-effect module load.
    bit_and,
    bit_and_filter,
    bit_or,
    bit_or_filter,
    bit_xor,
    bit_xor_filter,
)
from .bitstring import (  # noqa: E402  # Imported after side-effect module load.
    bitstring_agg,
    bitstring_agg_filter,
)
from .counting import (  # noqa: E402  # Imported after side-effect module load.
    count,
    count_filter,
    count_if,
    count_if_filter,
    count_star,
    count_star_filter,
    countif,
    countif_filter,
)
from .generic import (  # noqa: E402  # Imported after side-effect module load.
    any_value,
    any_value_filter,
)
from .list import (  # noqa: E402  # Imported after side-effect module load.
    list as duck_list,  # noqa: A002 - match DuckDB helper name.
    list_filter,
)
from .map import (  # noqa: E402  # Imported after side-effect module load.
    map as duck_map,  # noqa: A002 - match DuckDB helper name.
)
from .median import (  # noqa: E402  # Imported after side-effect module load.
    median,
    median_filter,
)
from .mode import (  # noqa: E402  # Imported after side-effect module load.
    mode,
    mode_filter,
)
from .quantiles import (  # noqa: E402  # Imported after side-effect module load.
    quantile,
    quantile_cont,
    quantile_cont_filter,
    quantile_disc,
    quantile_disc_filter,
    quantile_filter,
)
from .ordering import (  # noqa: E402  # Imported after side-effect module load.
    first,
    first_filter,
)
from .summation import (  # noqa: E402  # Imported after side-effect module load.
    product,
    product_filter,
    sum,
    sum_filter,
)
from .string import (  # noqa: E402  # Imported after side-effect module load.
    string_agg,
    string_agg_filter,
)
from .statistics import (  # noqa: E402  # Imported after side-effect module load.
    skewness,
    skewness_filter,
)
from .averages import (  # noqa: E402  # Imported after side-effect module load.
    avg,
    avg_filter,
    mean,
    mean_filter,
)
from .regression import (  # noqa: E402  # Imported after side-effect module load.
    covar_pop,
    covar_pop_filter,
    covar_samp,
    covar_samp_filter,
    regr_avgx,
    regr_avgx_filter,
    regr_avgy,
    regr_avgy_filter,
    regr_count,
    regr_count_filter,
    regr_intercept,
    regr_intercept_filter,
    regr_r2,
    regr_r2_filter,
    regr_slope,
    regr_slope_filter,
    regr_sxx,
    regr_sxx_filter,
    regr_sxy,
    regr_sxy_filter,
    regr_syy,
    regr_syy_filter,
)

list = duck_list  # pylint: disable=redefined-builtin
map = duck_map  # pylint: disable=redefined-builtin

SIDE_EFFECT_MODULES: tuple[str, ...] = _SIDE_EFFECT_MODULES

__all__ = [
    "approximation",
    "arg_extrema",
    "extremum_by_value",
    "extrema_module",
    "boolean",
    "bitwise",
    "bitstring_module",
    "counting",
    "generic",
    "list_module",
    "map_module",
    "median_module",
    "mode_module",
    "quantiles_module",
    "ordering",
    "summation",
    "string_module",
    "statistics",
    "averages_module",
    "regression_module",
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
    "arg_max",
    "arg_max_filter",
    "arg_max_null",
    "arg_max_null_filter",
    "arg_min",
    "arg_min_filter",
    "arg_min_null",
    "arg_min_null_filter",
    "max_by",
    "max_by_filter",
    "min_by",
    "min_by_filter",
    "max",
    "max_filter",
    "min",
    "min_filter",
    "bool_and",
    "bool_and_filter",
    "bool_or",
    "bool_or_filter",
    "bit_and",
    "bit_and_filter",
    "bit_or",
    "bit_or_filter",
    "bit_xor",
    "bit_xor_filter",
    "bitstring_agg",
    "bitstring_agg_filter",
    "count",
    "count_filter",
    "count_if",
    "count_if_filter",
    "count_star",
    "count_star_filter",
    "countif",
    "countif_filter",
    "any_value",
    "any_value_filter",
    "list",
    "list_filter",
    "map",
    "median",
    "median_filter",
    "mode",
    "mode_filter",
    "quantile",
    "quantile_cont",
    "quantile_cont_filter",
    "quantile_disc",
    "quantile_disc_filter",
    "quantile_filter",
    "first",
    "first_filter",
    "sum",
    "sum_filter",
    "product",
    "product_filter",
    "string_agg",
    "string_agg_filter",
    "skewness",
    "skewness_filter",
    "covar_pop",
    "covar_pop_filter",
    "covar_samp",
    "covar_samp_filter",
    "regr_avgx",
    "regr_avgx_filter",
    "regr_avgy",
    "regr_avgy_filter",
    "regr_count",
    "regr_count_filter",
    "regr_intercept",
    "regr_intercept_filter",
    "regr_r2",
    "regr_r2_filter",
    "regr_slope",
    "regr_slope_filter",
    "regr_sxx",
    "regr_sxx_filter",
    "regr_sxy",
    "regr_sxy_filter",
    "regr_syy",
    "regr_syy_filter",
    "avg",
    "avg_filter",
    "mean",
    "mean_filter",
    "SIDE_EFFECT_MODULES",
]
