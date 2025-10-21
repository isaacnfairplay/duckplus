# DuckDB Function Module Audit

This audit inventories the generated DuckDB function namespaces so that we can break the monolithic `_generated_function_namespaces.py` module into focused, direct-Python modules without reintroducing runtime registries. Counts were gathered by parsing the generated module and tallying the number of wrapper methods defined on each namespace class.

## Namespace scale snapshot

| Namespace class | Function type | Return category | Wrapper count |
| --- | --- | --- | --- |
| `AggregateBlobFunctions` | aggregate | blob | 16 |
| `AggregateBooleanFunctions` | aggregate | boolean | 4 |
| `AggregateGenericFunctions` | aggregate | generic | 64 |
| `AggregateNumericFunctions` | aggregate | numeric | 112 |
| `AggregateVarcharFunctions` | aggregate | varchar | 22 |
| `ScalarBlobFunctions` | scalar | blob | 8 |
| `ScalarBooleanFunctions` | scalar | boolean | 41 |
| `ScalarGenericFunctions` | scalar | generic | 142 |
| `ScalarNumericFunctions` | scalar | numeric | 198 |
| `ScalarVarcharFunctions` | scalar | varchar | 235 |
| `WindowBlobFunctions` | window | blob | 0 |
| `WindowBooleanFunctions` | window | boolean | 0 |
| `WindowGenericFunctions` | window | generic | 0 |
| `WindowNumericFunctions` | window | numeric | 0 |
| `WindowVarcharFunctions` | window | varchar | 0 |

Window namespaces currently expose no wrappers beyond the legacy `_IDENTIFIER_FUNCTIONS` placeholders, which means the per-function migration can focus on aggregate and scalar helpers first.【F:duckplus/typed/_generated_function_namespaces.py†L45616-L45646】

## Scalar function clustering

The scalar namespaces contain the largest surface area. Key clusters are:

- **Math and numeric core (≈30 wrappers)** – trigonometric, exponential, and rounding helpers such as `abs`, `acos`, `atan2`, `exp`, `round`, and `sqrt` live in `ScalarNumericFunctions`. These belong together in a `math.py` module when split.【F:duckplus/typed/_generated_function_namespaces.py†L26262-L26321】
- **Date/time (≈18 wrappers)** – functions including `date_part`, `date_trunc`, `epoch_ms`, and related aliases span numeric and generic namespaces. A `datetime.py` module can consolidate them while keeping overload-specific helpers in Python code.【F:duckplus/typed/_generated_function_namespaces.py†L21071-L21132】【F:duckplus/typed/_generated_function_namespaces.py†L28205-L28266】
- **String + ICU (≈150 wrappers)** – `ScalarVarcharFunctions` houses 132 ICU collation helpers (`icu_collate_*`) plus string manipulation utilities like `concat_ws`, `lower`, `trim`, and `regexp_replace`. These naturally split into `string/icu.py` for locale tables and `string/core.py` for everyday operations.【F:duckplus/typed/_generated_function_namespaces.py†L38789-L38802】【F:duckplus/typed/_generated_function_namespaces.py†L40284-L40332】
- **JSON (≈19 wrappers)** – helpers such as `json_extract`, `json_keys`, and `json_merge_patch` cluster tightly and should share a `json.py` module.【F:duckplus/typed/_generated_function_namespaces.py†L42199-L42258】【F:duckplus/typed/_generated_function_namespaces.py†L42469-L42529】
- **Collection helpers (≈60 wrappers)** – list and array utilities (`list_aggr`, `list_extract`, `array_concat`, `array_reduce`) live in the generic namespace today and merit `collections/list.py` and `collections/array.py` modules.【F:duckplus/typed/_generated_function_namespaces.py†L22022-L22082】【F:duckplus/typed/_generated_function_namespaces.py†L20294-L20356】
- **Map/struct helpers (≈15 wrappers)** – `map_concat`, `map_keys`, `struct_extract`, and siblings can group under `collections/map_struct.py` to keep nested-type helpers discoverable.【F:duckplus/typed/_generated_function_namespaces.py†L22469-L22536】 The aggregate ``map`` builder already lives in :mod:`duckplus.functions.aggregate.map`, which registers the decorator-backed helper onto the generic namespace and is exercised by the barrel and namespace tests.【F:duckplus/functions/aggregate/map.py†L1-L86】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:tests/test_typed_function_namespace.py†L200-L280】
- **Internal codecs** – low-level functions prefixed with `__internal_*` should stay tucked away in a private `_internal.py` module so the public API exports only ergonomic helpers.【F:duckplus/typed/_generated_function_namespaces.py†L25394-L25431】

## Aggregate function highlights

Aggregate namespaces skew toward statistical helpers:

- **Regression suite (18 wrappers)** – `regr_avgx`, `regr_count`, `regr_r2`, and the rest of the regression helpers now live in `aggregate/regression.py`, which decorates the numeric namespace directly and is exercised by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/regression.py†L1-L390】【F:tests/test_function_import_barrels.py†L1-L120】【F:tests/test_typed_function_namespace.py†L200-L320】
- **Quantile family (12 wrappers)** – `quantile`, `quantile_cont`, and `quantile_disc` (with filters) now live in `aggregate/quantiles.py`, registering the percentile helpers directly on the generic namespace with decorator-backed metadata validated by barrel and namespace tests.【F:duckplus/functions/aggregate/quantiles.py†L1-L223】【F:tests/test_function_import_barrels.py†L6-L120】【F:tests/test_typed_function_namespace.py†L212-L264】
- **Approximation + sketches (≈10 wrappers)** – helpers prefixed with `approx_` and `histogram` can share `aggregate/approximation.py` for streaming-friendly aggregates.【F:duckplus/typed/_generated_function_namespaces.py†L7068-L7188】
- **Arg min/max variants (≈24 wrappers)** – `arg_max`, `arg_min`, and typed overloads now consolidate under `aggregate/arg_extrema.py`, which registers blob, varchar, numeric, and generic namespaces directly with the decorator-backed helpers.【F:duckplus/functions/aggregate/arg_extrema.py†L1-L438】【F:tests/test_typed_function_namespace.py†L248-L296】
- **Central tendency (median/mode)** – `median`, `median_filter`, `mode`, and `mode_filter` now live in `aggregate/median.py` and `aggregate/mode.py`, exposing decorator-backed helpers that register onto the generic aggregate namespace during import.【F:duckplus/functions/aggregate/median.py†L1-L118】【F:duckplus/functions/aggregate/mode.py†L1-L132】
- **String concatenation** – `string_agg` and `string_agg_filter` now live in `aggregate/string.py`, registering onto the varchar aggregate namespace with decorator-backed helpers validated by updated barrel and namespace tests.【F:duckplus/functions/aggregate/string.py†L1-L136】【F:duckplus/functions/aggregate/__init__.py†L1-L120】【F:duckplus/functions/__init__.py†L5-L48】【F:tests/test_function_import_barrels.py†L6-L94】【F:tests/test_typed_function_namespace.py†L213-L235】
- **Bitstring aggregation** – `bitstring_agg` and `bitstring_agg_filter` now live in `aggregate/bitstring.py`, registering onto the generic aggregate namespace with decorator-backed helpers validated by barrel and namespace tests.【F:duckplus/functions/aggregate/bitstring.py†L1-L126】【F:duckplus/functions/aggregate/__init__.py†L1-L120】【F:duckplus/functions/__init__.py†L5-L48】【F:tests/test_function_import_barrels.py†L6-L94】【F:tests/test_typed_function_namespace.py†L201-L231】
- **Summation helpers** – `sum`, `sum_filter`, `product`, and `product_filter` now live in `aggregate/summation.py`, registering onto both the generic and numeric namespaces with decorator-backed helpers validated by barrel and namespace tests.【F:duckplus/functions/aggregate/summation.py†L1-L236】【F:duckplus/functions/aggregate/__init__.py†L1-L170】【F:duckplus/functions/__init__.py†L5-L60】【F:tests/test_function_import_barrels.py†L6-L120】【F:tests/test_typed_function_namespace.py†L200-L260】
- **Average helpers** – `avg`, `avg_filter`, `mean`, and `mean_filter` now live in `aggregate/averages.py`, registering onto the generic and numeric namespaces with decorator-backed docstrings validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/averages.py†L1-L184】【F:duckplus/functions/aggregate/__init__.py†L1-L200】【F:duckplus/functions/__init__.py†L5-L60】【F:tests/test_function_import_barrels.py†L6-L130】【F:tests/test_typed_function_namespace.py†L200-L260】
- **Extremum by value** – `max_by` and `min_by` (plus `FILTER` variants) now live in `aggregate/extremum_by_value.py`, registering across blob, varchar, numeric, and generic namespaces with decorator-backed overload metadata validated by barrel and namespace tests.【F:duckplus/functions/aggregate/extremum_by_value.py†L1-L272】【F:duckplus/functions/aggregate/__init__.py†L1-L160】【F:duckplus/functions/__init__.py†L5-L76】【F:tests/test_function_import_barrels.py†L1-L84】【F:tests/test_typed_function_namespace.py†L248-L356】
- **List aggregation** – `list` and `list_filter` now live in `aggregate/list.py`, registering onto the generic aggregate namespace while tests verify the barrels expose the decorator-backed helpers instead of generated wrappers.【F:duckplus/functions/aggregate/list.py†L1-L120】【F:duckplus/functions/aggregate/__init__.py†L1-L125】【F:duckplus/functions/__init__.py†L5-L44】【F:tests/test_function_import_barrels.py†L6-L110】【F:tests/test_typed_function_namespace.py†L175-L239】

## Recommended module boundaries

1. **`duckplus/functions/aggregate/`**
   - `regression.py` for regression and covariance helpers.
   - `quantiles.py` for percentile/median families.
   - `approximation.py` for `approx_count_distinct`, `histogram`, and sketch-style aggregates.
   - `arg_extrema.py` for `arg_{min,max}` overload suites shared across return categories.

2. **`duckplus/functions/scalar/`**
   - `math.py` for trigonometric, exponential, and numeric utilities.
   - `datetime.py` for date, time, and epoch conversions.
   - `string/core.py` for concatenation, substring, trimming, regex, and formatting helpers.
   - `string/icu.py` for locale-aware collation wrappers so heavy tables stay isolated.
   - `json.py` for JSON extraction and mutation functions.
   - `collections/array.py`, `collections/list.py`, and `collections/map_struct.py` for complex-type tooling.
   - `_internal.py` for `__internal_*` codecs and symbol helpers kept out of the public barrel.

3. **`duckplus/functions/window/`**
   - Reserve the namespace for future work; once DuckDB exposes window helpers through `duckdb_functions()`, mirror the aggregate splits to keep parity with `SCALAR_FUNCTIONS` and `AGGREGATE_FUNCTIONS`.

## Next steps for the migration

- Generate stubs for the proposed modules that import and re-export the existing helpers to verify counts before moving definitions.
- ``duckplus/functions/aggregate/approximation.py`` now hosts ``approx_count_distinct`` and ``approx_count_distinct_filter`` as direct Python implementations registered onto ``AggregateNumericFunctions``.
- Update tests in `tests/test_typed_function_namespace.py` to assert imports from the new modules while maintaining decorator registration guarantees.
- Adjust documentation (`docs/typed_api.md`, `docs/function_namespace_generator_retirement.md`) so contributors know where each helper family lives once modules start splitting.
