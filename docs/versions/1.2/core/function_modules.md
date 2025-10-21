# DuckDB function modules

DuckPlus 1.2 introduces domain-organised DuckDB function helpers so IDEs and
documentation can point at real Python modules instead of generated registries.
The helpers live under :mod:`duckplus.functions` and bind to the typed
expression namespaces at import time via decorator side effects.

## Importing the aggregate helpers

The approximation aggregates ship in
:mod:`duckplus.functions.aggregate.approximation`. Importing the package exposes
helper functions directly from Python modules while keeping the registration
side effects explicit:

```python
from duckplus import functions

# Side-effect modules register helpers onto the typed aggregate namespace.
functions.approx_count_distinct
functions.histogram_filter
```

The :mod:`duckplus.functions` barrel re-exports the helpers and publishes a
:pydataattr:`duckplus.functions.SIDE_EFFECT_MODULES` tuple so tests and tooling
can verify which modules must be imported to register additional helpers.
Downstream code can iterate over the tuple to ensure every decorator executes
before relying on the helpers.【F:duckplus/functions/__init__.py†L5-L40】

```python
import importlib
from duckplus import functions

for module_name in functions.SIDE_EFFECT_MODULES:
    importlib.import_module(module_name)
```

## Working within a domain package

Each domain package mirrors the public helpers it exposes. For aggregates, the
:mod:`duckplus.functions.aggregate` barrel imports the approximation module and
re-exports the helpers so callers can choose between
``duckplus.functions`` and ``duckplus.functions.aggregate`` imports while tests
still assert the side-effect modules execute. The tuple published as
:pydataattr:`duckplus.functions.aggregate.SIDE_EFFECT_MODULES` records every
module with registration side effects.【F:duckplus/functions/aggregate/__init__.py†L1-L40】

Ordering-sensitive aggregates such as :func:`duckplus.functions.aggregate.ordering.first`
follow the same pattern: the dedicated module defines the helpers with
decorators, the aggregate barrel imports the module for its registration side
effects, and the top-level :mod:`duckplus.functions` package re-exports the
helpers for convenience.【F:duckplus/functions/aggregate/ordering.py†L1-L116】【F:duckplus/functions/__init__.py†L5-L40】

Central tendency helpers such as
:func:`duckplus.functions.aggregate.mode.mode` and
:func:`duckplus.functions.aggregate.median.median` are implemented in their own
modules so documentation and introspection surface the decorator-backed helpers
rather than generated methods. The aggregate barrel loads both
:mod:`duckplus.functions.aggregate.mode` and
:mod:`duckplus.functions.aggregate.median` alongside the other side-effect
modules and re-exports :func:`duckplus.functions.mode` and
:func:`duckplus.functions.median` for callers who import from the package
root.【F:duckplus/functions/aggregate/mode.py†L1-L132】【F:duckplus/functions/aggregate/median.py†L1-L118】【F:duckplus/functions/aggregate/__init__.py†L1-L110】【F:duckplus/functions/__init__.py†L5-L52】

String concatenation helpers live in
:mod:`duckplus.functions.aggregate.string`, which registers
:func:`duckplus.functions.aggregate.string.string_agg` and its FILTER variant on
the varchar aggregate namespace while the barrels re-export
:func:`duckplus.functions.string_agg` for convenience. Tests assert the helper
docstrings and module provenance so IDEs surface the decorator-backed
implementations instead of generated wrappers.【F:duckplus/functions/aggregate/string.py†L1-L136】【F:duckplus/functions/aggregate/__init__.py†L1-L120】【F:duckplus/functions/__init__.py†L5-L48】【F:tests/test_function_import_barrels.py†L6-L94】

Bitstring aggregation helpers live in
:mod:`duckplus.functions.aggregate.bitstring`, which registers
:func:`duckplus.functions.aggregate.bitstring.bitstring_agg` and its FILTER variant on
the generic aggregate namespace while the barrels re-export
:func:`duckplus.functions.bitstring_agg`. Tests pin the new module inside the
barrel `SIDE_EFFECT_MODULES` tuple and confirm the typed namespace exposes the
decorator-backed docstrings so downstream tooling resolves the Python
implementations instead of generated wrappers.【F:duckplus/functions/aggregate/bitstring.py†L1-L126】【F:duckplus/functions/aggregate/__init__.py†L1-L120】【F:duckplus/functions/__init__.py†L5-L48】【F:tests/test_function_import_barrels.py†L6-L94】【F:tests/test_typed_function_namespace.py†L201-L231】

Summation helpers now live in :mod:`duckplus.functions.aggregate.summation`,
which defines :func:`duckplus.functions.aggregate.summation.sum` (and its FILTER
variant) for both generic and numeric namespaces alongside the
:func:`duckplus.functions.aggregate.summation.product` aggregate for numeric
expressions. The aggregate and top-level barrels import the module so the
helpers register at import time, and regression tests assert the side-effect
tuple lists the module while the typed namespaces expose the decorator-backed
docstrings.【F:duckplus/functions/aggregate/summation.py†L1-L236】【F:duckplus/functions/aggregate/__init__.py†L1-L170】【F:duckplus/functions/__init__.py†L5-L60】【F:tests/test_function_import_barrels.py†L6-L120】【F:tests/test_typed_function_namespace.py†L200-L260】

Regression and covariance helpers live in
:mod:`duckplus.functions.aggregate.regression`, which registers the
``covar_*`` and ``regr_*`` aggregates directly onto the numeric namespace with
decorator-backed metadata. The aggregate barrels import the module explicitly so
side-effect tests pin its presence, and typed-namespace checks validate the
docstrings for the regression helpers.【F:duckplus/functions/aggregate/regression.py†L1-L390】【F:duckplus/functions/aggregate/__init__.py†L1-L190】【F:duckplus/functions/__init__.py†L5-L92】【F:tests/test_function_import_barrels.py†L1-L120】【F:tests/test_typed_function_namespace.py†L200-L320】

Average helpers live in :mod:`duckplus.functions.aggregate.averages`, which
registers :func:`duckplus.functions.aggregate.averages.avg` and
:func:`duckplus.functions.aggregate.averages.mean` (plus their FILTER variants)
onto the generic and numeric aggregate namespaces. The aggregate barrel and
top-level :mod:`duckplus.functions` package import the module so the helpers
register at import time, while the regression tests assert the side-effect
tuple lists the new module and that the typed namespaces surface the
decorator-backed docstrings.【F:duckplus/functions/aggregate/averages.py†L1-L184】【F:duckplus/functions/aggregate/__init__.py†L1-L200】【F:duckplus/functions/__init__.py†L5-L60】【F:tests/test_function_import_barrels.py†L6-L140】【F:tests/test_typed_function_namespace.py†L200-L360】

Extremum-by-value helpers live in
:mod:`duckplus.functions.aggregate.extremum_by_value`, which registers
:func:`duckplus.functions.aggregate.extremum_by_value.max_by` and
:func:`duckplus.functions.aggregate.extremum_by_value.min_by` (plus their FILTER
variants) across blob, varchar, numeric, and generic namespaces. The aggregate
barrels import the module explicitly so side-effect coverage stays testable, and
regression tests assert that the decorator-backed docstrings—including the
``ANY[]`` overload metadata—originate from the Python implementation instead of
the generated namespace.【F:duckplus/functions/aggregate/extremum_by_value.py†L1-L272】【F:duckplus/functions/aggregate/__init__.py†L1-L160】【F:duckplus/functions/__init__.py†L5-L76】【F:tests/test_function_import_barrels.py†L1-L84】【F:tests/test_typed_function_namespace.py†L248-L356】

Basic extrema helpers live in :mod:`duckplus.functions.aggregate.extrema`,
which registers :func:`duckplus.functions.aggregate.extrema.max` and
:func:`duckplus.functions.aggregate.extrema.min` (plus FILTER variants) across
boolean, blob, varchar, numeric, and generic aggregate namespaces. The aggregate
and top-level barrels import the module for its registration side effects, and
tests assert both the barrel re-exports and the typed namespace provenance so
callers resolve the decorator-backed overload metadata instead of the generated
wrappers.【F:duckplus/functions/aggregate/extrema.py†L1-L302】【F:duckplus/functions/aggregate/__init__.py†L1-L140】【F:duckplus/functions/__init__.py†L5-L72】【F:tests/test_function_import_barrels.py†L1-L94】【F:tests/test_typed_function_namespace.py†L150-L360】

Quantile helpers live in :mod:`duckplus.functions.aggregate.quantiles`, which
registers :func:`duckplus.functions.aggregate.quantiles.quantile`,
:func:`duckplus.functions.aggregate.quantiles.quantile_disc`, and
:func:`duckplus.functions.aggregate.quantiles.quantile_cont` (plus FILTER
variants) onto the generic aggregate namespace. The aggregate barrels import the
module so percentile helpers register at import time, and regression tests pin
the decorator-backed docstrings to the Python implementation rather than the
generated namespace.【F:duckplus/functions/aggregate/quantiles.py†L1-L223】【F:duckplus/functions/aggregate/__init__.py†L1-L140】【F:tests/test_function_import_barrels.py†L6-L120】【F:tests/test_typed_function_namespace.py†L212-L264】

List aggregation helpers live in
:mod:`duckplus.functions.aggregate.list`, which registers
:func:`duckplus.functions.aggregate.list.list` and its FILTER variant onto the generic
aggregate namespace with the DuckDB overload metadata captured directly in Python
code. The aggregate barrels expose the helpers alongside the other side-effect
modules, and regression tests assert both the re-export provenance and the
decorator-backed docstrings so IDEs surface the new module.【F:duckplus/functions/aggregate/list.py†L1-L120】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:duckplus/functions/__init__.py†L5-L44】【F:tests/test_function_import_barrels.py†L6-L130】【F:tests/test_typed_function_namespace.py†L175-L245】

Map aggregation helpers live in :mod:`duckplus.functions.aggregate.map`, which registers
:func:`duckplus.functions.aggregate.map.map` onto the generic aggregate namespace with
the DuckDB overload metadata defined directly in Python. The aggregate barrel loads
the module for its registration side effects while the top-level
:mod:`duckplus.functions` package re-exports :func:`duckplus.functions.map` and tests
assert the helper’s provenance and docstring so downstream tooling resolves the
decorator-backed implementation.【F:duckplus/functions/aggregate/map.py†L1-L86】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:duckplus/functions/__init__.py†L5-L44】【F:tests/test_function_import_barrels.py†L6-L140】【F:tests/test_typed_function_namespace.py†L200-L256】

Arg-extrema helpers for blob, varchar, numeric, and generic expressions now live
in :mod:`duckplus.functions.aggregate.arg_extrema`. The module registers
``arg_max``/``arg_min`` (and their ``FILTER`` and ``_null`` variants) onto each
aggregate namespace while preserving DuckDB’s overload metadata—including the
``ANY[]`` signatures—in pure Python definitions validated by the namespace
tests.【F:duckplus/functions/aggregate/arg_extrema.py†L1-L438】【F:tests/test_typed_function_namespace.py†L248-L296】

The approximation module defines helpers with the
:func:`duckplus.functions._base.register_duckdb_function` decorator. Each helper
holds the DuckDB function signature, forwards through
:func:`duckplus.functions._base.invoke_duckdb_function`, and returns the typed
expression category expected by the aggregate namespace. All metadata lives in
Python code so docstrings and language servers mirror the shipped behaviour
without parsing generated registries.【F:duckplus/functions/aggregate/approximation.py†L1-L200】【F:duckplus/functions/_base.py†L1-L68】

## When to add new modules

Follow the approximation suite’s structure when migrating additional DuckDB
helpers. Group related functions into new modules (for example,
``duckplus.functions.aggregate.quantiles`` now consolidates percentile helpers)
and list them inside the package-level
``SIDE_EFFECT_MODULES`` tuple. This keeps imports explicit, makes side effects
discoverable for tests, and ensures future releases continue to rely on direct
Python definitions instead of runtime registries.
