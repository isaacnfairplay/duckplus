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
``duckplus.functions.aggregate.quantiles`` or
``duckplus.functions.scalar.string``) and list them inside the package-level
``SIDE_EFFECT_MODULES`` tuple. This keeps imports explicit, makes side effects
discoverable for tests, and ensures future releases continue to rely on direct
Python definitions instead of runtime registries.
