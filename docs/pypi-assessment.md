# PyPI Distribution Assessment (duckplus 1.4.3)

## Overview
We installed `duckplus==1.4.3` from PyPI and exercised its public API inside an
isolated subprocess so the packaged code would not interfere with the repository
modules. The probe covers connection helpers, typed expressions, domain
functions, and schema utilities, and returns a JSON summary that the regression
suite asserts on.【F:tests/test_pypi_distribution.py†L22-L331】

## Confirmed behaviour
- `DuckCon` from the wheel exposes the documented file readers and keeps the
  connection lifecycle healthy. We successfully loaded a CSV file, added a typed
  column, aggregated it via `Numeric.Aggregate.sum`, and observed the expected
  total of `12` while the context manager closed the connection on exit.【F:tests/test_pypi_distribution.py†L55-L229】
- The packaged `DuckCon` class still provides the documented helper methods
  (`read_csv`, `read_parquet`, `read_json`, `read_excel`, `read_odbc_query`,
  and `read_odbc_table`) with callable implementations.【F:tests/test_pypi_distribution.py†L33-L114】【F:tests/test_pypi_distribution.py†L240-L273】
- All 47 scalar macros described in the documentation load from
  `duckplus.functions.scalar`, and the typed namespaces expose the same
  helpers across the varchar, boolean, and generic categories. Patch release
  1.4.3 also makes the applicable macros available as fluent expression methods
  (for example, `ducktype.Varchar("label").split_part(" ", 1)`), matching the
  repository documentation.【F:tests/test_pypi_distribution.py†L36-L157】【F:tests/test_pypi_distribution.py†L243-L273】
- `duckplus.schema.diff_relations` still detects type drift between relations,
  reporting the baseline `VARCHAR` vs. candidate `INTEGER` mismatch without
  raising an exception.【F:tests/test_pypi_distribution.py†L120-L177】

## Divergences from the docs
- Boolean expressions exposed by `ducktype.Boolean` in the wheel do **not**
  provide a `.coalesce()` helper, so the probe synthesises the documented
  behaviour with an explicit `CASE` expression instead.【F:tests/test_pypi_distribution.py†L49-L68】
- The approximation helpers advertised under `duckplus.functions` are missing
  from the numeric aggregate namespace; attempting to call the exported
  `approx_quantile_numeric` raises `AttributeError("'str' object has no
  attribute 'return_category'")`, confirming the decorator did not register a
  usable wrapper.【F:tests/test_pypi_distribution.py†L87-L170】
- Importing `duckplus.typed` from the wheel still emits the expected
  deprecation warning, but it does not return the exact same factory objects as
  `duckplus.static_typed`, so consumers relying on object identity or module
  paths may observe the discrepancy.【F:tests/test_pypi_distribution.py†L110-L172】

## Git installation probe
Installing directly from the Git repository mirrors the same helper surface as
the PyPI wheel and passes a fresh mypy run after installation. The regression
suite provisions a temporary virtual environment, installs
`pip install git+https://github.com/isaacnfairplay/duckplus.git`, and asserts
that:

- `DuckCon` helpers remain callable, aggregate a small dataset, and close the
  managed connection after exiting the context.【F:tests/test_git_installation.py†L45-L102】
- Boolean typed expressions match the repository's `.coalesce()` availability,
  so git installs track the same ergonomics contributors exercise locally.【F:tests/test_git_installation.py†L12-L102】
- Running `python -m mypy -p duckplus` inside the freshly installed
  environment reports success, confirming the packaged stubs remain
  PEP 561-compliant.【F:tests/test_git_installation.py†L104-L113】

