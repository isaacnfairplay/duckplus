# Duck+ (`duckplus`)

Pythonic, typed helpers that make [DuckDB](https://duckdb.org/) feel at home in larger Python projects. Duck+
wraps DuckDB relations and tables so you can apply familiar, chainable transformations without losing sight of the
underlying SQL. The package favors explicit projections, predictable casing, and deterministic joins—perfect for
analytics pipelines that need safety as much as speed.

## Why Duck+

- **Typed relational wrappers** – `DuckRel` keeps transformations immutable, while `DuckTable` encapsulates mutating
  table operations such as appends and insert strategies.
- **Connection management** – `duckplus.connect()` is a context manager that yields a light connection facade and
  loads optional DuckDB extensions (e.g., `secrets`) only when they are available.
- **Opinionated defaults** – joins project columns explicitly, drop duplicate right-side keys, and error on naming
  collisions unless you explicitly opt into suffixes mirroring DuckDB (`_1`, `_2`).
- **Case-aware column handling** – columns preserve their original case while still supporting case-insensitive
  lookup helpers (`columns_lower`, `columns_lower_set`).
- **Batteries-included I/O** – helpers in `duckplus.io` read Parquet, CSV, and JSON inputs from paths or sequences of
  paths, and provide appenders for CSV and NDJSON outputs with sensible defaults (UTF-8, headers on, temp-then-rename).

## Installation

Duck+ targets Python 3.12+ and DuckDB 1.3.0 or newer.

```bash
uv pip install duckplus
```

For local development, use the provided `uv` configuration:

```bash
uv sync
```

This will create and manage the virtual environment with development dependencies (pytest, mypy, and friends).

## Quickstart

```python
from pathlib import Path

from duckplus import connect
from duckplus.io import read_parquet

with connect() as conn:
    orders = read_parquet(conn, Path("data/orders.parquet"))

    # `DuckRel` exposes relational transformations without mutating the source.
    totals = (
        orders
        .select("customer_id", "order_total")
        .group_by("customer_id")
        .aggregate(total_amount=("sum", "order_total"))
    )

    # Convert the relation to a pandas DataFrame (or Arrow table) when you need to materialize it.
    df = totals.to_df()
    print(df.head())
```

Need to persist results? Promote a relation to a mutable table and append safely:

```python
from duckplus.core import DuckTable

with connect() as conn:
    DuckTable.create(conn, "analytics.daily_totals", schema=totals)
    DuckTable.append(conn, totals, by_name=True)
```

The insert helpers `insert_antijoin` and `insert_by_continuous_id` ensure that appends remain idempotent by checking
keys or monotonically increasing identifiers before inserting.

## Project layout

```
src/duckplus/
  __init__.py      # public exports (`connect`, `DuckRel`, `DuckTable`, io helpers)
  connect.py       # connection context manager and facade
  core.py          # `DuckRel` (immutable) and `DuckTable` (mutating) implementations
  io.py            # read_* / write_* helpers and appenders
  util.py          # case-insensitive resolution, path helpers, shared utilities
  py.typed         # marks the package as typed for downstream type-checkers

tests/
  test_columns.py
  test_select_and_rename.py
  test_join_projection.py
  test_insert_strategies.py
```

Tests exercise the guarantees Duck+ makes around casing, projections, joins, and insert safety. If you add new
behavior, be sure to add or update unit tests alongside it.

## Testing & quality checks

Run the test suite and strict type checks via `uv`:

```bash
uv run pytest
uv run mypy src/duckplus
uvx ty src/duckplus
```

All three commands are expected to pass before opening a pull request.

## Design principles

Duck+ enforces a few rules so analytical pipelines stay predictable:

1. **Immutability by default** – relations never mutate; materialization happens on demand at the edges.
2. **Explicit projections** – no relying on DuckDB defaults to pick or order columns for you.
3. **Strict missing-column behavior** – operations raise when referenced columns are absent unless you explicitly opt
   into lenient resolution.
4. **Safe mutation APIs** – insert helpers avoid duplicate data, respect column names, and support continuous ID
   workflows.
5. **Offline-first** – the core package is non-interactive and avoids network prompts; optional extras live in separate
   modules.

These principles keep Duck+ small, composable, and production-friendly.

## License

Duck+ is available under the [MIT License](LICENSE).

