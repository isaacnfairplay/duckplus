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
- **Mutation helpers** – `DuckTable` adds append, anti-join inserts, and continuous-ID ingestion without reaching for
  handwritten SQL.

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
from duckplus import DuckRel, DuckTable, connect

with connect() as conn:
    base = DuckRel(
        conn.raw.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'Alpha', 10),
                (2, 'Beta', 5),
                (3, 'Gamma', 8)
            ) AS t(id, name, score)
            """
        )
    )

    # `DuckRel` exposes immutable relational transformations.
    top_scores = (
        base
        .filter('"score" >= ?', 8)
        .project_columns("id", "name", "score")
        .order_by(score="desc")
    )

    table = top_scores.materialize().require_table()
    print(table.to_pylist())

    # Need to persist results? Promote the relation to a table wrapper and append safely.
    conn.raw.execute("CREATE TABLE scores(id INTEGER, name VARCHAR, score INTEGER)")
    table_wrapper = DuckTable(conn, "scores")
    table_wrapper.insert_antijoin(top_scores, keys=["id"])
```

### Join interface

Duck+ exposes two families of joins: *natural* helpers that line up shared
column names automatically, and explicit joins driven by structured
specifications. Natural joins can accept keyword aliases when the right-hand
column differs in name, while explicit joins use `JoinSpec` to describe the
relationship and optional predicates.

```python
from duckplus.core import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    JoinProjection,
    JoinSpec,
)

# Natural join on shared columns plus an alias:
orders_rel = DuckRel(conn.raw.sql("SELECT 1 AS order_id, 100 AS customer_ref"))
customers_rel = DuckRel(conn.raw.sql("SELECT 100 AS id, 'Alice' AS name"))
orders_with_customer = orders_rel.natural_inner(customers_rel, customer_ref="id")

# Explicit join with a predicate and suffix handling:
orders_dates = DuckRel(
    conn.raw.sql("SELECT 1 AS order_id, DATE '2024-01-01' AS order_date")
)
customers_profile = DuckRel(
    conn.raw.sql(
        "SELECT 1 AS id, DATE '2023-12-01' AS customer_since, 'gold' AS tier"
    )
)
spec = JoinSpec(
    equal_keys=[("order_id", "id")],
    predicates=[ColumnPredicate("order_date", ">=", "customer_since")],
)
joined = orders_dates.left_outer(
    customers_profile, spec, project=JoinProjection(allow_collisions=True)
)

# Time-aware joins use ASOF helpers:
trades_rel = DuckRel(conn.raw.sql("SELECT 'A' AS symbol, NOW() AS trade_ts"))
quotes_rel = DuckRel(conn.raw.sql("SELECT 'A' AS symbol, NOW() - INTERVAL '5 seconds' AS quote_ts"))
latest = trades_rel.natural_asof(
    quotes_rel, order=AsofOrder(left="trade_ts", right="quote_ts")
)
nearest = trades_rel.asof_join(
    quotes_rel,
    AsofSpec(
        equal_keys=[("symbol", "symbol")],
        order=AsofOrder(left="trade_ts", right="quote_ts"),
        direction="nearest",
        tolerance="5 seconds",
    ),
)
```

`DuckTable.insert_antijoin` and `DuckTable.insert_by_continuous_id` keep appends idempotent by filtering existing
rows before inserting.

## Project layout

```
src/duckplus/
  __init__.py      # public exports (`connect`, `DuckRel`, `DuckTable`, materialize helpers)
  connect.py       # connection context manager and facade
  core.py          # `DuckRel` immutable relational wrapper
  materialize.py   # materialization strategies for DuckRel
  table.py         # `DuckTable` mutation helpers
  util.py          # case-insensitive resolution and shared utilities
  py.typed         # marks the package as typed for downstream type-checkers

tests/
  test_connect.py
  test_core.py
  test_table.py
  test_util.py
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

