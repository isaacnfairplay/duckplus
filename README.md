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
- **Secrets-ready** – `SecretManager` fronts a connection-independent registry and
  synchronizes entries into DuckDB when the optional `secrets` extension is
  installed, so pipelines can avoid embedding passwords in code or config files.
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

## Continuous integration

Every push to `main` and pull request targeting `main` runs the [CI workflow](.github/workflows/ci.yml). The job
provisions Python 3.12 and 3.13 via `astral-sh/setup-uv`, installs project dependencies with `uv sync`, and then runs
three gates:

- `uv run pytest` for the test suite
- `uv run mypy src/duckplus` for strict type checking
- `uvx ty check src/duckplus` for the Rust-based static analysis helper

All three steps must succeed for the workflow to pass, matching the local developer commands.

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

## Demo gallery

Looking for end-to-end walkthroughs? Explore the scenario-driven
[Duck+ demo gallery](docs/demo_gallery.rst) for detailed examples covering
joins, ingestion patterns, IO helpers, secrets management, materialization
strategies, and the CLI.

## Publishing to PyPI

Ready to cut a release? Follow the [publishing checklist](docs/publishing.md)
for the commands to run and the `uv` workflow to ship Duck+ to PyPI safely.

### Join interface

Duck+ exposes two families of joins: *natural* helpers that line up shared
column names automatically, and explicit joins driven by structured
specifications. Natural joins can accept keyword aliases when the right-hand
column differs in name, while explicit joins use `JoinSpec` to describe the
relationship and optional predicates.

Need to understand how a potential join will behave before wiring it into a
pipeline? `DuckRel.inspect_partitions()` reports how many rows fall into each
partition so you can gauge whether a partitioned join will rein in explosive
combinations.

```python
from duckplus import DuckRel
from duckplus.core import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    JoinProjection,
    JoinSpec,
    PartitionSpec,
)

# Natural join on shared columns plus an alias:
orders_rel = DuckRel(conn.raw.sql("SELECT 1 AS order_id, 100 AS customer_ref"))
customers_rel = DuckRel(conn.raw.sql("SELECT 100 AS id, 'Alice' AS name"))
orders_with_customer = orders_rel.natural_inner(customers_rel, customer_ref="id")

# Review partition sizes before adopting a partitioned join.
partition_review = orders_rel.inspect_partitions(
    customers_rel, PartitionSpec.from_mapping({"customer_ref": "id"})
)
print(partition_review.materialize().require_table().to_pylist())

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

Partition-aware joins pair a `PartitionSpec` with the explicit join family so
you can confine lookups to coarse keys (for example, partitioning by symbol
before applying a range predicate on timestamps) without exposing the
fine-grained join identifiers during inspection.

`DuckTable.insert_antijoin` and `DuckTable.insert_by_continuous_id` keep appends idempotent by filtering existing
rows before inserting.

## Command line interface

Duck+ ships with a small, read-only CLI for quick inspection of databases or ad-hoc queries. Install the package and
invoke the `duckplus` script (or run it via `uv run`) to execute SQL, inspect schemas, or drop into a REPL without
leaving the terminal.

```bash
# Execute a SQL query and show up to 20 rows by default
uv run duckplus sql "SELECT 42 AS answer"

# Display column names and DuckDB types inferred from a query
uv run duckplus schema "SELECT 1 AS id, 'alpha' AS label"

# Start an interactive, read-only REPL
uv run duckplus --repl
```

All commands operate against an in-memory DuckDB database unless you supply `--database /path/to/file.duckdb`. File-backed
connections are opened in read-only mode so ad-hoc exploration stays safe, while the in-memory default remains isolated to
the current process.

## HTML previews

For quick notebook previews Duck+ includes a lightweight HTML renderer that works entirely inside DuckDB. The helper limits
rows, escapes every value, and annotates the output when additional records were truncated.

```python
from duckplus import DuckRel, connect, to_html

with connect() as conn:
    rel = DuckRel(conn.raw.sql("SELECT 1 AS id, 'Alice & Bob' AS name UNION ALL SELECT 2, NULL"))

html = to_html(rel, max_rows=10, null_display="∅", class_="preview")
```

By default NULL values render as a blank cell, but you can supply `null_display` to use an explicit marker. Optional `class_`
and `id` keyword arguments attach CSS hooks without embedding styles directly in the markup.

## Project layout

```
src/duckplus/
  cli.py          # read-only command line interface (extras module)
  html.py         # HTML rendering helper (extras module)
  __init__.py      # public exports (`connect`, `DuckRel`, `DuckTable`, materialize helpers)
  connect.py       # connection context manager and facade
  secrets.py       # credential registry with DuckDB sync hooks
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

