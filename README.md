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

