# Duck+ (`duckplus`)

Duck+ is a user-friendly companion to [DuckDB](https://duckdb.org/) for Python
projects that want typed helpers, predictable joins, and safe table operations.
It wraps DuckDB relations so you can compose analytics pipelines with readable
Python while still generating explicit SQL under the hood.

---

## What you get

- **Typed relational wrappers** – `DuckRel` keeps transformations immutable and
  chainable.
- **Safe table workflows** – `DuckTable` owns inserts, appends, and
  idempotent ingestion strategies.
- **Explicit joins and casing rules** – column names stay intact, projections
  are deliberate, and collisions fail loudly unless you opt in to suffixes.
- **Optional helpers** – secrets management, a read-only CLI, and HTML previews
  stay in extras so the core package remains lightweight.

---

## Install in seconds

Duck+ targets Python 3.12+ and DuckDB 1.3.0 or newer.

```bash
uv pip install duckplus
```

For development, clone the repository and run `uv sync` to create the managed
environment with test and typing dependencies. Build the documentation locally
with `uv run sphinx-build -b html docs/source docs/_build/html`, then open
`docs/_build/html/index.html` in your browser to preview the site.

---

## Quickstart

```python
from duckplus import DuckRel, connect

with connect() as conn:
    rel = DuckRel(
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

    top_scores = (
        rel
        .filter('"score" >= ?', 8)
        .project_columns("id", "name", "score")
        .order_by(score="desc")
    )

    print(top_scores.materialize().require_table().to_pylist())
```

This snippet opens an in-memory DuckDB connection, builds a relation, filters
rows with positional parameters, and materializes results safely.

---

## Core workflows

### Connect and manage context

```python
from duckplus import connect

with connect(path="analytics.duckdb") as conn:
    rel = conn.relation("SELECT 42 AS answer")
    print(rel.to_df())
```

Connections default to in-memory databases. Pass `path` for file-backed
workloads; Duck+ keeps them read-only by default.

### Transform relations with `DuckRel`

```python
deduped = (
    rel
    .distinct()
    .project({"score": "AVG(score)"}, group_by=["name"])
    .order_by(score="desc")
)
```

DuckRel methods always return new relations and validate column names with
case-aware lookups.

### Promote to tables with `DuckTable`

```python
materialized = deduped.materialize().require_table()
table = materialized.to_table("scores")
table.insert_antijoin(deduped, keys=["name"])
```

Table wrappers provide append/insert helpers that guard against duplicates and
respect column names.

### Join with confidence

```python
from duckplus import JoinProjection, JoinSpec

spec = JoinSpec(equal_keys=[("order_id", "id")])

projection = JoinProjection(allow_collisions=False)
joined = orders.natural_join(customers, project=projection)

suffixes = JoinProjection(allow_collisions=True)
safe = orders.left_outer(customers, spec, project=suffixes)
```

Join helpers project columns explicitly, drop duplicate right-side keys, and
raise when collisions would occur. Opt into suffixes through
`JoinProjection(allow_collisions=True)` when needed.

---

## Extras worth knowing

### DataFrame interop

Install optional extras when you want pandas or Polars integration:

```bash
uv pip install "duckplus[pandas]"      # pandas DataFrame support
uv pip install "duckplus[polars]"      # Polars DataFrame support
```

Once installed, relations expose familiar helpers:

```python
df = rel.df()            # pandas.DataFrame
pl_frame = rel.pl()      # polars.DataFrame

from duckplus import DuckRel
rel_from_df = DuckRel.from_pandas(df)
rel_from_pl = DuckRel.from_polars(pl_frame)
```

Attempting to call these helpers without the matching extra raises a clear
``ModuleNotFoundError`` explaining how to install the dependency.

### Command line interface

```bash
uv run duckplus sql "SELECT 42 AS answer"
uv run duckplus schema "SELECT 1 AS id, 'alpha' AS label"
uv run duckplus --repl
```

The CLI provides read-only helpers for quick exploration. Point it at a DuckDB
file with `--database path/to/file.duckdb` when needed.

### HTML previews

```python
from duckplus import DuckRel, connect, to_html

with connect() as conn:
    rel = DuckRel(conn.raw.sql("SELECT 1 AS id, 'Alice & Bob' AS name"))

html = to_html(rel, max_rows=10, null_display="∅", class_="preview")
```

`to_html` renders safe, escaped previews with optional styling hooks.

---

## Documentation workflow

The documentation site is published automatically to GitHub Pages by the
[`Docs`](https://github.com/isaacnfairplay/duck/actions/workflows/docs.yml)
workflow. Every push to `main` and each pull request runs `uv sync`, builds the
Sphinx project, and deploys the generated HTML to the `gh-pages` branch. The
latest deployment is always available at
[https://isaacnfairplay.github.io/duck/](https://isaacnfairplay.github.io/duck/),
and workflow summaries include preview links you can share for review.

If a deployment fails:

1. Open the **Actions → Docs** run for the failing commit or pull request.
2. Review the build logs, especially the `uv run sphinx-build` step for Sphinx
   warnings promoted to errors.
3. Re-run the job from the Actions UI after fixing the problem to publish an
   updated preview.

For a local preview outside CI, run:

```bash
uv sync
uv run sphinx-build -b html docs/source docs/_build/html
python -m webbrowser docs/_build/html/index.html  # optional helper to open the preview
```

---

## Learn more

- Review the [API reference](https://isaacnfairplay.github.io/duck/api_reference.html) for detailed method docs and
  typing information.
- Explore unit tests under `tests/` to see edge cases and best practices.

If you run into questions or want to suggest improvements, open an issue or
pull request. We welcome contributions that keep Duck+ reliable for the long
haul.

---

## License

Duck+ is available under the [MIT License](LICENSE).
