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
from duckplus import Relation, connect

with connect() as conn:
    rel = Relation(
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

    columns = rel.columns

    top_scores = (
        rel
        .where(columns.score >= 8)
        .select({"id": columns.id, "name": columns.name, "score": columns.score})
        .order_by(columns.score.desc())
    )

    print(top_scores.materialize().require_table().to_pylist())
```

This snippet opens an in-memory DuckDB connection, projects typed column
expressions through ``Relation.columns``, and materializes the chained result
safely.

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

### Compose with column expressions

The :func:`duckplus.col` helper returns a ``ColumnExpression`` that validates
column references before rendering SQL. Provide an optional
``duck_type=duckplus.ducktypes.*`` marker to opt into static type hints and
runtime validation for aggregates and ordering. Use the helper to build
filters, projections, aggregates, and ordering clauses without hand-writing
identifiers:

```python
from duckplus import AggregateExpression, col, ducktypes

team = col("team", duck_type=ducktypes.Varchar)
score = col("score", duck_type=ducktypes.Integer)

ranked = (
    rel
    .filter(score > 0)
    .add_columns(double_score=score)
    .aggregate(
        team,
        total_score=AggregateExpression.sum(score),
        peak_score=AggregateExpression.max(score),
    )
    .order_by((col("total_score", duck_type=ducktypes.Integer), "desc"))
)
```

Typed column expressions propagate through the pipeline. Every relation exposes a
schema snapshot via :attr:`duckplus.DuckRel.schema`, which returns a
:class:`duckplus.DuckSchema` containing canonical column definitions, DuckDB
markers, and cached Python annotations. Iterate over the schema when you need
structured metadata, or fall back to ``column_type_markers`` for quick summaries:

```python
typed = rel.project({"team": team, "score": score})
for definition in typed.schema.definitions:
    print(definition.name, definition.duck_type.describe(), definition.python_type)
# team VARCHAR str
# score INTEGER int

print([marker.describe() for marker in typed.column_type_markers])
# ['VARCHAR', 'INTEGER']
```

Declaring an incompatible type raises a helpful error before execution. For
example, projecting ``col("score", duck_type=ducktypes.Varchar)`` on a numeric
column fails fast with ``Projection column 'score' is typed as INTEGER but was
declared as VARCHAR``.

Typed metadata also powers ``DuckRel.fetch_typed()``, which turns the current
relation into Python tuples using the declared :mod:`duckplus.ducktypes` markers
as type hints. The method always returns every projected column so you can rely
on the stored schema. Columns without explicit typing fall back to ``Any``;
Python hints are derived from the declared DuckDB markers so there's a single
source of truth for both runtime validation and static analysis:

```python
rows = typed.fetch_typed()
# list[tuple[str, int]]

totals = (
    typed
    .aggregate(team, total=AggregateExpression.sum(score))
    .order_by((team, "asc"))
)
print(totals.fetch_typed())
# [('alpha', 200), ('beta', 80)]
```

### Explore typed pipelines

End-to-end demos that exercise typed projections, aggregates, and fetches live
in :mod:`duckplus.examples.typed_pipeline_demos`. They build a small in-memory
orders dataset, carry DuckDB markers through transformations, and surface the
results with ``fetch_typed()`` so consumers receive precise Python annotations.

```python
from duckplus import connect
from duckplus.examples import typed_pipeline_demos

with connect() as conn:
    orders = typed_pipeline_demos.typed_orders_demo_relation(conn)

    print(typed_pipeline_demos.priority_order_snapshot(orders))
    # [(1, 'north', 'Alice', 120, 5, datetime.date(2024, 1, 1), True), ...]

    print(typed_pipeline_demos.schema_driven_projection(orders))
    # [(1, 'north', True), (2, 'north', False), ...]

    print(typed_pipeline_demos.regional_revenue_summary(orders))
    # [('east', 1, 15), ('north', 3, 365), ('south', 1, 98), ('west', 1, 155)]

    print(typed_pipeline_demos.priority_region_rollup(orders))
    # [('east', 0, 0, 1), ('north', 2, 1, 10), ...]

    print(typed_pipeline_demos.customer_priority_profile(orders))
    # [('Alice', datetime.date(2024, 1, 1), 218, 2), ('Bob', datetime.date(2024, 1, 2), 45, 0), ...]

    print(typed_pipeline_demos.regional_customer_diversity(orders))
    # [('east', 1, 0), ('north', 3, 2), ('south', 1, 1), ('west', 1, 1)]

    print(typed_pipeline_demos.daily_priority_summary(orders))
    # [(datetime.date(2024, 1, 1), 120, 1), (datetime.date(2024, 1, 2), 45, 0), ...]

    print(typed_pipeline_demos.describe_schema(orders)[0])
    # {'name': 'order_id', 'duckdb_type': 'INTEGER', 'marker': 'INTEGER', 'python': 'int'}

    taxed = typed_pipeline_demos.apply_manual_tax_projection(orders)
    print(typed_pipeline_demos.describe_markers(taxed))
    # ['INTEGER', 'VARCHAR', 'VARCHAR', 'UNKNOWN', 'INTEGER', 'DATE', 'BOOLEAN']
```

The helper functions double as living documentation—the automated tests execute
them to ensure guides stay accurate as the API evolves.

### Release 0.0.11 typed schema workflows

Release ``0.0.11`` finalises the schema-first typed API overhaul. Every
``DuckRel`` keeps a :class:`duckplus.DuckSchema` instance so column metadata is
centralised, case-insensitive, and available for both runtime validation and
static typing. The new demos lean on that shared schema to keep projections and
aggregations tight while preserving ``DuckType`` markers and stored Python
annotations.

Highlights include:

* ``DuckRel.schema`` for direct access to column definitions, including helper
  methods such as :meth:`duckplus.DuckSchema.column` and
  :meth:`duckplus.DuckSchema.select`.
* Schema-aware demos like ``priority_region_rollup`` and
  ``schema_driven_projection`` that fetch typed metrics without re-declaring
  markers.
* ``describe_schema`` and ``describe_markers`` utilities that turn the schema
  cache into human-readable reports for docs, notebooks, or assertions.

```python
from duckplus.examples import typed_pipeline_demos

orders = typed_pipeline_demos.typed_orders_demo_relation(connection)
schema_report = typed_pipeline_demos.describe_schema(orders)

first_column = orders.schema.column("order_id")
assert first_column.python_type is int

print(typed_pipeline_demos.priority_region_rollup(orders))
# [('east', 0, 0, 1), ('north', 2, 2, 9), ...]
```

### Release 0.0.7 reliability demos

Release ``0.0.7`` introduced :mod:`duckplus.examples.reliability_demos`, a
collection of production-grade helpers that demonstrate how to combine typed
column dictionaries, ``DuckTable`` idempotent writes, and Arrow materialisation
for resilient pipelines:

* ``priority_dispatch_payload`` builds an ordered alert payload with explicit
  revenue thresholds.
* ``incremental_fact_ingest`` shows how to hydrate a fact table while inserting
  only unseen keys.
* ``customer_spike_detector`` and ``regional_order_kpis`` provide ready-made
  guardrails that leverage aggregate filters without losing type metadata.
* ``arrow_priority_snapshot`` materializes a cache-friendly Arrow table for fast
  fan-out.
* ``lean_projection_shortcut`` keeps curated projections short by reusing typed
  expressions even after string-based transforms.

Each helper returns plain Python structures so orchestration layers can assert
on counts, payloads, or schema drift with zero additional wiring.

### Aggregate with ``AggregateExpression``

```python
import duckdb
from duckplus import AggregateExpression, DuckRel, col

with duckdb.connect() as conn:
    sales = DuckRel(
        conn.sql(
            """
            SELECT *
            FROM (VALUES
                ('north', 50, DATE '2024-01-03'),
                ('north', 60, DATE '2024-01-02'),
                ('south', 30, DATE '2024-01-01'),
                ('east', 20, DATE '2024-01-04'),
                ('west', 70, DATE '2024-01-05')
            ) AS t(region, amount, sale_date)
            """
        )
    )

    rollup = (
        sales.aggregate(
            col("region"),
            total_amount=AggregateExpression.sum(col("amount")),
            non_north=AggregateExpression.sum(col("amount")).with_filter(col("region") != "north"),
            first_sale_amount=(
                AggregateExpression.function("first", col("amount")).with_order_by((col("sale_date"), "asc"))
            ),
        )
        .order_by((col("region"), "asc"))
    )

    print(rollup.relation.fetchall())
```

This produces alphabetized totals, a filtered sum, and the first sale per region
without hand-writing aggregate SQL. See ``docs/source/aggregate_demos.rst`` for a
tested, larger set of aggregate examples.

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
from duckplus import JoinProjection, JoinSpec, column

spec = JoinSpec(equal_keys=[("order_id", "id")])

projection = JoinProjection(allow_collisions=False)
joined = orders.natural_join(customers, project=projection)

# Add additional join predicates with column comparisons when needed.
currency_safe = orders.left_outer(
    customers,
    JoinSpec(
        equal_keys=[("order_id", "id")],
        predicates=[column("order_date") >= column("customer_since")],
    ),
    project=projection,
)

suffixes = JoinProjection(allow_collisions=True)
safe = orders.left_outer(customers, spec, project=suffixes)
```

Join helpers project columns explicitly, drop duplicate right-side keys, and
raise when collisions would occur. Opt into suffixes through
`JoinProjection(allow_collisions=True)` when needed, and use `column()` to
declare predicates that compare two columns without writing raw SQL.

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
Sphinx project, and uploads the generated HTML as a GitHub Pages artifact. Pages
serves the most recent deployment at
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
