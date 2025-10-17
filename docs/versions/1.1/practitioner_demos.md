# Practitioner demos

Real-world workflows highlight how DuckPlus integrates with editors and data
science tooling. The demos double as regression tests: each notebook or module
is executed in CI so the snippets published here always match the checked-in
code.

```{toctree}
:maxdepth: 1

language_server_demo
pi_demo
sales_pipeline_demo
duckdb_demos/index
```

## Language server walkthrough

The :doc:`language_server_demo` notebook demonstrates how the typed expression DSL
pairs with Python language servers. Because helper signatures are explicit,
completions and hover tooltips surface parameter documentation without custom
stubs. Start the notebook in VS Code or PyCharm with the Python extension enabled
and observe how ``ducktype.Numeric`` autocompletes window helpers and docstrings
inline.

## Pi approximation notebook

The :doc:`pi_demo` notebook walks through computing π via Monte Carlo simulation,
showcasing how immutable relations stay composable even when the underlying
DuckDB queries become complex. The example doubles as an integration smoke test
for typed expressions and relation transforms. Open the notebook in Jupyter or
your preferred Markdown-aware editor to verify that sampling helpers behave
identically in interactive environments.

## Schema diff quickstart

Pair :func:`duckplus.schema.diff_relations` with an editor that supports
``pytest`` integration to build automated regression checks. The curated tests in
:mod:`tests.test_schema` provide a blueprint for validating pipelines in CI.

## Sales analytics pipeline

The :doc:`sales_pipeline_demo` walkthrough composes relations, typed expressions,
and aggregation helpers to produce leadership-ready metrics. The documentation
captures the deterministic region and channel summaries along with the generated
SELECT statement so the demo's assertions and prose stay in sync. Execute
``python -m duckplus.examples.sales_pipeline`` to regenerate the artefacts locally
or import ``SalesDemoReport`` in a notebook to explore the underlying relations.

## DuckDB demo parity guides

The :doc:`duckdb_demos/index` series mirrors DuckDB's public demos with
DuckPlus-specific snippets. Use it as a checklist when migrating from raw
DuckDB code: each section highlights the corresponding helper, required
extensions, and typed-expression counterparts.

## Contributing demos

Have a workflow that showcases DuckPlus in production? Add a notebook or Markdown
tutorial under {file}`docs/versions/1.1/practitioner_demos.md` and link to it in
this section. Focus on readability—explain the problem, highlight how DuckPlus
simplifies it, provide a runnable example, and list the commands required to run
tests locally. Demos that exercise extensions should mention the necessary
``extra_extensions`` tuple so readers can reproduce the environment quickly.
