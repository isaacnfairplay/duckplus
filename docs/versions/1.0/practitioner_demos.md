# Practitioner demos

Real-world workflows highlight how DuckPlus integrates with editors and data
science tooling. The demos double as regression tests: each notebook or module
is executed in CI so the snippets published here always match the checked-in
code.

```{toctree}
:maxdepth: 1
:hidden:

sales_pipeline_demo
```

## Language server walkthrough

The {file}`docs/language_server_demo.md` notebook demonstrates how the typed
expression DSL pairs with Python language servers. Because helper signatures are
explicit, completions and hover tooltips surface parameter documentation without
custom stubs. Start the notebook in VS Code or PyCharm with the Python extension
enabled and observe how ``ducktype.Numeric`` autocompletes window helpers and
docstrings inline.

## Pi approximation notebook

The {file}`docs/pi_demo.md` notebook walks through computing π via Monte Carlo
simulation, showcasing how immutable relations stay composable even when the
underlying DuckDB queries become complex. The example doubles as an integration
smoke test for typed expressions and relation transforms. Open the notebook in
Jupyter or your preferred Markdown-aware editor to verify that sampling helpers
behave identically in interactive environments.

## Schema diff quickstart

Pair :func:`duckplus.schema.diff_relations` with an editor that supports
``pytest`` integration to build automated regression checks. The curated tests in
:mod:`tests.test_schema` provide a blueprint for validating pipelines in CI.

## Sales analytics pipeline

The :doc:`sales_pipeline_demo` walkthrough composes relations, typed
expressions, and aggregation helpers to produce leadership-ready metrics. The
documentation captures the deterministic region and channel summaries along
with the generated SELECT statement so the demo's assertions and prose stay in
sync. Execute ``python -m duckplus.examples.sales_pipeline`` to regenerate the
artefacts locally or import ``SalesDemoReport`` in a notebook to explore the
underlying relations.

## Contributing demos

Have a workflow that showcases DuckPlus in production? Add a notebook or Markdown
tutorial under {file}`docs/versions/1.0/practitioner_demos.md` and link to it in
this section. Focus on readability—explain the problem, highlight how DuckPlus
simplifies it, provide a runnable example, and list the commands required to run
tests locally. Demos that exercise extensions should mention the necessary
``extra_extensions`` tuple so readers can reproduce the environment quickly.
