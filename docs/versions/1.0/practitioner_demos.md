# Practitioner demos

Real-world workflows highlight how DuckPlus integrates with editors and data
science tooling.

## Language server walkthrough

The {file}`docs/language_server_demo.md` notebook demonstrates how the typed
expression DSL pairs with Python language servers. Because helper signatures are
explicit, completions and hover tooltips surface parameter documentation without
custom stubs.

## Pi approximation notebook

The {file}`docs/pi_demo.md` notebook walks through computing π via Monte Carlo
simulation, showcasing how immutable relations stay composable even when the
underlying DuckDB queries become complex. The example doubles as an integration
smoke test for typed expressions and relation transforms.

## Schema diff quickstart

Pair :meth:`Relation.schema_diff <duckplus.relation.Relation.schema_diff>` with
an editor that supports ``pytest`` integration to build automated regression
checks. The curated tests in :mod:`tests.test_schema` provide a blueprint for
validating pipelines in CI.

## Contributing demos

Have a workflow that showcases DuckPlus in production? Add a notebook or Markdown
tutorial under {file}`docs/versions/1.0/practitioner_demos.md` and link to it in
this section. Focus on readability—explain the problem, highlight how DuckPlus
simplifies it, and provide a runnable example.
