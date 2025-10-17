# Schema management

DuckPlus keeps schema validation front and centre so data pipelines remain
predictable. Schemas are derived lazily from DuckDB itself, ensuring any new
types introduced upstream are reflected automatically in your metadata.

## Inspecting schemas

DuckPlus surfaces schema metadata through the :mod:`duckplus.schema` module. At
its simplest, you can inspect :attr:`Relation.columns
<duckplus.relation.Relation.columns>` and :attr:`Relation.types
<duckplus.relation.Relation.types>` directly on immutable relations. For richer
reports, call :func:`duckplus.schema.diff_relations` or
:func:`duckplus.schema.diff_files` to produce dataclasses that describe the
differences between two sources.

```python
columns = dict(zip(relation.columns, relation.types, strict=False))
for name, duck_type in columns.items():
    print(name, duck_type)
```

## Diffing relations

:func:`duckplus.schema.diff_relations` compares two relations and reports column
additions, removals, and type changes. The resulting
:class:`duckplus.schema.SchemaDiff` exposes ``missing_from_candidate``,
``unexpected_in_candidate``, and ``type_drift`` tuples, making it trivial to
render change reports or emit warnings. A similar interface exists for files via
:func:`duckplus.schema.diff_files`—pass CSV, Parquet, or JSON locations and let
DuckPlus load the data for you.

```python
from duckplus import schema as schema_utils

report = schema_utils.diff_relations(relation, other_relation)
print(report.missing_from_candidate)
print(report.type_drift)
```

## Exporters

Sampling helpers convert relations into in-memory data structures without
breaking immutability:

- :meth:`Relation.sample_arrow <duckplus.relation.Relation.sample_arrow>`
- :meth:`Relation.sample_polars <duckplus.relation.Relation.sample_polars>`
- :meth:`Relation.sample_pandas <duckplus.relation.Relation.sample_pandas>`

Each helper validates that the source relation still has access to an open
connection, surfacing immediate feedback when the manager is closed.

## Table utilities

The :mod:`duckplus.table` module manages DuckDB tables with schema validation and
idempotent insert helpers. It reuses the metadata cached on relations to ensure
columns align before writes run.

### Automating checks in CI

Schema helpers shine in automated test suites. Pair
``schema_utils.diff_relations(relation, other)`` with ``pytest`` assertions to
guarantee staging datasets remain compatible with production tables. Because
diffs return regular Python dataclasses, you can snapshot the output with tools
like ``pytest-approvaltests`` or ``pytest-regressions``.

### Capturing documentation

Use ``report.type_drift`` (where ``report`` is the result of
``diff_relations``) to render documentation-friendly tables that stay
synchronised with the actual DuckDB definitions. Because the results are regular
dataclasses, they slot neatly into static site generators and Markdown
renderers.
