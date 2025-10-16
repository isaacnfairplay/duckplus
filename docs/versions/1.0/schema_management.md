# Schema management

DuckPlus keeps schema validation front and centre so data pipelines remain
predictable.

## Inspecting schemas

:meth:`duckplus.relation.Relation.schema` returns a rich description of each
column, including DuckDB's logical type, nullability, and order.

```python
schema = relation.schema()
for column in schema.columns:
    print(column.name, column.type)
```

## Diffing relations

:meth:`duckplus.relation.Relation.schema_diff` compares two relations and reports
column additions, removals, and type changes. The helper is invaluable when
validating migrations or append workflows.

```python
report = relation.schema_diff(other_relation)
print(report.new_columns)
print(report.changed_columns)
```

## Exporters

Sampling helpers convert relations into in-memory data structures without
breaking immutability:

- :meth:`Relation.to_arrow_table`
- :meth:`Relation.to_polars`
- :meth:`Relation.to_pandas`

Each helper validates that the source relation still has access to an open
connection, surfacing immediate feedback when the manager is closed.

## Table utilities

The :mod:`duckplus.table` module manages DuckDB tables with schema validation and
idempotent insert helpers. It reuses the metadata cached on relations to ensure
columns align before writes run.
