# Immutable relation helpers

The :class:`~duckplus.relation.Relation` wrapper keeps DuckDB relations immutable
while still exposing the familiar query primitives. Each helper validates column
references against the cached metadata so mistakes are caught before SQL is
executed.

## Constructing relations

Relations originate from `DuckCon`:

- :meth:`Relation.from_relation <duckplus.relation.Relation.from_relation>` wraps
  an existing ``DuckDBPyRelation``.
- :meth:`Relation.from_table <duckplus.relation.Relation.from_table>` and
  :meth:`Relation.from_query <duckplus.relation.Relation.from_query>` issue SQL.
- IO helpers such as :meth:`Relation.from_csv` and :meth:`Relation.from_parquet`
  create relations from files while keeping the parent connection cached.

All constructors record the available columns and DuckDB types so downstream
operations can validate dependencies.

## Column management

Column helpers prefer keyword arguments to keep code readable:

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as con:
    base = Relation.from_relation(
        manager,
        con.sql("SELECT 3::INTEGER AS value, 5::INTEGER AS other"),
    )

    enriched = base.add(
        total=ducktype.Numeric("value") + ducktype.Numeric("other"),
        delta=ducktype.Numeric("value") - ducktype.Numeric("other"),
    )

    renamed = enriched.rename(total_sales="total")
    subset = renamed.keep("value", "total_sales")
```

The helpers never mutate ``base``—each call returns a fresh relation. DuckPlus
also provides ``*_if_exists`` variants that skip missing columns instead of
raising errors, making migration scripts resilient.

## Aggregation and window functions

:meth:`Relation.aggregate <duckplus.relation.Relation.aggregate>` groups rows and
computes aggregates with typed expressions. Column validation ensures typed
expressions only reference the source relation.

```python
amount = ducktype.Numeric("amount")
summary = base.aggregate(("category",), total_sales=amount.sum())
```

Typed expressions expose window helpers via
:meth:`duckplus.typed.expressions.base.TypedExpression.over`, enabling fluent
analytics queries without dropping down to SQL strings.

## Filtering and joins

`Relation.filter` accepts either SQL snippets or typed boolean expressions. The
helper normalises clauses, validates dependencies, and returns a new relation.

Join helpers such as :meth:`Relation.join`, :meth:`Relation.left_join`, and
:meth:`Relation.asof_join` mirror DuckDB semantics while ensuring join columns
exist on both sides. All join methods return immutable relations, so you can
chain operations confidently.

## Schema utilities

DuckPlus surfaces ergonomic schema helpers:

- :meth:`Relation.schema` returns DuckDB's column metadata.
- :meth:`Relation.schema_diff` compares two relations and highlights type drift.
- Sampling helpers export relations to Pandas, Arrow, and Polars without loading
  the full dataset into memory.

Additional schema guidance lives in the
[schema management walkthrough](../schema_management.md).
