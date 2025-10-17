# Immutable relation helpers

The :class:`~duckplus.relation.Relation` wrapper keeps DuckDB relations immutable
while still exposing the familiar query primitives. Each helper validates column
references against the cached metadata so mistakes are caught before SQL is
executed. Because relations retain a reference to the originating
:class:`~duckplus.duckcon.DuckCon`, downstream exporters and IO helpers can
assert that the connection is still open before performing work.

## Constructing relations

Relations originate from `DuckCon`:

- :meth:`Relation.from_relation <duckplus.relation.Relation.from_relation>` wraps
  an existing ``DuckDBPyRelation``.
- :meth:`Relation.from_table <duckplus.relation.Relation.from_table>` and
  :meth:`Relation.from_query <duckplus.relation.Relation.from_query>` issue SQL.
- IO helpers such as :meth:`Relation.from_csv` and :meth:`Relation.from_parquet`
  create relations from files while keeping the parent connection cached.

All constructors record the available columns and DuckDB types so downstream
operations can validate dependencies. When you need to debug column casing,
consult :attr:`Relation.columns <duckplus.relation.Relation.columns>` and
:attr:`Relation.types <duckplus.relation.Relation.types>`; casing is preserved
exactly as returned by DuckDB and comparison helpers normalise behind the
scenes.

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
raising errors, making migration scripts resilient. For ad-hoc projections, use
:meth:`Relation.transform <duckplus.relation.Relation.transform>` to apply a
callable that receives the underlying ``DuckDBPyRelation`` and returns a new
one. The wrapper will capture the resulting metadata automatically.

## Aggregation and window functions

:meth:`Relation.aggregate <duckplus.relation.Relation.aggregate>` groups rows and
computes aggregates with typed expressions. Column validation ensures typed
expressions only reference the source relation. Pass strings or non-aggregate
boolean expressions positionally to filter rows before grouping, and provide
aliased aggregate expressions either positionally or as keyword arguments.

```python
amount = ducktype.Numeric("amount")
summary = base.aggregate(
    ducktype.Varchar("category"),
    "amount > 0",
    amount.sum().alias("total_sales"),
    amount.avg().alias("average_sale"),
    amount.avg() > 25,
)
```

Aggregate boolean expressions are treated as ``HAVING`` clauses and rewritten to
reference the projected aliases. Strings containing aggregate functions follow
the same path, so ``"sum(amount) > 100"`` is rewritten to the corresponding
alias even if casing or identifier quoting differ. Non-aggregate expressions
become additional grouping expressions, keeping the aggregate SQL concise.

Typed expressions expose window helpers via
:meth:`duckplus.typed.expressions.base.TypedExpression.over`, enabling fluent
analytics queries without dropping down to SQL strings.

## Filtering and joins

`Relation.filter` accepts either SQL snippets or typed boolean expressions. The
helper normalises clauses, validates dependencies, and returns a new relation.
Chain the result into additional helpers—aggregations, joins, or the typed
SELECT builder—to continue refining datasets without mutating the original.

Join helpers such as :meth:`Relation.join`, :meth:`Relation.left_join`, and
:meth:`Relation.asof_join` mirror DuckDB semantics while ensuring join columns
exist on both sides. All join methods return immutable relations, so you can
chain operations confidently.

## Schema utilities

DuckPlus surfaces ergonomic schema helpers via :mod:`duckplus.schema`:

- :func:`duckplus.schema.diff_relations` compares two relations and highlights
  column additions, removals, and type drift.
- :func:`duckplus.schema.diff_files` performs the same comparison across CSV,
  Parquet, or JSON sources using the IO module under the hood.
- Sampling helpers export relations to Pandas, Arrow, and Polars without loading
  the full dataset into memory.

Additional schema guidance lives in the
[schema management walkthrough](../schema_management.md).

## Introspection and diagnostics

Because relations retain an active connection, diagnostic helpers can compute
statistics lazily:

- :meth:`Relation.row_count <duckplus.relation.Relation.row_count>` executes a
  ``COUNT(*)`` under the hood, returning ``0`` when the relation yields no rows.
- :meth:`Relation.null_ratios <duckplus.relation.Relation.null_ratios>` reports a
  floating-point ratio for each column, making it easier to surface data-quality
  issues early.
- Sampling helpers such as :meth:`Relation.sample_pandas
  <duckplus.relation.Relation.sample_pandas>` and :meth:`Relation.sample_arrow
  <duckplus.relation.Relation.sample_arrow>` let you inspect representative
  samples without materialising the entire dataset in memory.

All helpers validate that the connection is still open, raising a descriptive
error that points back to :class:`DuckCon` when the relation is detached.

## Exporting data

DuckPlus focuses on interoperability:

- :meth:`Relation.iter_arrow_batches <duckplus.relation.Relation.iter_arrow_batches>`
  integrates with PyArrow pipelines.
- :meth:`Relation.iter_pandas_batches <duckplus.relation.Relation.iter_pandas_batches>`
  and :meth:`Relation.iter_polars_batches <duckplus.relation.Relation.iter_polars_batches>`
  support streaming into external systems.
- :meth:`Relation.append_csv <duckplus.relation.Relation.append_csv>` and
  :meth:`Relation.append_parquet <duckplus.relation.Relation.append_parquet>`
  write immutable relations back to disk while maintaining schema guarantees.

Each exporter performs capability checks (for example ensuring ``pyarrow`` is
installed) and explains the remedy when a dependency is missing.
