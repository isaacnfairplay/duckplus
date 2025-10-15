# Relation helpers

The `duckplus.relation.Relation` wrapper keeps the lightweight DuckDB relation
API ergonomic while enforcing immutability. Column helpers like
[`Relation.add`](../duckplus/relation.py) let callers extend a relation with new
expressions without mutating the original instance.

## Adding computed columns

`Relation.add` accepts keyword arguments mapping new column names to SQL
expressions or typed expressions from :mod:`duckplus.typed`. When typed
expressions are provided, their dependency metadata allows DuckPlus to validate
that references only target columns already present on the relation.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    base = Relation.from_relation(
        manager,
        connection.sql("SELECT 3::INTEGER AS value, 5::INTEGER AS other"),
    )

    extended = base.add(
        total=ducktype.Numeric("value") + ducktype.Numeric("other"),
        delta="value - other",
    )

assert extended.columns == ("value", "other", "total", "delta")
assert extended.relation.fetchall() == [(3, 5, 8, -2)]
```

If an expression references a column that does not exist on the original
relation—for example, referencing `total` while defining `delta`—DuckPlus raises
a `ValueError` explaining that the expression references unknown columns. This
mirrors the validation performed for other column helpers and keeps column
creation deterministic.

## Grouping and aggregating data

`Relation.aggregate` groups rows by a set of existing columns and computes
named aggregate expressions. Aggregations can be provided as raw SQL strings or
typed expressions, and optional filter predicates limit the input rows before
aggregation. Grouping columns are validated against the original relation so
typos surface immediately.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    base = Relation.from_relation(
        manager,
        connection.sql(
            "SELECT * FROM (VALUES"
            " ('a'::VARCHAR, 1::INTEGER),"
            " ('a'::VARCHAR, 2::INTEGER),"
            " ('b'::VARCHAR, 3::INTEGER)
            ) AS data(category, amount)"
        ),
    )

    summary = base.aggregate(
        ("category",),
        ducktype.Boolean.raw("amount > 1", dependencies=["amount"]),
        total_sales=ducktype.Numeric.Aggregate.sum("amount"),
        average_sale="avg(amount)",
    )

assert summary.columns == ("category", "total_sales", "average_sale")
assert summary.relation.order("category").fetchall() == [
    ("a", 2, 2.0),
    ("b", 3, 3.0),
]
```

Filters accept either SQL snippets or typed boolean expressions. If a filter or
aggregate references an unknown column, DuckPlus raises a descriptive error
before executing the query, keeping failures easy to diagnose.
