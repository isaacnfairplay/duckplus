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

## Filtering rows

`Relation.filter` applies one or more conditions to a relation while keeping the
original untouched. Conditions can mix raw SQL snippets with typed boolean
expressions, and each clause is validated against the source relation's column
metadata before DuckDB executes the query.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    base = Relation.from_relation(
        manager,
        connection.sql(
            "SELECT * FROM (VALUES",
            " ('a'::VARCHAR, 1::INTEGER),",
            " ('a'::VARCHAR, 2::INTEGER),",
            " ('b'::VARCHAR, 3::INTEGER)",
            ") AS data(category, amount)",
        ),
    )

    filtered = base.filter(
        "amount > 1",
        ducktype.Boolean.raw("category = 'b'", dependencies=["category"]),
    )

assert filtered.columns == ("category", "amount")
assert filtered.relation.fetchall() == [("b", 3)]
```

Like aggregation filters, blank conditions or references to unknown columns
raise descriptive errors so mistakes surface quickly.

## Joining relations

`Relation.join`, `left_join`, `right_join`, `outer_join`, and `semi_join` wrap
DuckDB's join operations while enforcing deterministic column ordering. DuckPlus
automatically joins on every column shared between the two relations (matching
names case-insensitively) and prefers the left relation's values when
duplicates exist. Additional join pairs can be supplied with the `on` keyword
argument, mapping left column names to right column names for equality
comparisons.

```python
from duckplus import DuckCon, Relation

manager = DuckCon()
with manager as connection:
    customers = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 'north'::VARCHAR),
                (2::INTEGER, 'south'::VARCHAR)
            ) AS data(customer_id, region)
            """
        ),
    )
    orders = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT * FROM (VALUES
                ('north'::VARCHAR, 1::INTEGER, 500::INTEGER),
                ('south'::VARCHAR, 2::INTEGER, 700::INTEGER)
            ) AS data(region, order_customer_id, total)
            """
        ),
    )

    joined = customers.join(orders, on={"customer_id": "order_customer_id"})

assert joined.columns == (
    "customer_id",
    "region",
    "order_customer_id",
    "total",
)
assert joined.relation.order("customer_id").fetchall() == [
    (1, "north", 1, 500),
    (2, "south", 2, 700),
]
```

When no additional columns are provided, DuckPlus joins solely on shared
column names. Helpers for other join flavours work identically: `left_join`
retains unmatched rows from the left relation while filling right-side columns
with `NULL`, and `semi_join` filters rows using the join keys but keeps only the
left relation's columns. Attempting to join relations originating from different
`DuckCon` instances or referencing unknown columns raises clear errors so
callers can rename inputs before running the query.

### As-of joins

`Relation.asof_join` aligns rows by the most recent ordering value from the
right-hand relation that does not violate the requested direction (backward by
default) or tolerance. The helper automatically reuses the case-insensitive
column matching logic from `Relation.join`, but requires callers to supply the
ordering columns explicitly. Ordering operands can be supplied as column names
or typed expressions; when using typed expressions, qualify column references
with the synthetic aliases `left` and `right` that DuckPlus assigns to the
underlying relations during query generation.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    trades = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 10::INTEGER),
                (1::INTEGER, 35::INTEGER)
            ) AS data(symbol, event_ts)
            """,
        ),
    )
    quotes = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 5::INTEGER, 100::INTEGER),
                (1::INTEGER, 30::INTEGER, 110::INTEGER)
            ) AS data(symbol, quote_ts, price)
            """,
        ),
    )

    joined = trades.asof_join(
        quotes,
        on={"symbol": "symbol"},
        order=("event_ts", "quote_ts"),
        tolerance=ducktype.Numeric.literal(15),
    )

assert joined.relation.fetchall() == [
    (1, 10, 5, 100),
    (1, 35, 30, 110),
]
```

Per-row tolerances can be provided via typed expressions as well:

```python
joined = trades.asof_join(
    quotes,
    on={"symbol": "symbol"},
    order=(
        ducktype.Numeric.coerce(("left", "event_ts")),
        ducktype.Numeric.coerce(("right", "quote_ts")),
    ),
    tolerance=ducktype.Numeric.coerce(("left", "max_gap")),
)
```

The helper validates that all referenced columns exist on the participating
relations and raises informative errors when aliases or dependencies do not
resolve. In the per-row tolerance example above, `trades` exposes a `max_gap`
column that controls how far each row may look back when selecting a matching
quote.
