# Relation helpers

The `duckplus.relation.Relation` wrapper keeps the lightweight DuckDB relation
API ergonomic while enforcing immutability. Column helpers like
[`Relation.add`](../duckplus/relation.py) let callers extend a relation with new
expressions without mutating the original instance.

## Adding dependent columns

`Relation.add` accepts keyword arguments mapping new column names to SQL
expressions. Expressions are applied sequentially, so later columns can depend
on ones defined earlier in the same call:

```python
from duckplus import DuckCon, Relation

manager = DuckCon()
with manager as connection:
    base = Relation.from_relation(
        manager,
        connection.sql("SELECT 3::INTEGER AS value"),
    )

    extended = base.add(
        double="value * 2",
        quadruple="double * 2",
    )

assert extended.columns == ("value", "double", "quadruple")
assert extended.relation.fetchall() == [(3, 6, 12)]
```

If an expression references a column that does not exist yet (for example,
referencing `quadruple` while defining `double`), Duck Plus raises a
`ValueError` with a clear message describing the problem, mirroring the
validation behaviour for other column helpers.
