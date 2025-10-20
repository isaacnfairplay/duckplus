# Analytics recipes with typed expressions

The DuckDB demo closes with advanced analytics features such as window functions
and pivot-style summaries. DuckPlus exposes those constructs through the typed
DSL so column dependencies remain explicit.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    base = Relation.from_sql(
        manager,
        """
        SELECT * FROM (VALUES
            ('electronics', 'north', 1000),
            ('electronics', 'south', 700),
            ('furniture', 'north', 450),
            ('furniture', 'south', 1250)
        ) AS sales(category, region, amount)
        """,
    )

    amount = ducktype.Numeric("amount")
    region = ducktype.Varchar("region")
    running = amount.sum().over(partition_by=(region,), order_by=(amount.desc(),))

    ranked = base.add(running_total=running)
    pivoted = (
        ranked.aggregate()
        .start_agg()
        .agg(
            (
                ducktype.Numeric.case()
                .when(region == "north", amount)
                .else_(ducktype.Numeric.literal(0))
                .end()
                .sum()
            ),
            alias="north_total",
        )
        .agg(
            (
                ducktype.Numeric.case()
                .when(region == "south", amount)
                .else_(ducktype.Numeric.literal(0))
                .end()
                .sum()
            ),
            alias="south_total",
        )
        .all()
    )
    print(ranked.order_by("region", "amount").relation.fetchall())
    print(pivoted.relation.fetchall())
```

The :meth:`~duckplus.typed.expressions.base.TypedNumericExpression.over`
helper mirrors DuckDB's window syntax. Passing ``partition_by`` and ``order_by``
sequences keeps the signature discoverable in IDEs while DuckPlus validates the
referenced columns before executing the query.
