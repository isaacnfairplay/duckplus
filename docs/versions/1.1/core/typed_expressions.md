# Typed expression API

DuckPlus' typed expression DSL keeps SQL construction readable while preserving
DuckDB semantics. Expressions carry dependency metadata so relation helpers can
validate column references early. Unlike plain strings, typed expressions know
their DuckDB logical type, whether they are nullable, and which columns they
depend on. That context powers validation across the rest of the package.

```{note}
An experimental, statically defined variant of the DSL lives under
``duckplus.static_typed``. Import ``static_ducktype`` from the package root to
opt into the new API while ``duckplus.typed`` continues to ship unchanged for
existing integrations.
```

## Creating typed expressions

Use the :mod:`duckplus.typed.ducktype` factory to construct expression objects
based on DuckDB types. The same factory is exposed from the experimental
``duckplus.static_typed`` namespace as :data:`duckplus.static_ducktype`.

```python
from duckplus.typed import ducktype

amount = ducktype.Numeric("amount")
customer = ducktype.Varchar("customer")
order_date = ducktype.Date("order_date")
order_id = ducktype.Integer("order_id")
```

The namespace also exposes narrower numeric and temporal factories so column
definitions can mirror DuckDB storage types without sacrificing ergonomics.
Pair them with literal helpers such as ``ducktype.Date.literal("2024-01-01")``
or ``ducktype.Timestamp.literal(datetime.utcnow())`` when embedding constants in
expressions. Timestamp factories are available for each precision DuckDB
supports (``ducktype.Timestamp_s``, ``ducktype.Timestamp_ms``,
``ducktype.Timestamp_us``, ``ducktype.Timestamp_ns``) along with
``ducktype.Timestamp_tz`` for ``TIMESTAMP WITH TIME ZONE`` columns. Decimal
storage is also enumerated via ``ducktype.Decimal_<width>_<scale>`` factories so
typed expressions can match DuckDB's ``DECIMAL`` permutations without manual
type strings.

Expressions know which columns they depend on. Combining expressions merges
those dependencies so downstream helpers can ensure referenced columns exist.
You can inspect dependencies directly via
:attr:`duckplus.typed.expressions.base.TypedExpression.dependencies` to build
documentation or runtime audits.

## Arithmetic and comparison helpers

Typed expressions implement Python operators. Comparisons return boolean typed
expressions that can be fed into :meth:`Relation.filter
<duckplus.relation.Relation.filter>`.

```python
high_value = amount > 100
promotions = (customer == "prime") | (customer == "enterprise")
discounted = ducktype.Boolean("is_discounted")
recent_orders = order_date > ducktype.Date.literal("2024-01-01")

eligible = promotions & ~discounted & recent_orders
```

## Aggregation helpers

Aggregation helpers live on the typed expression itself:

```python
summary = (
    base.aggregate()
    .start_agg()
    .agg(amount.sum(), alias="total_sales")
    .agg(amount.avg(), alias="average_sale")
    .agg(amount.max(), alias="largest_sale")
    .by("customer")
)
```

Aggregations can be filtered using :meth:`duckplus.typed.expressions.base.TypedExpression.filter` and
composed with window functions via :meth:`duckplus.typed.expressions.base.TypedExpression.over`.

## Window functions

Window helpers mirror DuckDB's ``OVER`` clause while keeping dependencies
explicit:

```python
running_total = amount.sum().over(
    partition_by=("customer",),
    order_by=(("order_date", "ASC"),),
)
```

Typed expressions expose ``rows`` and ``range`` framing helpers, as well as
``order`` direction keywords so the DSL stays expressive.

## Conditional expressions and aliases

Use :meth:`duckplus.typed.expressions.base.TypedExpression.alias` to assign an
output name without immediately materialising the expression. Pair it with the
:class:`~duckplus.typed.CaseExpressionBuilder` returned by the varchar factory
to model conditional logic. Remember to call ``end()`` once the branches are
defined:

```python
status = (
    ducktype.Varchar.case()
    .when(ducktype.Boolean("is_returned"), ducktype.Varchar.literal("returned"))
    .otherwise(ducktype.Varchar.literal("fulfilled"))
    .end()
    .alias("order_status")
)

case = (
    ducktype.Varchar.case()
    .when(amount > 500, ducktype.Varchar.literal("enterprise"))
    .when(amount > 100, ducktype.Varchar.literal("growth"))
    .otherwise(ducktype.Varchar.literal("starter"))
    .end()
    .alias("segment")
)
```

Both helpers keep dependencies precise, ensuring that relation operations flag a
missing ``amount`` column before executing SQL.

## Scalar helpers

Scalar helpers surface directly on the typed expressions. This keeps editor
integrations ergonomic while maintaining dependency tracking:

```python
abs_distance = ducktype.Numeric("distance").abs()
snippet = ducktype.Varchar("description").slice(1, 5)
trimmed = ducktype.Varchar("raw_id").trim("-")
coerced = ducktype.Varchar("invoice").try_cast("INTEGER")
```

Factory namespaces also expose ``Aggregate`` helpers mirroring DuckDB's built-in
functions, making it straightforward to aggregate literal columns without
building the expression first:

```python
count_all = ducktype.Numeric.Aggregate.count()
latest_event = ducktype.Generic.Aggregate.max("event_timestamp")
```

## Select builder

The :mod:`duckplus.typed.select` module provides a chainable builder for SELECT
statements. The builder keeps projections, filters, ordering, and window clauses
immutable while validating dependencies just like the relation helpers.

```python
from duckplus.typed import select

query = (
    select.from_relation(base)
    .with_columns(amount, amount.sum().over(partition_by=("customer",)))
    .order_by(("customer", "ASC"))
)
result = query.execute()
```

When combined with :class:`DuckCon`, the builder makes it straightforward to
construct reusable query templates while honouring the open/closed principle:
extending the API involves adding new expression helpers rather than rewriting
existing ones.

## Debugging typed expressions

Every typed expression renders to SQL via ``str(expression)``. Use this to debug
unexpected behaviour or to embed expressions in logs during development. Inspect
``expression.dependencies`` to confirm the referenced columns—values are stored
as :class:`~duckplus.typed.dependencies.ExpressionDependency` objects matching
the checks performed by :class:`Relation`. Keeping the inspection surface area
rich makes it easier to reason about complex queries without leaving Python.
