# Typed API Overview

> **Note**
> Updated typed expression documentation is hosted at
> {doc}`versions/1.0/core/typed_expressions <versions/1.0/core/typed_expressions>`.
> The full DuckDB function catalog for DuckPlus 1.1 is available at
> {doc}`versions/1.1/api/typed/function_catalog <versions/1.1/api/typed/function_catalog>`.

DuckPlus exposes a typed expression builder that mirrors DuckDB's type system. Expressions record their DuckDB type metadata and column or table dependencies so downstream helpers can validate usage across complex queries.

## Core Factories

```python
from duckplus import ducktype

order_total = ducktype.Numeric("total")
customer_name = ducktype.Varchar("customer")
was_discounted = ducktype.Boolean("has_discount")
raw_payload = ducktype.Blob("payload")

# Optional table qualification is supported for dependency-aware column references
order_total_orders = ducktype.Numeric("total", table="orders")
assert order_total_orders.render() == '"orders"."total"'
```

Typed factories are also available directly from the top-level package, so
callers may import conveniences like `from duckplus import Varchar` when they
want direct access to the concrete factories without the shared namespace.

Each factory exposes helpers to construct literal and raw expressions:

```python
from decimal import Decimal

numeric_literal = ducktype.Numeric.literal(Decimal("5.75"))
string_literal = ducktype.Varchar.literal("VIP")
boolean_literal = ducktype.Boolean.literal(True)
blob_literal = ducktype.Blob.literal(b"\x00\xFF")
```

Numeric literals automatically tighten their DuckDB type based on the provided value. For example, `ducktype.Numeric.literal(1)` is typed as `UTINYINT` while larger integers flow through progressively wider integer types before falling back to `NUMERIC`. `Decimal` instances capture precision and scale so downstream consumers retain the full metadata.

Varchar expressions support Pythonic concatenation with literal operands:

```python
full_name = ducktype.Varchar("first_name") + " " + ducktype.Varchar("last_name")
assert full_name.render() == "(\"first_name\" || ' ' || \"last_name\")"
```

## Generic Expressions

`ducktype.Generic` creates expressions that preserve dependencies without making assumptions about the underlying DuckDB type. Generic expressions are ideal for helpers such as `max_by` where the return type follows a different operand.

```python
best_customer = ducktype.Generic("customer").max_by(ducktype.Numeric("score"))
```

Generic expressions intentionally do **not** expose numeric-only helpers. Attempting to call numeric-specific APIs such as `.sum()` will raise both a mypy error and a runtime exception.

## Aggregation

Numeric expressions grow aggregation helpers directly on the expression instance:

```python
total_revenue = ducktype.Numeric("revenue").sum()
mean_discount = ducktype.Numeric("discount").avg()
```

Raw helpers accept explicit dependency metadata for advanced scenarios such as table-level lineage tracking:

```python
from duckplus.typed import ExpressionDependency

orders_count = ducktype.Numeric.raw(
    "count(*)",
    dependencies=[("orders", None)],
)
assert orders_count.dependencies == {ExpressionDependency.table("orders")}
```

## Window Functions

Aggregates (and any other typed expressions) can be wrapped in a window clause with
`.over`, which accepts `partition_by`, `order_by`, and raw `frame` arguments:

```python
rolling_revenue = ducktype.Numeric("revenue").sum().over(
    partition_by=["customer_id"],
    order_by=[(ducktype.Numeric("event_ts"), "DESC")],
    frame="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
)
```

Dependencies from the partition and ordering expressions are merged onto the
result, keeping downstream helpers aware of all referenced columns.

## CASE Expressions

Typed factories expose a `.case()` helper that builds searched CASE expressions
while preserving type information and dependencies:

```python
customer_segment = (
    ducktype.Varchar.case()
    .when(ducktype.Boolean("is_high_value"), "VIP")
    .when(ducktype.Boolean("is_employee"), "Internal")
    .else_("Standard")
    .end()
)
```

Each `when` clause accepts any boolean expression (including literals), and the
`then`/`else` branches coerce through the underlying factory so literals and
typed expressions compose naturally. The builder returns a normal typed
expression, allowing nested CASE expressions or further chaining such as
`.alias("segment")`.

## SELECT Statements

The typed namespace also exposes a lightweight SELECT statement builder that
assembles projection lists and optional ``FROM`` clauses:

```python
statement = (
    ducktype.select()
    .column(ducktype.Numeric("amount"))
    .column(ducktype.Numeric("amount").sum().alias("total"))
    .column("CURRENT_DATE", alias="today")
    .from_("orders")
    .build()
)
assert statement == (
    'SELECT "amount", sum("amount") AS "total", CURRENT_DATE AS "today" '
    "FROM orders"
)
```

Expressions can be passed positionally or with explicit aliases, and existing
aliased expressions retain or override their alias depending on whether the
``alias`` parameter is supplied. The builder validates that the statement
includes at least one projection and enforces a single ``FROM`` clause before
rendering the final SQL string.

For convenience a ``star`` helper appends ``*`` projections with DuckDB's
``REPLACE`` and ``EXCLUDE`` modifiers, making it easy to build statements such
as ``SELECT * REPLACE ("value" AS "renamed")``. Call ``build_select_list`` to
render only the projection list—ideal for feeding into
``Relation.project``—or ``build`` to generate the full ``SELECT`` statement.

Column projections and ``star`` modifiers can be marked ``if_exists`` so they
are only applied when the required columns are available. When optional clauses
are used, pass the relation's column names to ``build`` or ``build_select_list``
and the builder will filter out any clauses that reference missing columns:

```python
def configure_builder():
    return (
        ducktype.select()
        .column(ducktype.Numeric("total"))
        .column(ducktype.Numeric("discount"), if_exists=True)
        .star(replace_if_exists={"net": ducktype.Numeric("net_total")})
    )

# ``discount`` and ``net`` are skipped when those columns are absent.
projection = configure_builder().build_select_list(available_columns=["total"])
assert projection == '"total"'

# They are included when the dependencies are available.
projection = configure_builder().build_select_list(
    available_columns=["total", "discount", "net_total"]
)
assert projection == '"total", "discount", * REPLACE ("net_total" AS "net")'
```

## Function Namespaces

The DuckDB function catalog is exposed via `ducktype.Functions` with scalar, aggregate, and window namespaces. Each namespace is grouped by return type for discoverability.

```python
abs_value = ducktype.Functions.Scalar.Numeric.abs(ducktype.Numeric("balance"))
substring = ducktype.Functions.Scalar.Varchar.substr(
    ducktype.Varchar("name"),
    ducktype.Numeric.literal(1),
    ducktype.Numeric.literal(3),
)
rolling_sum = ducktype.Functions.Window.Numeric.sum(ducktype.Numeric("sales"))
prefix_match = ducktype.Functions.Scalar.Boolean.starts_with(
    ducktype.Varchar("name"),
    "VIP",
)
```

The generated stubs advertise the return expression type so tools such as mypy and language servers understand the chaining semantics.

Function docstrings now embed DuckDB's catalog metadata, surfacing parameter
names, type signatures, macro definitions, and inline comments directly in your
editor. Each overload lists its schema-qualified SQL form so the generated
helpers remain aligned with DuckDB upgrades without manual bookkeeping.

## Type Metadata

Expression objects expose their DuckDB type via the `duck_type` attribute. The attribute stores concrete type classes that model DuckDB's hierarchy and allow nested structures such as lists or structs to be represented without losing metadata.

```python
expr = ducktype.Numeric("total")
assert expr.duck_type.render() == "NUMERIC"
```

The type objects can be extended for nested types by composing the helpers exposed in `duckplus.typed.types`.

Typed functions validate argument compatibility against the DuckDB type hierarchy. Narrow integer expressions (for example, a `UTINYINT` literal) can satisfy broader parameter slots such as `UINTEGER` or `UBIGINT` thanks to the ordered integer families baked into the type metadata, while incompatible widths raise clear errors during helper invocation.
