# Typed API Overview

> **Note**
> Updated typed expression documentation is hosted at
> {doc}`versions/1.1/core/typed_expressions <versions/1.1/core/typed_expressions>`.
> The full DuckDB function catalog for DuckPlus 1.1 is available at
> {doc}`versions/1.1/api/typed/function_catalog <versions/1.1/api/typed/function_catalog>`.

DuckPlus exposes a typed expression builder that mirrors DuckDB's type system. Expressions record their DuckDB type metadata and column or table dependencies so downstream helpers can validate usage across complex queries.

```{tip}
Typed helpers register through normal Python imports. Decorators such as
:func:`duckplus.typed.functions.duckdb_function` attach each helper to its
namespace class during module import, keeping IDEs and documentation aligned
with the runtime objects. Avoid adding new registry dictionaries—prefer explicit
methods or mixins instead.
```

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
Legacy integrations that manually called `ducktype._register_decimal_factories()` (or inspected `_decimal_names`) continue to work, but new instances automatically expose every decimal combination and publish the available names through `decimal_factory_names`.

Varchar expressions support Pythonic concatenation with literal operands:

```python
full_name = ducktype.Varchar("first_name") + " " + ducktype.Varchar("last_name")
assert full_name.render() == "(\"first_name\" || ' ' || \"last_name\")"
```

Use `.trim()` to remove leading and trailing characters from varchar
expressions, optionally passing custom characters to strip:

```python
identifier = ducktype.Varchar("raw_id").trim("-")
assert identifier.render() == "trim(\"raw_id\", '-')"
```

All typed expressions surface `.cast(target)` and `.try_cast(target)` helpers
that accept either a DuckDB type specification string or a `DuckDBType`
instance. The helpers return a new typed expression that tracks the requested
type while preserving the original dependencies:

```python
invoice_number = ducktype.Varchar("invoice").try_cast("INTEGER")
assert invoice_number.render() == 'TRY_CAST("invoice" AS INTEGER)'
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

When you need to aggregate directly from the factory without first building an
expression, use the ``Aggregate`` helpers on each namespace:

```python
orders_count = ducktype.Numeric.Aggregate.count()
recent_revenue = ducktype.Numeric.Aggregate.sum("revenue")
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
    .otherwise("Standard")
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

## Scalar Helpers

Scalar helpers surface as instance methods on the typed expressions themselves.
This keeps the API discoverable and removes the need for a central function
registry:

```python
abs_value = ducktype.Numeric("balance").abs()
substring = ducktype.Varchar("name").slice(1, 3)
rolling_sum = ducktype.Numeric("sales").sum().over(order_by=["event_date"])
prefix_match = ducktype.Varchar("name").starts_with("VIP")
```

These methods return typed expressions that preserve dependencies and DuckDB
type metadata, so downstream chaining behaves exactly like column-based
expressions.

## Type Metadata

Expression objects expose their DuckDB type via the `duck_type` attribute. The attribute stores concrete type classes that model DuckDB's hierarchy and allow nested structures such as lists or structs to be represented without losing metadata.

```python
expr = ducktype.Numeric("total")
assert expr.duck_type.render() == "NUMERIC"
```

The type objects can be extended for nested types by composing the helpers exposed in `duckplus.typed.types`.

Typed functions validate argument compatibility against the DuckDB type hierarchy. Narrow integer expressions (for example, a `UTINYINT` literal) can satisfy broader parameter slots such as `UINTEGER` or `UBIGINT` thanks to the ordered integer families baked into the type metadata, while incompatible widths raise clear errors during helper invocation.

The full DuckDB function catalog is statically generated into Python source and exposed via the `SCALAR_FUNCTIONS`, `AGGREGATE_FUNCTIONS`, and `WINDOW_FUNCTIONS` namespaces re-exported from `duckplus.typed`. Every helper is a concrete method decorated with `duckdb_function`, which registers the DuckDB identifier (and any symbolic operators) when the module imports. This keeps the API introspectable without relying on runtime-populated registries, and the same data feeds the shipped type stub so completions remain available even without a DuckDB connection.

For callers that previously inspected `_IDENTIFIER_FUNCTIONS` or `_SYMBOLIC_FUNCTIONS` directly, those dictionaries remain populated so legacy discovery code keeps functioning during the decorator transition.

```python
from duckplus.typed import SCALAR_FUNCTIONS

lowered = SCALAR_FUNCTIONS.Varchar.lower("customer_name")
assert lowered.render() == 'lower("customer_name")'
```

