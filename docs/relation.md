# Relation helpers

The `duckplus.relation.Relation` wrapper keeps the lightweight DuckDB relation
API ergonomic while enforcing immutability. Column helpers like
[`Relation.add`](../duckplus/relation.py) let callers extend a relation with new
expressions without mutating the original instance.

## Working with ODBC data sources

DuckPlus wraps the DuckDB nano-ODBC community extension so relations can be
created directly from external databases. Pass ``extra_extensions=("nanodbc",)``
when constructing [:class:`~duckplus.duckcon.DuckCon`] to ensure the extension
is installed and loaded as the connection opens. DuckDB installs community
extensions per user profile; the first load may download the bundle, and offline
environments should pre-install it via the DuckDB CLI or the
``duckdb-extensions`` package before creating the connection.

```python
from duckplus import DuckCon, Relation

manager = DuckCon(extra_extensions=("nanodbc",))
with manager:
    # nano-ODBC is ready once the connection is open.

    sales = Relation.from_odbc_query(
        manager,
        "Driver={DuckDB};Database=/data/sales.duckdb",
        "SELECT * FROM annual_sales",
    )
    customers = Relation.from_odbc_table(
        manager,
        "Driver={SQLite3};Database=/data/crm.sqlite",
        "customers",
    )
```

Both helpers validate that the managed connection is open and raise a helpful
``RuntimeError`` when the extension has not been loaded yet, recommending the
``extra_extensions`` workflow. Queries can supply
parameter bindings via the ``parameters`` keyword, e.g.
``Relation.from_odbc_query(..., parameters=[2024])`` to avoid string
interpolation.

Call ``manager.extensions()`` inside the context manager to audit which
extensions are installed, their versions, and whether they are currently loaded
in the session. The information is sourced from DuckDB's ``duckdb_extensions()``
table function and reflects the machine-level installation DuckDB maintains for
community bundles.

DuckPlus' own integration tests rely on the environment variables
``DUCKPLUS_TEST_ODBC_CONNECTION``, ``DUCKPLUS_TEST_ODBC_QUERY``, and
``DUCKPLUS_TEST_ODBC_TABLE`` to point at an accessible data source. Configure
these variables locally to exercise the helpers end-to-end; otherwise the tests
will be skipped automatically in offline environments.

## Loading Excel workbooks

DuckDB's Excel community extension exposes a ``read_excel`` table function which
DuckPlus now wraps via :meth:`Relation.from_excel`. Either request the extension
when constructing :class:`DuckCon` or let the helper load it on demand:

```python
from duckplus import DuckCon, Relation

manager = DuckCon(extra_extensions=("excel",))
with manager:
    sales = Relation.from_excel(
        manager,
        "workbooks/sales.xlsx",
        sheet="Q1",
        header=True,
        names=("region", "amount"),
        dtype={"amount": "INTEGER"},
    )
```

If the extension has not been installed yet, DuckPlus attempts to install it via
DuckDB's ``INSTALL excel`` command and surfaces a helpful error message when the
machine is offline. The helper accepts the most common ``read_excel`` keyword
arguments—``sheet``, ``header``, ``skip``, ``limit``, ``names``, ``dtype``, and
``all_varchar``—while keeping the underlying relation immutable.

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

## Sampling relation data for notebooks

Notebook workflows often need a quick preview of relation data. DuckPlus now
offers dedicated sampling helpers that stream results into popular dataframe
libraries without mutating the original relation.

```python
from duckplus import DuckCon, Relation

manager = DuckCon()
with manager as connection:
    source = Relation.from_relation(
        manager,
        connection.sql(
            "SELECT * FROM (VALUES"
            " (1::INTEGER, 'alpha'::VARCHAR),"
            " (2::INTEGER, 'beta'::VARCHAR)"
            ") AS data(id, label)"
        ),
    )

preview = source.sample_pandas(limit=10)
for arrow_batch in source.iter_arrow_batches(batch_size=100):
    ...  # stream into analytics tooling

for polars_frame in source.iter_polars_batches(batch_size=1_000):
    ...  # process in polars without loading the full relation
```

`sample_pandas`, `sample_arrow`, and `sample_polars` return a single object
containing up to ``limit`` rows (defaulting to 50), while the corresponding
``iter_*_batches`` helpers yield generators that respect the requested
``batch_size``. Each helper validates that the managed connection remains open
and surfaces informative ``ModuleNotFoundError`` messages when optional
dependencies such as pandas, PyArrow, or Polars have not been installed yet.

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

## Profiling relation data

Exploratory analysis often benefits from quick metadata about the result set
before diving into more involved transformations. DuckPlus provides
:meth:`Relation.row_count` and :meth:`Relation.null_ratios` so callers can
profile relations without leaving Python:

```python
from duckplus import DuckCon, Relation

manager = DuckCon()
with manager as connection:
    relation = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT * FROM (VALUES
                (1::INTEGER, 'north'::VARCHAR, NULL::VARCHAR),
                (2::INTEGER, 'south'::VARCHAR, 'prime'::VARCHAR),
                (NULL::INTEGER, 'east'::VARCHAR, NULL::VARCHAR)
            ) AS data(id, region, segment)
            """.strip(),
        ),
    )

    rows = relation.row_count()
    nulls = relation.null_ratios()

print(rows)
print(nulls)
```

``Relation.null_ratios`` returns a dictionary keyed by the relation's column
names with floating-point ratios between ``0.0`` and ``1.0``. Empty relations
produce zero ratios while relations without columns return an empty dictionary.
In the example above the helper reports ``3`` rows and a mapping such as
``{"id": 0.3333333333, "region": 0.0, "segment": 0.6666666667}``.

## Writing relations into tables

Once a relation contains the desired result set, call
`DuckCon.table(<name>).insert(...)` to materialise it into a DuckDB table while
keeping connection management consistent with other helpers. The wrapper reuses
the same validation as the file appenders so callers can opt into table
creation, overwriting, or inserting into a subset of columns when defaults
should populate the remainder.

```python
from duckplus import DuckCon, Relation

manager = DuckCon()
with manager as connection:
    # Build a relation using the fluent helpers.
    relation = Relation.from_sql(
        manager,
        "SELECT * FROM (VALUES (1::INTEGER, 'north'::VARCHAR)) AS data(id, region)",
    )

    # Persist the relation into a table, creating it on first run.
    manager.table("analytics.customers").insert(relation, create=True, overwrite=True)

    rows = connection.sql("SELECT * FROM analytics.customers").fetchall()

assert rows == [(1, "north")]
```

Attempting to persist a relation created from another `DuckCon` instance raises
an informative error so call sites keep connection ownership explicit. When
`create=True`, the helper requires inserting every column to keep table schemas
deterministic; during append operations, specify `target_columns` to rely on
table defaults for any omitted values.

## Appending CSV and Parquet files

DuckPlus now treats file appends as IO operations performed directly by a
relation. Use :meth:`Relation.append_csv` to extend a CSV file with the relation
content while keeping the helper intentionally lightweight:

```python
from pathlib import Path

from duckplus import DuckCon, Relation

manager = DuckCon()
with manager:
    new_rows = Relation.from_sql(
        manager,
        "SELECT * FROM (VALUES (1, 'north'), (2, 'south')) AS data(id, region)",
    )

    # The CSV header is written automatically when the file does not exist.
    new_rows.append_csv(Path("data.csv"))
```

Existing files can be deduplicated before writing by either matching on a
unique identifier column (or columns) or by checking every column for a
duplicate match. Set ``unique_id_column="id"`` to perform an anti join on that
column, or pass ``match_all_columns=True`` to only append rows that differ from
all existing values. The helper returns a relation representing the rows that
would be appended, which makes it easy to inspect the anti-joined results when
``mutate=False``.

Parquet appends follow the same interface via
:meth:`Relation.append_parquet`, rewriting the target file through a temporary
Parquet file whenever ``mutate=True``:

```python
manager = DuckCon()
with manager:
    relation = Relation.from_sql(
        manager,
        "SELECT * FROM (VALUES (1, 'north'), (2, 'south')) AS data(id, region)",
    )

    # Rewrites data.parquet using a temporary file to keep Parquet immutable.
    relation.append_parquet(Path("data.parquet"), unique_id_column="id", mutate=True)
```

Because Parquet files are immutable, ``mutate`` defaults to ``False`` so callers
opt into the rewrite explicitly. CSV appends default to ``mutate=True`` since
they can extend the file without rebuilding existing rows. In both cases the
helpers reject directory targets, ensuring folder-level relations are not used
accidentally. DuckPlus keeps column comparisons case-insensitive and raises
clear errors when a requested identifier cannot be found on the relation or the
existing file.

## Writing partitioned Parquet datasets

When working with immutable file stores it is often preferable to maintain one
Parquet file per logical partition. :meth:`Relation.write_parquet_dataset`
persists a relation into a directory of Parquet files using a column to derive
the partition key. The helper creates the directory when needed and accepts a
filename template so pipelines can keep the existing naming convention:

```python
target = Path("/data/events")

relation.write_parquet_dataset(
    target,
    partition_column="partition_key",
    filename_template="{partition}.parquet",
)
```

By default the helper overwrites the Parquet file associated with each
partition. Provide ``partition_actions`` to mix append and overwrite behaviour
within the same batch. In the following example new rows for the ``north``
partition are appended while ``west`` is rebuilt from scratch:

```python
relation.write_parquet_dataset(
    target,
    partition_column="partition_key",
    partition_actions={"north": "append", "west": "overwrite"},
)
```

Set ``immutable=True`` to enforce append-only semantics. Existing partition
files trigger an error so ingestion jobs can guarantee that each run only adds
new partitions:

```python
relation.write_parquet_dataset(
    target,
    partition_column="partition_key",
    immutable=True,
)
```

All strategies require the underlying connection to remain open. The helper
relies on DuckDB's Parquet writer so compression and existing schema inference
behaviour mirror DuckDB's native commands.
