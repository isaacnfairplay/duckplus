Duck+ Demo Gallery
==================

This gallery collects scenario-driven walkthroughs that highlight how Duck+
wraps DuckDB for production-friendly analytics. Each section focuses on a
specific capability—connections, relational transforms, joins, IO, secrets, and
presentations—so you can adapt the snippets directly into your own pipelines.

.. contents::
   :local:
   :depth: 2

In-memory analytics pipeline
----------------------------

Start with an in-memory database, build a reusable relation, and materialize the
results. This demonstrates the immutable ``DuckRel`` wrapper, strict column
projection, parameterized filters, and Arrow-backed materialization.

.. code-block:: python

   from pprint import pprint

   from duckplus import DuckRel, connect

   with connect() as conn:
       source = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (1, 'North', 'Widget', 120.0),
                   (2, 'South', 'Widget', 95.0),
                   (3, 'North', 'Gadget', 180.0),
                   (4, 'East',  'Widget', 60.0)
               ) AS t(order_id, region, product, revenue)
               """
           )
       )

       regional_mix = (
           source
           .filter('"revenue" >= ?', 75)
           .project_columns("region", "product", "revenue")
           .order_by(region="asc", revenue="desc")
       )

       summary = regional_mix.materialize()
       top_rows = summary.require_table().to_pylist()
       pprint(top_rows)

.. code-block:: text

   [{'product': 'Gadget', 'region': 'North', 'revenue': Decimal('180.0')},
    {'product': 'Widget', 'region': 'North', 'revenue': Decimal('120.0')},
    {'product': 'Widget', 'region': 'South', 'revenue': Decimal('95.0')}]

Key takeaways
~~~~~~~~~~~~~

* ``DuckRel`` tracks column casing and type metadata, ensuring downstream
  projections stay explicit.
* ``filter`` accepts DuckDB parameter placeholders (``?``) and automatically
  coerces scalar Python arguments to SQL literals.
* ``materialize`` defaults to ``ArrowMaterializeStrategy`` so you can access an
  Arrow table without exporting data back into Python row by row.

Join choreography with inspection
---------------------------------

Duck+ exposes natural joins, explicit ``JoinSpec`` driven joins, partitioned
joins, and partition inspectors. This demo shows how to reason about join
cardinality before executing a more selective partitioned join.

.. code-block:: python

   from pprint import pprint

   from duckplus import (
       AsofOrder,
       ColumnPredicate,
       DuckRel,
       JoinProjection,
       JoinSpec,
       PartitionSpec,
       connect,
   )

   with connect() as conn:
       orders = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
               (1001, 501, DATE '2024-02-01', 'Widget', 120.50),
               (1002, 502, DATE '2024-02-02', 'Widget', 75.25),
               (1003, 501, DATE '2024-02-04', 'Gadget', 89.00)
           ) AS t(order_id, customer_ref, order_date, product, amount)
               """
           )
       )

       customers = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (501, 'Acme Corp', DATE '2023-01-10', 'gold'),
                   (502, 'Globex',    DATE '2023-03-02', 'silver')
               ) AS t(id, name, customer_since, tier)
               """
           )
       )

       inventory = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('Widget', DATE '2024-02-01', 200),
                   ('Widget', DATE '2024-02-03', 150),
                   ('Gadget', DATE '2024-02-02',  80)
               ) AS t(product, snapshot_date, on_hand)
               """
           )
       )

       partition_review = orders.inspect_partitions(
           customers, PartitionSpec.from_mapping({"customer_ref": "id"})
       )
       pprint(partition_review.materialize().require_table().to_pylist())

       join_spec = JoinSpec(
           equal_keys=[("customer_ref", "id")],
           predicates=[
               ColumnPredicate("order_date", ">=", "customer_since"),
           ],
       )

       decorated = orders.partitioned_inner(
           customers,
           PartitionSpec.from_mapping({"customer_ref": "id"}),
           join_spec,
           project=JoinProjection(allow_collisions=True),
       )

       priced = decorated.natural_left(inventory)

       latest_stock = priced.natural_asof(
           inventory,
           order=AsofOrder(left="order_date", right="snapshot_date"),
           suffixes=("", "_latest"),
       )
       pprint(latest_stock.materialize().require_table().to_pylist())

.. code-block:: text

   [{'customer_ref': 501,
     'left_count': 2,
     'pair_count': 2,
     'right_count': 1,
     'shared_partition': True},
    {'customer_ref': 502,
     'left_count': 1,
     'pair_count': 1,
     'right_count': 1,
     'shared_partition': True}]

.. code-block:: text

   [{'amount': Decimal('120.50'),
     'customer_ref': 501,
     'customer_since': datetime.date(2023, 1, 10),
     'name': 'Acme Corp',
     'on_hand': 150,
     'order_date': datetime.date(2024, 2, 1),
     'order_id': 1001,
     'product': 'Widget',
     'snapshot_date': datetime.date(2024, 2, 3),
     'tier': 'gold'},
    {'amount': Decimal('75.25'),
     'customer_ref': 502,
     'customer_since': datetime.date(2023, 3, 2),
     'name': 'Globex',
     'on_hand': 150,
     'order_date': datetime.date(2024, 2, 2),
     'order_id': 1002,
     'product': 'Widget',
     'snapshot_date': datetime.date(2024, 2, 3),
     'tier': 'silver'},
    {'amount': Decimal('89.00'),
     'customer_ref': 501,
     'customer_since': datetime.date(2023, 1, 10),
     'name': 'Acme Corp',
     'on_hand': 80,
     'order_date': datetime.date(2024, 2, 4),
     'order_id': 1003,
     'product': 'Gadget',
     'snapshot_date': datetime.date(2024, 2, 2),
     'tier': 'gold'},
    {'amount': Decimal('120.50'),
     'customer_ref': 501,
     'customer_since': datetime.date(2023, 1, 10),
     'name': 'Acme Corp',
     'on_hand': 200,
     'order_date': datetime.date(2024, 2, 1),
     'order_id': 1001,
     'product': 'Widget',
     'snapshot_date': datetime.date(2024, 2, 1),
     'tier': 'gold'},
    {'amount': Decimal('75.25'),
     'customer_ref': 502,
     'customer_since': datetime.date(2023, 3, 2),
     'name': 'Globex',
     'on_hand': 200,
     'order_date': datetime.date(2024, 2, 2),
     'order_id': 1002,
     'product': 'Widget',
     'snapshot_date': datetime.date(2024, 2, 1),
     'tier': 'silver'}]

Highlights
~~~~~~~~~~

* ``inspect_partitions`` surfaces partition-level row counts so you can gauge
  join fan-out before running the heavier join.
* ``partitioned_inner`` keeps equality and predicate logic separate: partition
  keys restrict candidate matches, while ``JoinSpec`` describes the precise join
  columns and comparison predicates.
* ``natural_asof`` layers time-aware lookups on top of the natural join
  semantics, dropping duplicate keys from the right-hand side while allowing
  suffixes to disambiguate payload columns.

Idempotent ingestion into managed tables
----------------------------------------

``DuckTable`` complements ``DuckRel`` by managing mutable tables. The following
walkthrough shows how to create a staging table, append rows in column order, and
protect downstream inserts with anti-joins and continuous ID guards.

.. code-block:: python

   from pprint import pprint

   from duckplus import DuckRel, DuckTable, connect

   with connect() as conn:
       conn.raw.execute(
           "CREATE TABLE staging_orders(order_id INTEGER, product VARCHAR, amount DOUBLE)"
       )
       table = DuckTable(conn, "staging_orders")

       feed = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (2001, 'Widget', 55.25),
                   (2002, 'Gadget', 210.00),
                   (2003, 'Widget', 35.00)
               ) AS t(order_id, product, amount)
               """
           )
       )

       table.append(feed)

       updates = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (2002, 'Gadget', 210.00),
                   (2004, 'Widget', 90.00)
               ) AS t(order_id, product, amount)
               """
           )
       )

       inserted = table.insert_antijoin(updates, keys=["order_id"])
       print(f"Inserted {inserted} new rows via anti-join")

       late_feed = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (2005, 'Widget', 102.00),
                   (2006, 'Widget', 88.00),
                   (2007, 'Gadget', 150.00)
               ) AS t(order_id, product, amount)
               """
           )
       )

       gated = table.insert_by_continuous_id(late_feed, id_column="order_id")
       print(f"Appended {gated} rows beyond current max ID")

       snapshot = DuckRel(conn.raw.table("staging_orders"))
       pprint(snapshot.materialize().require_table().to_pylist())

.. code-block:: text

   Inserted 1 new rows via anti-join
   Appended 3 rows beyond current max ID
   [{'amount': 55.25, 'order_id': 2001, 'product': 'Widget'},
    {'amount': 210.0, 'order_id': 2002, 'product': 'Gadget'},
    {'amount': 35.0, 'order_id': 2003, 'product': 'Widget'},
    {'amount': 90.0, 'order_id': 2004, 'product': 'Widget'},
    {'amount': 102.0, 'order_id': 2005, 'product': 'Widget'},
    {'amount': 88.0, 'order_id': 2006, 'product': 'Widget'},
    {'amount': 150.0, 'order_id': 2007, 'product': 'Gadget'}]

Operational notes
~~~~~~~~~~~~~~~~~

* ``append`` defaults to ``by_name=True`` and automatically reorders relation
  columns to match the table definition.
* ``insert_antijoin`` filters incoming rows by the specified keys before
  appending, returning the number of persisted rows.
* ``insert_by_continuous_id`` reads the current maximum ID directly from DuckDB
  and reuses ``insert_antijoin`` after filtering rows greater than (or equal to,
  when ``inclusive=True``) the maximum.

File IO and pipeline bridging
-----------------------------

Reader and writer helpers keep filesystem interactions declarative. This demo
illustrates loading multiple Parquet files, shaping the schema, and writing both
Parquet and CSV outputs with explicit compression.

.. code-block:: python

   from pathlib import Path
   from tempfile import TemporaryDirectory

   from duckplus import (
       read_parquet,
       write_csv,
       write_parquet,
       connect,
   )

   with TemporaryDirectory() as tmpdir:
       base = Path(tmpdir)
       warehouse = base / "warehouse"
       analytics = base / "analytics"
       warehouse.mkdir()
       analytics.mkdir()

       with connect() as conn:
           conn.raw.execute(
               """
               COPY (
                   SELECT 'Widget' AS product, DATE '2024-02-01' AS snapshot_date, 200 AS on_hand
               ) TO $warehouse1 (FORMAT PARQUET)
               """,
               parameters={"warehouse1": str(warehouse / "inventory_2024-02-01.parquet")},
           )
           conn.raw.execute(
               """
               COPY (
                   SELECT *
                   FROM (
                       VALUES
                           ('Widget', DATE '2024-02-03', 150),
                           ('Gadget', DATE '2024-02-02', 80)
                   ) AS t(product, snapshot_date, on_hand)
               ) TO $warehouse2 (FORMAT PARQUET)
               """,
               parameters={"warehouse2": str(warehouse / "inventory_2024-02-02.parquet")},
           )

           dataset = read_parquet(
               conn,
               [
                   warehouse / "inventory_2024-02-01.parquet",
                   warehouse / "inventory_2024-02-02.parquet",
               ],
               union_by_name=True,
           )

           normalized = (
               dataset
               .project({
                   "product": '"product"',
                   "snapshot_date": 'CAST("snapshot_date" AS DATE)',
                   "on_hand": 'CAST("on_hand" AS INTEGER)',
               })
               .order_by(snapshot_date="asc", product="asc")
           )

           write_parquet(
               normalized,
               analytics / "inventory_normalized.parquet",
               compression="zstd",
           )

           write_csv(
               normalized.limit(50),
               analytics / "inventory_sample.csv",
               compression="gzip",
               header=True,
           )

       print(sorted(p.name for p in analytics.iterdir()))

.. code-block:: text

   ['inventory_normalized.parquet', 'inventory_sample.csv']

Practical points
~~~~~~~~~~~~~~~~

* Duck+ normalizes paths via ``os.fspath`` so ``Path`` objects, ``os.DirEntry``,
  or any ``__fspath__`` implementer is accepted.
* ``write_parquet`` stages writes through a temporary file and renames only when
  DuckDB finishes successfully, preventing partially written outputs.
* ``write_csv`` exposes DuckDB's encoding and header controls while defaulting
  to UTF-8 with column headers enabled.

Secrets management with graceful fallback
-----------------------------------------

Use ``SecretManager`` to manage DuckDB secrets without assuming that the DuckDB
``secrets`` extension is always available. The manager persists records in a
connection-independent registry and mirrors them into DuckDB when possible.

.. code-block:: python

   from duckplus import SecretDefinition, SecretManager, connect

   with connect() as conn:
       secrets = SecretManager(conn)

       warehouse_secret = SecretDefinition(
           name="warehouse_creds",
           engine="postgres",
           parameters={
               "username": "warehouse_ro",
               "password": "not-a-real-secret",
               "host": "analytics.example.com",
           },
       )

       stored = secrets.create_secret(warehouse_secret, replace=True)
       print(stored)

       snapshot = secrets.list_secrets()
       print(snapshot)

       secrets.drop_secret("warehouse_creds")

       print(secrets.list_secrets())

.. code-block:: text

   SecretRecord(name='warehouse_creds', engine='postgres', parameters=(('username', 'warehouse_ro'), ('password', 'not-a-real-secret'), ('host', 'analytics.example.com')))
   [SecretRecord(name='warehouse_creds', engine='postgres', parameters=(('username', 'warehouse_ro'), ('password', 'not-a-real-secret'), ('host', 'analytics.example.com')))]
   []

Design considerations
~~~~~~~~~~~~~~~~~~~~~

* ``SecretDefinition`` validates identifiers (and keeps quoting rules) before
  persisting the record.
* ``SecretManager.ensure_extension`` attempts to ``LOAD secrets`` but silently
  downgrades to the in-memory registry when the extension is missing.
* Dropping a secret removes it from both Duck+ and DuckDB (when available), so
  subsequent calls fall back to the registry-only state until a new secret is
  created.

Materialization strategies and HTML previews
--------------------------------------------

For downstream integrations you often need multiple representations: Arrow
buffers, on-disk Parquet snapshots, HTML previews for dashboards, or even new
relations registered on the same connection. Combine ``materialize`` strategies
with ``duckplus.html.to_html`` to serve those needs.

.. code-block:: python

   from pathlib import Path
   from tempfile import TemporaryDirectory

   from duckplus import (
       ArrowMaterializeStrategy,
       ParquetMaterializeStrategy,
       DuckRel,
       connect,
   )
   from duckplus.html import to_html

   with TemporaryDirectory() as tmpdir:
       parquet_path = Path(tmpdir) / "product_snapshot.parquet"

       with connect() as conn:
           rel = DuckRel(
               conn.raw.sql(
                   """
                   SELECT *
                   FROM (VALUES
                       (1, 'Widget', 120.0),
                       (2, 'Gadget',  85.5),
                       (3, 'Doodad',  42.0)
                   ) AS t(product_id, name, price)
                   """
               )
           )

           arrow_materialized = rel.materialize(
               strategy=ArrowMaterializeStrategy(retain_table=True)
           )
           arrow_table = arrow_materialized.require_table()
           print(arrow_table.schema)

           parquet_materialized = rel.materialize(
               strategy=ParquetMaterializeStrategy(
                   path=parquet_path,
                   cleanup=False,
               ),
               into=conn.raw,
           )
           table_rel = parquet_materialized.require_relation()
           print(table_rel.columns)

           preview_html = to_html(rel, max_rows=10, null_display="∅", class_="table")
           print(preview_html)

.. code-block:: text

   product_id: int32
   name: string
   price: decimal128(4, 1)
   ['product_id', 'name', 'price']
   <table class="table"><thead><tr><th>product_id</th><th>name</th><th>price</th></tr></thead><tbody><tr><td>1</td><td>Widget</td><td>120.0</td></tr><tr><td>2</td><td>Gadget</td><td>85.5</td></tr><tr><td>3</td><td>Doodad</td><td>42.0</td></tr></tbody></table>

What to notice
~~~~~~~~~~~~~~

* ``ArrowMaterializeStrategy`` can retain the Arrow table for in-Python
  processing while avoiding round-trips through DuckDB for simple previews.
* ``ParquetMaterializeStrategy`` optionally writes to a provided path and can
  clean up temporary files automatically. When ``into`` is supplied the
  materialized Parquet dataset is registered as a new relation on the provided
  connection and wrapped back into ``DuckRel``.
* ``to_html`` escapes values inside DuckDB and adds a footer summarizing how
  many rows were omitted when ``max_rows`` limits the preview.

Command-line exploration
------------------------

The ``duckplus`` CLI offers a read-only SQL runner and schema inspector. Use
``uv`` to invoke it so you stay consistent with the project tooling.

.. code-block:: bash

   uv run duckplus sql "SELECT 42 AS answer"

   uv run duckplus schema "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)"

   printf 'SELECT 1 AS value;\n.exit\n' | uv run duckplus --repl

.. code-block:: text

   answer
   ------
   42
   (1 row)

   column | type
   -------+--------
   id     | INTEGER
   name   | VARCHAR
   (2 rows)

   duckplus> value
   -----
   1
   (1 row)
   duckplus>

CLI essentials
~~~~~~~~~~~~~~

* Subcommands operate in read-only mode. Opening a database path automatically
  toggles ``read_only`` on the underlying connection wrapper.
* ``duckplus sql`` streams up to ``--limit`` rows (default ``20``) using Arrow
  materialization under the hood and prints a textual table. Exceeding the limit
  prints a truncation notice so you know more data is available.
* ``duckplus schema`` renders column names and DuckDB type names, mirroring the
  strict projection semantics enforced by ``DuckRel``.

