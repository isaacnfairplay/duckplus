Demo walkthroughs
=================

The following walkthroughs combine the primitives documented in this reference
so teams can visualize how the API maps onto everyday tasks without relying on
additional helpers.

Build a transformation pipeline
-------------------------------

This example chains ``Relation`` helpers to prepare a transformed view before
materializing it for downstream use.

.. code-block:: python

   from pathlib import Path

   from duckplus import connect

   with connect() as conn:
       # Load two datasets from disk into immutable Relation wrappers.
       staging = conn.read_parquet([Path("/data/staging_orders.parquet")])
       reference = conn.read_csv([Path("/data/customer_lookup.csv")])

       columns = staging.columns

       enriched = (
           staging
           # Cast total to a DECIMAL column for downstream precision.
           .cast_columns(total="DECIMAL(18,2)")
           # Join on shared customer_id while tolerating extra right-side columns.
           .natural_left(reference, allow_collisions=True)
           # Filter to shipped orders in the current quarter with typed columns.
           .where(columns.status == "SHIPPED")
           .where(columns.ship_date >= columns.literal("2024-01-01"))
           .order_by(columns.order_id.asc())
           .limit(1000)
       )

       # Spill the relation to an Arrow table for analytics clients.
       arrow_snapshot = enriched.materialize().require_table()

- ``DuckConnection.read_parquet`` and ``DuckConnection.read_csv`` validate paths
  and wrap the resulting relations in ``Relation`` for further
  composition.【F:src/duckplus/connect.py†L85-L143】【F:src/duckplus/io.py†L680-L812】
- ``cast_columns``, ``natural_left``, ``where``, ``order_by``, and ``limit`` each
  return a new ``Relation``, ensuring the pipeline stays immutable and
  case-aware.【F:src/duckplus/core.py†L533-L807】
- ``materialize()`` defaults to the Arrow strategy and ensures the resulting
  table can be reused without mutating the original
  relation.【F:src/duckplus/core.py†L844-L904】【F:src/duckplus/materialize.py†L21-L55】

Append only unseen rows into a fact table
-----------------------------------------

Mutable table helpers complement the immutable pipeline by enforcing explicit
ingestion semantics.

.. code-block:: python

   from duckplus import DuckTable, connect

   with connect("warehouse.duckdb") as conn:
       fact_orders = DuckTable(conn, "analytics.fact_orders")
       staging = conn.read_parquet([Path("/loads/fact_orders_delta.parquet")])

       inserted = fact_orders.insert_antijoin(staging, keys=["order_id"])
       print(f"Inserted {inserted} new rows")

- ``DuckTable`` validates the dotted identifier without changing casing, keeping
  schema ownership explicit.【F:src/duckplus/table.py†L1-L76】
- ``insert_antijoin`` performs a case-aware anti join using the provided keys and
  returns the number of appended rows for
  observability.【F:src/duckplus/table.py†L113-L194】
