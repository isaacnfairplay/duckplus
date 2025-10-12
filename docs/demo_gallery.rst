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

Fulfillment reconciliation stress test
--------------------------------------

This scenario pressure-tests right/full join semantics by reconciling bookings
against fulfillment events. It deliberately triggers duplicate-column
protection before recovering with suffix-aware projections and finally surfaces
problematic rows via ``split``.

.. code-block:: python

   from pprint import pprint

   from duckplus import DuckRel, JoinSpec, connect

   with connect() as conn:
       bookings = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('A-100', DATE '2024-01-01', 'widget', 4, 120.0, 'direct'),
                   ('A-101', DATE '2024-01-03', 'widget', 6, 180.0, 'marketplace'),
                   ('A-102', DATE '2024-01-05', 'gadget', 2, 250.0, 'direct'),
                   ('A-103', DATE '2024-01-07', 'widget', 1, 30.0, 'partner')
               ) AS t(reservation_id, booked_on, sku, quantity, unit_price, channel)
               """
           )
       )

       fulfillments = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('A-100', DATE '2024-01-02', 'widget', 4, 120.0, 'DC-1'),
                   ('A-101', DATE '2024-01-06', 'widget', 5, 180.0, 'DC-3'),
                   ('A-104', DATE '2024-01-08', 'widget', 2, 40.0, 'DC-2')
               ) AS t(reservation_id, fulfilled_on, sku, quantity, unit_price, facility)
               """
           )
       )

       spec = JoinSpec(equal_keys=[("reservation_id", "reservation_id")])
       try:
           bookings.inner_join(fulfillments, spec)
       except ValueError as exc:
           print(f"Explicit join collision: {exc}")

       audit = bookings.natural_full(
           fulfillments,
           allow_collisions=True,
           suffixes=("_booking", "_ship"),
       )

       audit = audit.add_columns(
           channel_mismatch=(
               "CASE WHEN facility IS NOT NULL "
               "AND channel IS DISTINCT FROM facility THEN 1 ELSE 0 END"
           ),
           fulfillment_gap=(
               "CASE WHEN fulfilled_on IS NULL THEN 'missing shipment' "
               "WHEN booked_on IS NULL THEN 'untracked return' ELSE NULL END"
           ),
           qty_delta="COALESCE(quantity_booking, 0) - COALESCE(quantity_ship, 0)",
       )

       problems, clean = audit.split(
           "channel_mismatch = 1 OR fulfillment_gap IS NOT NULL OR qty_delta <> 0"
       )

       print("Problems:")
       pprint(
           problems.order_by(reservation_id_booking="asc").materialize().require_table().to_pylist()
       )
       print("Clean:")
       pprint(
           clean.order_by(reservation_id_booking="asc").materialize().require_table().to_pylist()
       )

.. code-block:: text

   Explicit join collision: Join would produce duplicate columns: quantity, sku, unit_price
   Problems:
   [{'booked_on': datetime.date(2024, 1, 1),
     'channel': 'direct',
     'channel_mismatch': 1,
     'facility': 'DC-1',
     'fulfilled_on': datetime.date(2024, 1, 2),
     'fulfillment_gap': None,
     'qty_delta': 0,
     'quantity_booking': 4,
     'quantity_ship': 4,
     'reservation_id_booking': 'A-100',
     'reservation_id_ship': 'A-100',
     'sku_booking': 'widget',
     'sku_ship': 'widget',
     'unit_price_booking': Decimal('120.0'),
     'unit_price_ship': Decimal('120.0')},
    {'booked_on': datetime.date(2024, 1, 3),
     'channel': 'marketplace',
     'channel_mismatch': 0,
     'facility': None,
     'fulfilled_on': None,
     'fulfillment_gap': 'missing shipment',
     'qty_delta': 6,
     'quantity_booking': 6,
     'quantity_ship': None,
     'reservation_id_booking': 'A-101',
     'reservation_id_ship': None,
     'sku_booking': 'widget',
     'sku_ship': None,
     'unit_price_booking': Decimal('180.0'),
     'unit_price_ship': None},
    {'booked_on': datetime.date(2024, 1, 5),
     'channel': 'direct',
     'channel_mismatch': 0,
     'facility': None,
     'fulfilled_on': None,
     'fulfillment_gap': 'missing shipment',
     'qty_delta': 2,
     'quantity_booking': 2,
     'quantity_ship': None,
     'reservation_id_booking': 'A-102',
     'reservation_id_ship': None,
     'sku_booking': 'gadget',
     'sku_ship': None,
     'unit_price_booking': Decimal('250.0'),
     'unit_price_ship': None},
    {'booked_on': datetime.date(2024, 1, 7),
     'channel': 'partner',
     'channel_mismatch': 0,
     'facility': None,
     'fulfilled_on': None,
     'fulfillment_gap': 'missing shipment',
     'qty_delta': 1,
     'quantity_booking': 1,
     'quantity_ship': None,
     'reservation_id_booking': 'A-103',
     'reservation_id_ship': None,
     'sku_booking': 'widget',
     'sku_ship': None,
     'unit_price_booking': Decimal('30.0'),
     'unit_price_ship': None},
    {'booked_on': None,
     'channel': None,
     'channel_mismatch': 1,
     'facility': 'DC-2',
     'fulfilled_on': datetime.date(2024, 1, 8),
     'fulfillment_gap': 'untracked return',
     'qty_delta': -2,
     'quantity_booking': None,
     'quantity_ship': 2,
     'reservation_id_booking': None,
     'reservation_id_ship': 'A-104',
     'sku_booking': None,
     'sku_ship': 'widget',
     'unit_price_booking': None,
     'unit_price_ship': Decimal('40.0')},
    {'booked_on': None,
     'channel': None,
     'channel_mismatch': 1,
     'facility': 'DC-3',
     'fulfilled_on': datetime.date(2024, 1, 6),
     'fulfillment_gap': 'untracked return',
     'qty_delta': -5,
     'quantity_booking': None,
     'quantity_ship': 5,
     'reservation_id_booking': None,
     'reservation_id_ship': 'A-101',
     'sku_booking': None,
     'sku_ship': 'widget',
     'unit_price_booking': None,
     'unit_price_ship': Decimal('180.0')}]
   Clean:
   []

Observed pitfalls
~~~~~~~~~~~~~~~~~

* ``JoinSpec``-driven joins require explicit collision handling—omitting suffixes
  raises ``ValueError`` once non-key payload columns overlap.
* ``natural_full`` retains right-hand keys only when collisions are permitted;
  forgetting to opt in will still surface the guardrail shown above.

Sensor telemetry triage with anomaly fan-out
-------------------------------------------

This pipeline joins live sensor events with partition metadata, calibration
windows, and noisy readings. It highlights partitioned joins and semi joins,
and demonstrates how ``insert_by_continuous_id`` can silently drop out-of-order
rows when device feeds rewind.

.. code-block:: python

   from pprint import pprint

   from duckplus import (
       AsofOrder,
       ColumnPredicate,
       DuckRel,
       DuckTable,
       JoinProjection,
       JoinSpec,
       PartitionSpec,
       connect,
   )

   with connect() as conn:
       conn.raw.execute(
           "CREATE TABLE sensor_events("
           "event_id INTEGER, device_id INTEGER, observed_at TIMESTAMP, "
           "temperature DOUBLE, status VARCHAR)"
       )
       table = DuckTable(conn, "sensor_events")

       seed = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (1001, 11, TIMESTAMP '2024-02-01 08:00:00', 68.5, 'ok'),
                   (1002, 11, TIMESTAMP '2024-02-01 09:00:00', 72.0, 'ok'),
                   (1003, 12, TIMESTAMP '2024-02-01 09:30:00', 89.1, 'degraded')
               ) AS t(event_id, device_id, observed_at, temperature, status)
               """
           )
       )
       table.append(seed)

       calibrations = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (11, TIMESTAMP '2024-02-01 07:55:00', 'stable'),
                   (11, TIMESTAMP '2024-02-01 08:45:00', 'drifting'),
                   (12, TIMESTAMP '2024-02-01 09:15:00', 'stable'),
                   (12, TIMESTAMP '2024-02-01 10:00:00', 'offline')
               ) AS t(device_id, calibration_ts, disposition)
               """
           )
       )

       partitions = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (11, 'north', 'critical'),
                   (12, 'west', 'critical'),
                   (13, 'west', 'experimental')
               ) AS t(device_id, location, tier)
               """
           )
       )

       noisy = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (11, TIMESTAMP '2024-02-01 08:05:00', 69.0),
                   (11, TIMESTAMP '2024-02-01 09:10:00', 74.5),
                   (12, TIMESTAMP '2024-02-01 09:40:00', 91.2),
                   (13, TIMESTAMP '2024-02-01 11:00:00', 65.0)
               ) AS t(device_id, observed_at, reading)
               """
           )
       )

       decorated = (
           DuckRel(conn.raw.table("sensor_events"))
           .natural_left(partitions, allow_collisions=True, suffixes=("", "_partition"))
           .natural_asof(
               calibrations,
               order=AsofOrder(left="observed_at", right="calibration_ts"),
               suffixes=("", "_calibration"),
           )
       )

       print("Decorated events:")
       pprint(decorated.materialize().require_table().to_pylist())

       join_spec = JoinSpec(
           equal_keys=[("device_id", "device_id")],
           predicates=[ColumnPredicate("observed_at", ">", "observed_at")],
       )
       anomaly_candidates = noisy.partitioned_inner(
           DuckRel(conn.raw.table("sensor_events")),
           PartitionSpec.from_mapping({"device_id": "device_id"}),
           join_spec,
           project=JoinProjection(allow_collisions=True, suffixes=("_noise", "_event")),
       )

       print("Anomaly candidates:")
       pprint(
           anomaly_candidates
           .order_by(device_id="asc", observed_at_noise="asc")
           .materialize()
           .require_table()
           .to_pylist()
       )

       flagged = DuckRel(conn.raw.table("sensor_events")).semi_join(
           anomaly_candidates.project_columns("event_id"),
           event_id="event_id",
       )
       print("Flagged events via semi join:")
       pprint(flagged.materialize().require_table().to_pylist())

       incoming = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (998, 13, TIMESTAMP '2024-02-01 07:00:00', 66.0, 'ok'),
                   (1004, 12, TIMESTAMP '2024-02-01 10:05:00', 92.0, 'critical'),
                   (1005, 11, TIMESTAMP '2024-02-01 10:10:00', 75.0, 'ok')
               ) AS t(event_id, device_id, observed_at, temperature, status)
               """
           )
       )

       inserted = table.insert_by_continuous_id(incoming, id_column="event_id")
       print(f"Inserted rows by continuous id: {inserted}")

       snapshot = DuckRel(conn.raw.table("sensor_events"))
       print("Snapshot after insert_by_continuous_id:")
       pprint(snapshot.order_by(event_id="asc").materialize().require_table().to_pylist())

       dropped = incoming.anti_join(snapshot, event_id="event_id")
       print("Rows dropped by ID guard:")
       pprint(dropped.materialize().require_table().to_pylist())

.. code-block:: text

   Decorated events:
   [{'calibration_ts': datetime.datetime(2024, 2, 1, 7, 55),
     'device_id': 11,
     'disposition': 'stable',
     'event_id': 1001,
     'location': 'north',
     'observed_at': datetime.datetime(2024, 2, 1, 8, 0),
     'status': 'ok',
     'temperature': 68.5,
     'tier': 'critical'},
    {'calibration_ts': datetime.datetime(2024, 2, 1, 8, 45),
     'device_id': 11,
     'disposition': 'drifting',
     'event_id': 1002,
     'location': 'north',
     'observed_at': datetime.datetime(2024, 2, 1, 9, 0),
     'status': 'ok',
     'temperature': 72.0,
     'tier': 'critical'},
    {'calibration_ts': datetime.datetime(2024, 2, 1, 9, 15),
     'device_id': 12,
     'disposition': 'stable',
     'event_id': 1003,
     'location': 'west',
     'observed_at': datetime.datetime(2024, 2, 1, 9, 30),
     'status': 'degraded',
     'temperature': 89.1,
     'tier': 'critical'}]
   Anomaly candidates:
   [{'device_id': 11,
     'event_id': 1001,
     'observed_at_event': datetime.datetime(2024, 2, 1, 8, 0),
     'observed_at_noise': datetime.datetime(2024, 2, 1, 8, 5),
     'reading': Decimal('69.0'),
     'status': 'ok',
     'temperature': 68.5},
    {'device_id': 11,
     'event_id': 1001,
     'observed_at_event': datetime.datetime(2024, 2, 1, 8, 0),
     'observed_at_noise': datetime.datetime(2024, 2, 1, 9, 10),
     'reading': Decimal('74.5'),
     'status': 'ok',
     'temperature': 68.5},
    {'device_id': 11,
     'event_id': 1002,
     'observed_at_event': datetime.datetime(2024, 2, 1, 9, 0),
     'observed_at_noise': datetime.datetime(2024, 2, 1, 9, 10),
     'reading': Decimal('74.5'),
     'status': 'ok',
     'temperature': 72.0},
    {'device_id': 12,
     'event_id': 1003,
     'observed_at_event': datetime.datetime(2024, 2, 1, 9, 30),
     'observed_at_noise': datetime.datetime(2024, 2, 1, 9, 40),
     'reading': Decimal('91.2'),
     'status': 'degraded',
     'temperature': 89.1}]
   Flagged events via semi join:
   [{'device_id': 11,
     'event_id': 1001,
     'observed_at': datetime.datetime(2024, 2, 1, 8, 0),
     'status': 'ok',
     'temperature': 68.5},
    {'device_id': 11,
     'event_id': 1002,
     'observed_at': datetime.datetime(2024, 2, 1, 9, 0),
     'status': 'ok',
     'temperature': 72.0},
    {'device_id': 12,
     'event_id': 1003,
     'observed_at': datetime.datetime(2024, 2, 1, 9, 30),
     'status': 'degraded',
     'temperature': 89.1}]
   Inserted rows by continuous id: 2
   Snapshot after insert_by_continuous_id:
   [{'device_id': 11,
     'event_id': 1001,
     'observed_at': datetime.datetime(2024, 2, 1, 8, 0),
     'status': 'ok',
     'temperature': 68.5},
    {'device_id': 11,
     'event_id': 1002,
     'observed_at': datetime.datetime(2024, 2, 1, 9, 0),
     'status': 'ok',
     'temperature': 72.0},
    {'device_id': 12,
     'event_id': 1003,
     'observed_at': datetime.datetime(2024, 2, 1, 9, 30),
     'status': 'degraded',
     'temperature': 89.1},
    {'device_id': 12,
     'event_id': 1004,
     'observed_at': datetime.datetime(2024, 2, 1, 10, 5),
     'status': 'critical',
     'temperature': 92.0},
    {'device_id': 11,
     'event_id': 1005,
     'observed_at': datetime.datetime(2024, 2, 1, 10, 10),
     'status': 'ok',
     'temperature': 75.0}]
   Rows dropped by ID guard:
   [{'device_id': 13,
     'event_id': 998,
     'observed_at': datetime.datetime(2024, 2, 1, 7, 0),
     'status': 'ok',
     'temperature': Decimal('66.0')}]

What broke or looked risky
~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``semi_join`` strips right-hand payload columns, so post-join diagnostics must
  materialize the earlier relation (``anomaly_candidates``) to understand why a
  row was flagged.
* ``insert_by_continuous_id`` quietly discards events whose IDs fall behind the
  table maximum—any upstream rewind or re-ordering needs a different ingest
  strategy to avoid data loss.

Demand fulfillment triage with partitioned joins
------------------------------------------------

When nightly planning uncovers fulfillment risks, planners often need to
cross-check demand, shipment history, forecasts, and vendor promises in one
place. This scenario stitches those data sources together and shows how
partitioned joins, ASOF lookups, and right joins now preserve the supplier
identifiers even when a receipt never arrived.

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
       demand = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (1001, 'SKU-1', 'North', DATE '2024-03-01', 50),
                   (1002, 'SKU-1', 'North', DATE '2024-03-05', 20),
                   (1003, 'SKU-2', 'West', DATE '2024-03-03', 35),
                   (1004, 'SKU-3', 'South', DATE '2024-03-02', 60)
               ) AS t(demand_id, sku, region, requested_date, needed_units)
               """
           )
       )

       shipments = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (5001, 'SKU-1', 'North', DATE '2024-02-28', 40),
                   (5002, 'SKU-1', 'North', DATE '2024-03-04', 30),
                   (5003, 'SKU-2', 'West', DATE '2024-03-01', 20),
                   (5004, 'SKU-3', 'South', DATE '2024-02-25', 70)
               ) AS t(shipment_id, sku, region, shipped_date, shipped_units)
               """
           )
       )

       forecasts = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('SKU-1', 'North', DATE '2024-02-27', 55),
                   ('SKU-1', 'North', DATE '2024-03-02', 60),
                   ('SKU-2', 'West', DATE '2024-02-25', 45),
                   ('SKU-3', 'South', DATE '2024-02-20', 90)
               ) AS t(sku, region, forecast_date, projected_units)
               """
           )
       )

       vendor_promises = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('SUP-1', 701, 'SKU-1', 'North', DATE '2024-02-26', 60),
                   ('SUP-2', 702, 'SKU-2', 'West', DATE '2024-02-28', 40),
                   ('SUP-3', 703, 'SKU-3', 'South', DATE '2024-02-27', 70),
                   ('SUP-4', 704, 'SKU-4', 'East', DATE '2024-02-25', 25)
               ) AS t(vendor_id, promise_id, sku, region, promise_date, promised_units)
               """
           )
       )

       receipts = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('SUP-1', 701, DATE '2024-02-27', 55),
                   ('SUP-2', 702, DATE '2024-02-29', 38)
               ) AS t(vendor_id, promise_id, receipt_date, received_units)
               """
           )
       )

       partition_review = demand.inspect_partitions(
           shipments, PartitionSpec.of_columns('sku', 'region')
       ).order_by(sku='asc', region='asc')
       pprint(partition_review.materialize().require_table().to_pylist())

       spec = JoinSpec(
           equal_keys=[('sku', 'sku'), ('region', 'region')],
           predicates=[ColumnPredicate('requested_date', '>=', 'shipped_date')],
       )
       staged = demand.partitioned_inner(
           shipments,
           PartitionSpec.of_columns('sku', 'region'),
           spec,
       ).order_by(requested_date='asc', demand_id='asc', shipment_id='asc')
       pprint(staged.materialize().require_table().to_pylist())

       urgent, regular = staged.split('"needed_units" >= ?', 40)
       pprint(urgent.order_by(requested_date='asc').materialize().require_table().to_pylist())
       pprint(regular.order_by(requested_date='asc').materialize().require_table().to_pylist())

       coverage = urgent.natural_asof(
           forecasts,
           order=AsofOrder(left='requested_date', right='forecast_date'),
           tolerance='30 days',
           allow_collisions=True,
           suffixes=('_demand', '_forecast'),
       ).order_by(requested_date='asc', demand_id='asc')
       pprint(coverage.materialize().require_table().to_pylist())

       promise_spec = JoinSpec(equal_keys=[('vendor_id', 'vendor_id'), ('promise_id', 'promise_id')])
       unresolved = receipts.left_right(
           vendor_promises,
           promise_spec,
           project=JoinProjection(allow_collisions=True, suffixes=('_receipt', '_promise')),
       ).order_by(promise_id_promise='asc')
       pprint(unresolved.materialize().require_table().to_pylist())

.. code-block:: text

   Partition overview:
   [{'left_count': 2, 'pair_count': 4, 'region': 'North', 'right_count': 2, 'shared_partition': True, 'sku': 'SKU-1'},
    {'left_count': 1, 'pair_count': 1, 'region': 'South', 'right_count': 1, 'shared_partition': True, 'sku': 'SKU-3'},
    {'left_count': 1, 'pair_count': 1, 'region': 'West', 'right_count': 1, 'shared_partition': True, 'sku': 'SKU-2'}]

   Staged matches:
   [{'demand_id': 1001, 'needed_units': 50, 'region': 'North', 'requested_date': datetime.date(2024, 3, 1),
     'shipment_id': 5001, 'shipped_date': datetime.date(2024, 2, 28), 'shipped_units': 40, 'sku': 'SKU-1'},
    {'demand_id': 1004, 'needed_units': 60, 'region': 'South', 'requested_date': datetime.date(2024, 3, 2),
     'shipment_id': 5004, 'shipped_date': datetime.date(2024, 2, 25), 'shipped_units': 70, 'sku': 'SKU-3'},
    {'demand_id': 1003, 'needed_units': 35, 'region': 'West', 'requested_date': datetime.date(2024, 3, 3),
     'shipment_id': 5003, 'shipped_date': datetime.date(2024, 3, 1), 'shipped_units': 20, 'sku': 'SKU-2'},
    {'demand_id': 1002, 'needed_units': 20, 'region': 'North', 'requested_date': datetime.date(2024, 3, 5),
     'shipment_id': 5001, 'shipped_date': datetime.date(2024, 2, 28), 'shipped_units': 40, 'sku': 'SKU-1'},
    {'demand_id': 1002, 'needed_units': 20, 'region': 'North', 'requested_date': datetime.date(2024, 3, 5),
     'shipment_id': 5002, 'shipped_date': datetime.date(2024, 3, 4), 'shipped_units': 30, 'sku': 'SKU-1'}]

   Urgent demand:
   [{'demand_id': 1001, 'needed_units': 50, 'region': 'North', 'requested_date': datetime.date(2024, 3, 1),
     'shipment_id': 5001, 'shipped_date': datetime.date(2024, 2, 28), 'shipped_units': 40, 'sku': 'SKU-1'},
    {'demand_id': 1004, 'needed_units': 60, 'region': 'South', 'requested_date': datetime.date(2024, 3, 2),
     'shipment_id': 5004, 'shipped_date': datetime.date(2024, 2, 25), 'shipped_units': 70, 'sku': 'SKU-3'}]

   Regular demand:
   [{'demand_id': 1003, 'needed_units': 35, 'region': 'West', 'requested_date': datetime.date(2024, 3, 3),
     'shipment_id': 5003, 'shipped_date': datetime.date(2024, 3, 1), 'shipped_units': 20, 'sku': 'SKU-2'},
    {'demand_id': 1002, 'needed_units': 20, 'region': 'North', 'requested_date': datetime.date(2024, 3, 5),
     'shipment_id': 5001, 'shipped_date': datetime.date(2024, 2, 28), 'shipped_units': 40, 'sku': 'SKU-1'},
    {'demand_id': 1002, 'needed_units': 20, 'region': 'North', 'requested_date': datetime.date(2024, 3, 5),
     'shipment_id': 5002, 'shipped_date': datetime.date(2024, 3, 4), 'shipped_units': 30, 'sku': 'SKU-1'}]

   Coverage snapshot:
   [{'demand_id': 1001, 'forecast_date': datetime.date(2024, 2, 27), 'needed_units': 50, 'projected_units': 55,
     'region': 'North', 'requested_date': datetime.date(2024, 3, 1), 'shipment_id': 5001,
     'shipped_date': datetime.date(2024, 2, 28), 'shipped_units': 40, 'sku': 'SKU-1'},
    {'demand_id': 1004, 'forecast_date': datetime.date(2024, 2, 20), 'needed_units': 60, 'projected_units': 90,
     'region': 'South', 'requested_date': datetime.date(2024, 3, 2), 'shipment_id': 5004,
     'shipped_date': datetime.date(2024, 2, 25), 'shipped_units': 70, 'sku': 'SKU-3'}]

   Receipt audit:
   [{'promise_date': datetime.date(2024, 2, 26), 'promise_id_promise': 701, 'promise_id_receipt': 701,
     'promised_units': 60, 'receipt_date': datetime.date(2024, 2, 27), 'received_units': 55,
     'region': 'North', 'sku': 'SKU-1', 'vendor_id_promise': 'SUP-1', 'vendor_id_receipt': 'SUP-1'},
    {'promise_date': datetime.date(2024, 2, 28), 'promise_id_promise': 702, 'promise_id_receipt': 702,
     'promised_units': 40, 'receipt_date': datetime.date(2024, 2, 29), 'received_units': 38,
     'region': 'West', 'sku': 'SKU-2', 'vendor_id_promise': 'SUP-2', 'vendor_id_receipt': 'SUP-2'},
    {'promise_date': datetime.date(2024, 2, 27), 'promise_id_promise': 703, 'promise_id_receipt': None,
     'promised_units': 70, 'receipt_date': None, 'received_units': None,
     'region': 'South', 'sku': 'SKU-3', 'vendor_id_promise': 'SUP-3', 'vendor_id_receipt': None},
    {'promise_date': datetime.date(2024, 2, 25), 'promise_id_promise': 704, 'promise_id_receipt': None,
     'promised_units': 25, 'receipt_date': None, 'received_units': None,
     'region': 'East', 'sku': 'SKU-4', 'vendor_id_promise': 'SUP-4', 'vendor_id_receipt': None}]

Key diagnostics
~~~~~~~~~~~~~~~

* ``inspect_partitions`` quickly surfaces skewed demand/shipments segments,
  letting planners focus remediation where pair counts explode.
* ``partitioned_inner`` coordinates partition and predicate logic so a single
  ``JoinSpec`` can gate late shipments without materializing interim tables.
* ``natural_asof`` contextualizes urgent orders with the latest forecast while
  retaining a deterministic projection of the involved columns.
* ``left_right`` joins now keep the supplier identifiers from the right-hand
  side even when the receipt never materializes, making escalation workflows far
  easier because promise IDs no longer disappear into ``NULL`` placeholders.

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

Chargeback exposure drill-down
------------------------------

This scenario simulates a finance operations investigation where stray
chargebacks are showing up without matching shipments. The right join uses
custom suffixes so late-arriving payment identifiers stay visible, and the
follow-on joins verify the unresolved payments against chargeback tickets and
service-level windows.

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
       shipments = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (5001, 'Acme', DATE '2024-02-02', 1200, 'EXP-11'),
                   (5002, 'Acme', DATE '2024-02-04', 700, 'EXP-12'),
                   (5003, 'Globex', DATE '2024-02-06', 650, 'EXP-13')
               ) AS t(invoice_id, customer_ref, ship_date, amount, export_batch)
               """
           )
       )
       payments = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   (5001, 'Acme', 'P-100', DATE '2024-02-05', 1200),
                   (5003, 'Globex', 'P-101', DATE '2024-02-09', 650),
                   (5004, 'Acme', 'P-102', DATE '2024-02-07', 400)
               ) AS t(invoice_id, customer_ref, payment_id, payment_date, amount)
               """
           )
       )

       review = shipments.inspect_partitions(
           payments,
           PartitionSpec.from_mapping({"customer_ref": "customer_ref"})
       )
       pprint(review.materialize().require_table().to_pylist())

       spec = JoinSpec(
           equal_keys=[("invoice_id", "invoice_id")],
           predicates=[ColumnPredicate("ship_date", "<=", "payment_date")],
       )
       recon = shipments.left_right(
           payments,
           spec,
           project=JoinProjection(suffixes=("_ship", "_pay")),
       )

       exposures = recon.project(
           {
               "invoice_id_ship": '"invoice_id_ship"',
               "invoice_id_pay": '"invoice_id_pay"',
               "customer_ref_ship": '"customer_ref_ship"',
               "customer_ref_pay": '"customer_ref_pay"',
               "ship_date_ship": '"ship_date"',
               "payment_date_pay": '"payment_date"',
               "amount_ship": '"amount_ship"',
               "amount_pay": '"amount_pay"',
               "payment_id_pay": '"payment_id"',
               "export_batch": '"export_batch"',
               "is_missing_shipment": '"invoice_id_ship" IS NULL',
               "is_late_payment": '"invoice_id_ship" IS NOT NULL AND "payment_date" > "ship_date" + INTERVAL 3 DAY',
           }
       )
       exposures = exposures.filter(
           '"is_missing_shipment" OR "is_late_payment"'
       )
       pprint(exposures.materialize().require_table().to_pylist())

       chargebacks = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('Acme', 5004, DATE '2024-02-10', 'CB-900'),
                   ('Acme', 5002, DATE '2024-02-08', 'CB-901')
               ) AS t(customer_ref, invoice_id, opened_at, ticket_id)
               """
           )
       )

       escalations = exposures.partitioned_inner(
           chargebacks,
           PartitionSpec.from_mapping({"customer_ref_pay": "customer_ref"}),
           JoinSpec(equal_keys=[("invoice_id_pay", "invoice_id")]),
           project=JoinProjection(suffixes=("_case", "_cb")),
       )

       sla_windows = DuckRel(
           conn.raw.sql(
               """
               SELECT *
               FROM (VALUES
                   ('Acme', DATE '2024-01-01', 2),
                   ('Acme', DATE '2024-02-09', 5),
                   ('Globex', DATE '2024-02-01', 4)
               ) AS t(customer_ref, effective_at, grace_days)
               """
           )
       )

       normalized = escalations.project(
           {
               "invoice_id_pay": '"invoice_id_pay"',
               "customer_ref": 'COALESCE("customer_ref_pay", "customer_ref_ship")',
               "payment_date_pay": '"payment_date_pay"',
               "ticket_id": '"ticket_id"',
               "opened_at": '"opened_at"',
               "is_missing_shipment": '"is_missing_shipment"',
               "is_late_payment": '"is_late_payment"',
           }
       )

       with_sla = normalized.natural_asof(
           sla_windows,
           order=AsofOrder(left="payment_date_pay", right="effective_at"),
           suffixes=("_case", "_sla"),
       )
       pprint(with_sla.materialize().require_table().to_pylist())

.. code-block:: text

   [{'customer_ref': 'Acme',
     'left_count': 2,
     'pair_count': 4,
     'right_count': 2,
     'shared_partition': True},
    {'customer_ref': 'Globex',
     'left_count': 1,
     'pair_count': 1,
     'right_count': 1,
     'shared_partition': True}]

   [{'amount_pay': 400,
     'amount_ship': None,
     'customer_ref_pay': 'Acme',
     'customer_ref_ship': None,
     'export_batch': None,
     'invoice_id_pay': 5004,
     'invoice_id_ship': None,
     'is_late_payment': False,
     'is_missing_shipment': True,
     'payment_date_pay': datetime.date(2024, 2, 7),
     'payment_id_pay': 'P-102',
     'ship_date_ship': None}]

   [{'customer_ref': 'Acme',
     'effective_at': datetime.date(2024, 1, 1),
     'grace_days': 2,
     'invoice_id_pay': 5004,
     'is_late_payment': False,
     'is_missing_shipment': True,
     'opened_at': datetime.date(2024, 2, 10),
     'payment_date_pay': datetime.date(2024, 2, 7),
     'ticket_id': 'CB-900'}]

Key takeaways
~~~~~~~~~~~~~

* ``JoinProjection(suffixes=("_ship", "_pay"))`` keeps both the shipment and
  payment identifiers visible so delayed payments do not hide their right-side
  keys during reconciliation.
* ``PartitionSpec.from_mapping`` lets the right join reuse the customer
  partitioning when chasing chargebacks, proving that the partition counts
  align before the expensive match.
* ``natural_asof`` enriches the escalated cases with the most recent SLA window
  so follow-up workflows can prioritize the tickets that are already out of
  compliance.

CLI essentials
~~~~~~~~~~~~~~

* Subcommands operate in read-only mode. Opening a database path automatically
  toggles ``read_only`` on the underlying connection wrapper.
* ``duckplus sql`` streams up to ``--limit`` rows (default ``20``) using Arrow
  materialization under the hood and prints a textual table. Exceeding the limit
  prints a truncation notice so you know more data is available.
* ``duckplus schema`` renders column names and DuckDB type names, mirroring the
  strict projection semantics enforced by ``DuckRel``.

