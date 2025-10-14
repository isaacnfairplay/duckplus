Release 0.0.7 reliability demos
================================

The ``duckplus.examples.reliability_demos`` module collects aggressive,
production-minded guides that debuted with release ``0.0.7``. Each helper shows
how typed expressions, idempotent mutation helpers, and Arrow materialisation
combine to keep analytics pipelines short, safe, and predictable. The examples
are organised so you can copy them directly into orchestration jobs or unit
tests without reworking the structure.

Priority dispatch payload
-------------------------

High urgency alerts often need a strict ordering guarantee. This demo filters
on a Boolean priority flag *and* a revenue threshold, then reuses a shared typed
column dictionary so the projection stays concise while still returning a fully
typed payload.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: priority_dispatch_payload
   :language: python

Idempotent fact ingests
-----------------------

Reliable release pipelines demand idempotent writes. ``incremental_fact_ingest``
creates a fact table, seeds existing rows, and then calls
:meth:`duckplus.table.DuckTable.insert_antijoin` so only unseen order IDs land in
the destination. The helper returns both the number of inserted rows and a
typed snapshot to make assertions easy.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: incremental_fact_ingest
   :language: python

Customer spike detector
-----------------------

``customer_spike_detector`` demonstrates a lightweight quality gate. It computes
per-customer counts and maximum order totals, then filters using typed
expressions so only outsized purchases surface. The helper returns customers in
descending order of their peak value, ready for escalation workflows.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: customer_spike_detector
   :language: python

Reusable KPI rollups
--------------------

``regional_order_kpis`` highlights how a single dictionary of typed expressions
can drive multiple aggregates with different filters. The count of priority
orders uses :meth:`duckplus.AggregateExpression.count` with a filter expression,
showing how to keep the code terse without sacrificing type metadata.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: regional_order_kpis
   :language: python

Arrow cache payloads
--------------------

Sometimes the fastest path is to cache pre-filtered results. The
``arrow_priority_snapshot`` demo materialises a priority queue into an Arrow
table so downstream services can consume an in-memory payload without issuing
new SQL. The typed fetch ensures the Python consumer sees ``list[tuple[int, str]]``.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: arrow_priority_snapshot
   :language: python

Lean projections with typing intact
-----------------------------------

``lean_projection_shortcut`` demonstrates a small but powerful pattern: project
columns with shared typed expressions, apply a single SQL transform, and then
reapply the markers so :meth:`duckplus.DuckRel.fetch_typed` continues to return
precise annotations. The approach keeps projection code tight while making it
obvious how formatting tweaks propagate through the pipeline.

.. literalinclude:: ../../src/duckplus/examples/reliability_demos.py
   :pyobject: lean_projection_shortcut
   :language: python
