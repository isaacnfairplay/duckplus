Typed pipeline demos
====================

Typed column metadata unlocks schema-aware workflows end to end. The helpers in
:mod:`duckplus.examples.typed_pipeline_demos` show how projections, filters,
aggregates, and raw SQL adjustments propagate DuckDB type markers that power
``DuckRel.fetch_typed()``.

Sample data
-----------

The demos start with a small orders dataset. Each column is projected through
:func:`duckplus.col` so the relation stores DuckDB markers alongside the SQL
statement.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: typed_orders_demo_relation
   :language: python

Fetch typed snapshots
---------------------

Typed filters and ordering clauses keep metadata intact. When the relation is
materialised with :meth:`duckplus.DuckRel.fetch_typed`, the return type is a
``list`` of tuples with Python annotations derived from the stored DuckDB
markers.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: priority_order_snapshot
   :language: python

Grouped summaries with automatic typing
---------------------------------------

Aggregate helpers enforce compatible argument types and derive the output marker
for each projection. The resulting relation can be fetched without specifying
columns—the stored metadata controls both runtime validation and static typing.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: regional_revenue_summary
   :language: python

Unknown falls back when using manual SQL
---------------------------------------

Raw SQL transformations are still available. When a column is rewritten using a
string expression, the marker becomes :class:`duckplus.ducktypes.Unknown` so the
API signals that Python typing falls back to ``Any``.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: apply_manual_tax_projection
   :language: python

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: describe_markers
   :language: python

The tests in :mod:`tests.test_examples` execute these functions to ensure the
examples stay in sync with the codebase.
