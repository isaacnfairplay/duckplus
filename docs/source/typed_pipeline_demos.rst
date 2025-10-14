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

Schema-driven projections
-------------------------

Schema metadata can drive downstream projections without reaching back to the
database. ``DuckRel.schema`` exposes a :class:`duckplus.DuckSchema` that stores
canonical column names, DuckDB markers, and Python annotations. The demo below
selects a case-insensitive subset of columns and fetches typed rows using only
the cached metadata.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: schema_driven_projection
   :language: python

Grouped summaries with automatic typing
---------------------------------------

Aggregate helpers enforce compatible argument types and derive the output marker
for each projection. The resulting relation can be fetched without specifying
columns—the stored metadata controls both runtime validation and static typing.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: regional_revenue_summary
   :language: python

Priority rollups with filtered aggregates
-----------------------------------------

The schema-aware projections power richer aggregates as well. ``priority_region_rollup``
filters on typed boolean columns, counts high-value orders, and sums item
quantities while preserving marker metadata for the resulting relation.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: priority_region_rollup
   :language: python

Customer lifetime and priority profiles
---------------------------------------

``customer_priority_profile`` combines minimum, sum, and filtered count
aggregates. The schema ensures comparisons and numeric operations are valid,
and the resulting tuples carry :class:`datetime.date` annotations alongside the
integer metrics.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: customer_priority_profile
   :language: python

Distinct customers per region
-----------------------------

``regional_customer_diversity`` demonstrates distinct counts with and without
filters. The helper reuses schema-derived expressions so both ``COUNT`` calls
validate compatible column types before DuckDB evaluates the query.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: regional_customer_diversity
   :language: python

Daily priority summaries
------------------------

``daily_priority_summary`` builds per-day metrics that track revenue and flagged
orders. Even with filtered aggregates the resulting relation remains fully
typed, so ``fetch_typed()`` produces ``date`` and ``int`` annotations without
manual casts.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: daily_priority_summary
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
   :pyobject: describe_schema
   :language: python

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: describe_markers
   :language: python

The tests in :mod:`tests.test_examples` execute these functions to ensure the
examples stay in sync with the codebase.
