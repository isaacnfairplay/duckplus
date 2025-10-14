Release 0.0.11 typed schema workflows
=====================================

Release ``0.0.11`` finishes migrating DuckRel to the shared
:class:`duckplus.DuckSchema` interface introduced during the typed API overhaul.
Relations now expose a case-insensitive schema cache that drives both runtime
marker validation and static type preservation. The refreshed demos show how to
query that metadata directly, reuse it inside projections and aggregates, and
export the structure for documentation or assertions.

Schema-driven metadata
----------------------

``DuckRel.schema`` grants direct access to every column definition, including the
resolved DuckDB marker and any stored Python annotations. The
``describe_schema`` helper turns that cache into a readable summary so pipeline
code and notebooks can confirm the metadata that will ship with a release.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: describe_schema
   :language: python

Typed rollups without redeclaring markers
-----------------------------------------

The new ``priority_region_rollup`` demo demonstrates how to build grouped
aggregates without repeating column declarations. By deriving expressions from
``DuckRel.schema`` each aggregate stays short while returning typed results that
keep marker information intact.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: priority_region_rollup
   :language: python

Schema-first projections
------------------------

``schema_driven_projection`` highlights how to reuse the schema cache for
projection dictionaries. The helper selects the desired definitions and converts
those into typed column expressions automatically, keeping the projection in
lockstep with the schema while still returning precise Python annotations.

.. literalinclude:: ../../src/duckplus/examples/typed_pipeline_demos.py
   :pyobject: schema_driven_projection
   :language: python
