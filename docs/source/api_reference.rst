.. _duckplus_api_reference:

Duck+ API Reference
===================

Duck+ organizes its public surface area around predictable relational workflows
plus a handful of batteries-included extras. Use the core API to open
connections, compose immutable relations, and manage mutation helpers. Reach for
the extras when you need first-party ingestion adapters, materialization
strategies, or operational tooling.

.. rubric:: Core API

.. toctree::
   :maxdepth: 2

   api/connection_management
   api/relational_pipeline
   api/mutable_tables

.. rubric:: Batteries-included extras

.. toctree::
   :maxdepth: 2

   api/data_ingestion
   api/materialization
   api/secrets_registry
   api/html_preview
   api/cli
   api/utilities
   api/public_namespace
   api/demos
