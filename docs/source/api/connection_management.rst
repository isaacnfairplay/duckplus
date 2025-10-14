Connection management
=====================

``duckplus.connect`` provides the core connection wrapper and adapters that keep
DuckDB interactions predictable across environments. The helpers mirror DuckDB's
APIs while enforcing identifier validation and immutable relational workflows.

.. currentmodule:: duckplus.connect

.. autosummary::
   :nosignatures:

   connect
   DuckConnection
   load_extensions
   attach_nanodbc
   query_nanodbc

.. automodule:: duckplus.connect
   :members:
   :member-order: bysource
   :noindex:

ODBC strategies
---------------

Duck+ offers a strategy framework for managing ODBC connection strings in
concert with :class:`duckplus.secrets.SecretManager`. Each strategy produces a
:class:`duckplus.secrets.SecretDefinition` or persists credentials directly with
the manager before reconstructing the final connection string.

.. currentmodule:: duckplus.odbc

.. autosummary::
   :nosignatures:

   AccessStrategy
   CustomODBCStrategy
   DuckDBDsnStrategy
   ExcelStrategy
   IBMiAccessStrategy
   MySQLStrategy
   PostgresStrategy
   SQLServerStrategy

.. automodule:: duckplus.odbc
   :members:
   :member-order: bysource
   :noindex:
