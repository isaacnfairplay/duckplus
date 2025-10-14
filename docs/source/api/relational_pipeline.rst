Relational pipelines
====================

``duckplus.core`` implements immutable relational pipelines that defer execution
until explicitly materialized. Helpers return new :class:`duckplus.Relation`
instances, keeping transformations composable and type-aware while mirroring
DuckDB semantics.

.. currentmodule:: duckplus.core

.. autosummary::
   :nosignatures:

   DuckRel
   AsofOrder
   AsofSpec
   ExpressionPredicate
   JoinProjection
   JoinSpec
   PartitionSpec
   ColumnExpression
   FilterExpression
   column
   col
   equals
   not_equals
   less_than
   less_than_or_equal
   greater_than
   greater_than_or_equal
   ducktypes

.. automodule:: duckplus.core
   :members:
   :member-order: bysource
   :noindex:

DuckRel facade
--------------

``duckplus.duckrel`` exposes the ``DuckRel`` class definition and
implementation details that underpin the relational wrapper used throughout the
library.

.. currentmodule:: duckplus.duckrel

.. automodule:: duckplus.duckrel
   :members:
   :member-order: bysource
   :noindex:
