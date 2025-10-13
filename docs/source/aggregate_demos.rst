Aggregate demos
===============

DuckRel.aggregate pairs nicely with :class:`duckplus.AggregateExpression` to build
structured aggregate queries without hand-writing SQL. The examples below reuse a
small in-memory dataset and mirror the questions commonly asked in analytics
pipelines.

Sample data
-----------

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: sales_demo_relation
   :language: python

Totals and grouped aggregates
-----------------------------

Summaries that collapse an entire relation or group by one or more columns rely
on the dedicated constructors exposed by :class:`duckplus.AggregateExpression`.

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: total_sales_amount
   :language: python

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: sales_by_region
   :language: python

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: regions_over_target
   :language: python

Distinct and filtered aggregates
--------------------------------

Use the ``distinct`` flag and :meth:`duckplus.AggregateExpression.with_filter`
helper when you need to focus on unique values or ignore specific rows.

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: distinct_region_count
   :language: python

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: filtered_total_excluding_north
   :language: python

Ordering-sensitive aggregates
-----------------------------

Aggregates like :func:`first` and :func:`list` become deterministic when paired
with :meth:`duckplus.AggregateExpression.with_order_by`.

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: ordered_region_list
   :language: python

.. literalinclude:: ../../src/duckplus/examples/aggregate_demos.py
   :pyobject: first_sale_amount
   :language: python

Using the demos
---------------

Each helper accepts a :class:`duckplus.DuckRel` instance, so you can compose them
inside a connection context:

.. code-block:: python

   import duckdb
   from duckplus import DuckRel
   from duckplus.examples.aggregate_demos import (
       first_sale_amount,
       sales_by_region,
       sales_demo_relation,
       total_sales_amount,
   )

   with duckdb.connect() as conn:
       sales = sales_demo_relation(conn)
       print(total_sales_amount(sales))
       print(sales_by_region(sales))
       print(first_sale_amount(sales))

The tests in :mod:`tests.test_examples` execute these functions to ensure the
documentation stays current.
