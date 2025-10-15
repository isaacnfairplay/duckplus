# Raspberry Pi Typed Expression Demo

The `duckplus.examples.pi_demo` module provides a hands-on walkthrough for the
typed expression API using a circle-math scenario built around π.  The helper
functions generate projection and aggregation SQL while preserving metadata such
as column dependencies and type annotations.

## Running the Demo Queries

The module is packaged so you can execute it directly::

    python -m duckplus.examples.pi_demo

If DuckDB is not installed you will receive a friendly error message explaining
how to add it.  Once DuckDB is available, the script creates a small `circles`
table and executes two queries built from typed expressions:

* A projection that surfaces the raw radius alongside calculated area and
  circumference columns.
* An aggregation that totals area and circumference across the dataset.

## Type Checker Feedback

We include ``reveal_type`` probes guarded by ``TYPE_CHECKING`` inside the module
so that ``mypy`` (or another static type checker) can confirm the expression
shapes.  Running::

    mypy -p duckplus.examples.pi_demo

produces output similar to::

    note: Revealed type is "duckplus.typed.expression.NumericExpression"
    note: Revealed type is "duckplus.typed.expression.NumericExpression"

This gives immediate assurance that downstream helpers receive strongly-typed
expressions with preserved dependencies.
