"""Raspberry Pi typed-expression demo anchored around π calculations.

The prose that lived in :mod:`docs.pi_demo` now resides here so the code is the
single source of truth.  The helpers show how to combine
``duckplus.static_typed`` primitives to generate deterministic projection and
aggregation SQL while preserving metadata such as dependencies and type
annotations.  Everything runs without importing DuckDB at module load so the
example stays friendly to resource-constrained hosts.

Running the demo queries
------------------------

Execute the module directly to render the generated SQL and, when possible,
fetch the results from DuckDB::

    python -m duckplus.examples.pi_demo

If DuckDB is unavailable the script prints installation guidance alongside the
projection and aggregation statements so you can still inspect the generated
queries.

Type checker feedback
---------------------

The module includes ``reveal_type`` probes guarded by ``TYPE_CHECKING`` so tools
like ``mypy`` can confirm the expression shapes.  Running::

    mypy -p duckplus.examples.pi_demo

produces output similar to::

    note: Revealed type is "duckplus.static_typed.expressions.numeric.NumericExpression"
    note: Revealed type is "duckplus.static_typed.expressions.numeric.NumericExpression"

This gives immediate assurance that downstream helpers receive strongly typed
expressions with preserved dependencies.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence, TYPE_CHECKING

from duckplus.static_typed import AliasedExpression, NumericExpression, TypedExpression, ducktype
from duckplus.static_typed.types import NumericType

if TYPE_CHECKING:  # pragma: no cover - executed only during type checking
    from typing import reveal_type

    _radius_probe = ducktype.Numeric("radius")
    reveal_type(_radius_probe)
    _radius_sum = ducktype.Numeric.Aggregate.sum(_radius_probe)
    reveal_type(_radius_sum)


@dataclass(frozen=True)
class CircleExpressions:
    """Reusable expressions describing circle metrics.

    The trio of expressions—``radius``, ``area``, and ``circumference``—feed the
    projections and aggregations showcased when you execute the module as a
    script.  Each retains the dependency metadata expected by
    :func:`summarise_circle_metrics`.
    """

    radius: NumericExpression
    area: NumericExpression
    circumference: NumericExpression


def build_circle_expressions(radius_column: str = "radius") -> CircleExpressions:
    """Construct numeric expressions for circle area and circumference.

    Parameters
    ----------
    radius_column:
        Name of the column supplying circle radii.

    The expressions power both the projection and aggregation queries described
    in the module documentation.  They demonstrate how literal values—such as π
    and the ``2`` used for circumference—can be embedded with explicit
    ``duck_type`` metadata so downstream ``render`` calls remain deterministic.
    """

    radius = ducktype.Numeric(radius_column)
    pi_literal = ducktype.Numeric.literal(
        3.141592653589793,
        duck_type=NumericType("DOUBLE"),
    )
    area = pi_literal * radius * radius
    circumference = pi_literal * radius * ducktype.Numeric.literal(2)
    return CircleExpressions(radius=radius, area=area, circumference=circumference)


def project_circle_metrics(radius_column: str = "radius") -> Sequence[AliasedExpression]:
    """Return aliased projections for radius, area, and circumference.

    These projections correspond to the "projection" query printed when the demo
    runs.  Each alias ensures the rendered SQL lines up with the sample output in
    the documentation and makes the resulting relation ergonomic to inspect.
    """

    expressions = build_circle_expressions(radius_column)
    return (
        expressions.radius.alias("radius"),
        expressions.area.alias("area"),
        expressions.circumference.alias("circumference"),
    )


def summarise_circle_metrics(radius_column: str = "radius") -> Sequence[AliasedExpression]:
    """Produce aggregations that total area and circumference.

    The aggregation is the second query executed by :func:`run_duckdb_demo`.  It
    totals each derived metric while demonstrating how typed expressions compose
    with aggregation helpers such as ``sum``.
    """

    expressions = build_circle_expressions(radius_column)
    return (
        ducktype.Numeric.Aggregate.sum(expressions.area).alias("total_area"),
        ducktype.Numeric.Aggregate.sum(expressions.circumference).alias("total_circumference"),
    )


def render_select_sql(
    select_list: Iterable[TypedExpression],
    relation_sql: str,
) -> str:
    """Render a ``SELECT`` statement using the provided expressions.

    ``build_demo_queries`` uses this helper to produce the projection and
    aggregation SQL quoted in the module documentation.  The function keeps the
    demo self-contained by avoiding reliance on higher-level relation objects.
    """

    projections = ", ".join(expression.render() for expression in select_list)
    return f"SELECT {projections} FROM {relation_sql}"


def build_demo_queries(radius_column: str = "radius") -> dict[str, str]:
    """Generate demo SQL queries illustrating projection and aggregation.

    The returned dictionary includes ``projection`` and ``summary`` keys.  When
    the module is executed as a script the queries are printed verbatim so they
    can be compared to the ``mypy`` output captured in the documentation.
    """

    projection = render_select_sql(project_circle_metrics(radius_column), "circles")
    summary = render_select_sql(summarise_circle_metrics(radius_column), "circles")
    return {"projection": projection, "summary": summary}


def run_duckdb_demo() -> Sequence[tuple[str, Sequence[tuple[object, ...]]]]:
    """Execute the demo SQL against DuckDB if the package is installed.

    The helper returns a list of ``(name, rows)`` tuples that mirror the console
    output produced by ``python -m duckplus.examples.pi_demo``.  A friendly
    :class:`RuntimeError` guides readers towards ``pip install duckdb`` when the
    dependency is missing, matching the behaviour described in the module
    docstring.
    """

    queries = build_demo_queries()
    try:
        import duckdb  # type: ignore[import-not-found]  # pylint: disable=import-outside-toplevel
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on environment
        message = (
            "DuckDB is not installed. Install it with 'pip install duckdb' to run the "
            "demo queries on your Raspberry Pi."
        )
        raise RuntimeError(message) from exc

    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE circles AS SELECT * FROM (VALUES (1.5), (2.0), (3.25)) AS t(radius)"
        )
        results: list[tuple[str, Sequence[tuple[object, ...]]]] = []
        for name, sql in queries.items():
            results.append((name, connection.execute(sql).fetchall()))
        return results
    finally:
        connection.close()


def main() -> None:
    """Entry point used when running the module as a script."""

    queries = build_demo_queries()
    try:
        results = run_duckdb_demo()
    except RuntimeError as exc:
        print(exc)
        print("\nGenerated SQL:")
        for name, sql in queries.items():
            print(f"- {name}: {sql}")
        return

    for name, rows in results:
        print(f"Query: {name}")
        for row in rows:
            print("  ", row)


if __name__ == "__main__":  # pragma: no cover - manual invocation utility
    main()
