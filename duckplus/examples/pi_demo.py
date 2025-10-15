"""Circle-math demo showcasing typed expressions with Pi-friendly calculations.

This module highlights how DuckPlus typed expressions can be composed to build
DuckDB SQL while retaining strong typing information.  It is designed to run on
resource-constrained hosts (like a Raspberry Pi) without needing DuckDB at
import time.  If DuckDB is available, :func:`run_duckdb_demo` executes the SQL
that the expressions render so you can inspect real results.

To inspect the static typing feedback these helpers provide, run::

    mypy -p duckplus.examples.pi_demo

The module includes ``reveal_type`` probes guarded by ``TYPE_CHECKING`` so the
type checker will surface the expression types when you run the command above.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence, TYPE_CHECKING

from duckplus.typed import AliasedExpression, NumericExpression, TypedExpression, ducktype
from duckplus.typed.types import NumericType

if TYPE_CHECKING:  # pragma: no cover - executed only during type checking
    from typing import reveal_type

    _radius_probe = ducktype.Numeric("radius")
    reveal_type(_radius_probe)
    _radius_sum = ducktype.Numeric.Aggregate.sum(_radius_probe)
    reveal_type(_radius_sum)


@dataclass(frozen=True)
class CircleExpressions:
    """Reusable expressions describing circle metrics."""

    radius: NumericExpression
    area: NumericExpression
    circumference: NumericExpression


def build_circle_expressions(radius_column: str = "radius") -> CircleExpressions:
    """Construct numeric expressions for circle area and circumference.

    Parameters
    ----------
    radius_column:
        Name of the column supplying circle radii.
    """

    radius = ducktype.Numeric(radius_column)
    pi_literal = ducktype.Numeric.raw(
        "3.141592653589793::DOUBLE",
        duck_type=NumericType("DOUBLE"),
    )
    area = pi_literal * radius * radius
    circumference = pi_literal * radius * ducktype.Numeric.literal(2)
    return CircleExpressions(radius=radius, area=area, circumference=circumference)


def project_circle_metrics(radius_column: str = "radius") -> Sequence[AliasedExpression]:
    """Return aliased projections for radius, area, and circumference."""

    expressions = build_circle_expressions(radius_column)
    return (
        expressions.radius.alias("radius"),
        expressions.area.alias("area"),
        expressions.circumference.alias("circumference"),
    )


def summarise_circle_metrics(radius_column: str = "radius") -> Sequence[AliasedExpression]:
    """Produce aggregations that total area and circumference."""

    expressions = build_circle_expressions(radius_column)
    return (
        ducktype.Numeric.Aggregate.sum(expressions.area).alias("total_area"),
        ducktype.Numeric.Aggregate.sum(expressions.circumference).alias("total_circumference"),
    )


def render_select_sql(
    select_list: Iterable[TypedExpression],
    relation_sql: str,
) -> str:
    """Render a SELECT statement using the provided expressions."""

    projections = ", ".join(expression.render() for expression in select_list)
    return f"SELECT {projections} FROM {relation_sql}"


def build_demo_queries(radius_column: str = "radius") -> dict[str, str]:
    """Generate demo SQL queries illustrating projection and aggregation."""

    projection = render_select_sql(project_circle_metrics(radius_column), "circles")
    summary = render_select_sql(summarise_circle_metrics(radius_column), "circles")
    return {"projection": projection, "summary": summary}


def run_duckdb_demo() -> Sequence[tuple[str, Sequence[tuple[object, ...]]]]:
    """Execute the demo SQL against DuckDB if the package is installed."""

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
