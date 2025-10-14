"""Aggressive best-practice demonstrations for production pipelines.

The helpers in this module lean on typed relations, idempotent table writes,
and Arrow materialisation to showcase how Duck+ can power reliable release
pipelines. Each demo is intentionally compact so teams can lift the patterns
directly into tooling or orchestration code with minimal adjustments.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple, cast

from duckplus import (
    AggregateExpression,
    ArrowMaterializeStrategy,
    ColumnExpression,
    DuckConnection,
    DuckRel,
    DuckTable,
    Relation,
    col,
    ducktypes,
)
from duckplus.schema import AnyRow
from duckplus.examples import typed_pipeline_demos

__all__ = [
    "priority_dispatch_payload",
    "incremental_fact_ingest",
    "customer_spike_detector",
    "regional_order_kpis",
    "arrow_priority_snapshot",
    "lean_projection_shortcut",
]


def _typed_orders(conn: DuckConnection) -> Relation[AnyRow]:
    """Return the shared typed orders relation for the demos."""

    return typed_pipeline_demos.typed_orders_demo_relation(conn.raw)


def _typed_columns(
    names: Iterable[str],
) -> Dict[str, ColumnExpression[ducktypes.DuckType, object]]:
    """Return typed column expressions keyed by *names* for reuse."""

    candidates: Dict[str, ColumnExpression[ducktypes.DuckType, object]] = {
        "order_id": col("order_id", duck_type=ducktypes.Integer),
        "region": col("region", duck_type=ducktypes.Varchar),
        "customer": col("customer", duck_type=ducktypes.Varchar),
        "order_total": col("order_total", duck_type=ducktypes.Integer),
        "items": col("items", duck_type=ducktypes.Integer),
        "placed_at": col("placed_at", duck_type=ducktypes.Date),
        "priority": col("priority", duck_type=ducktypes.Boolean),
    }
    return {name: candidates[name] for name in names}


def priority_dispatch_payload(conn: DuckConnection) -> List[tuple[int, str, int, bool]]:
    """Return high-urgency orders sorted by revenue for alerting systems."""

    typed = _typed_columns(["order_id", "region", "order_total", "priority"])
    orders = _typed_orders(conn)
    urgent = orders.filter(
        (typed["priority"] == True)  # noqa: E712 - explicit comparison keeps typing
        & (typed["order_total"] >= 150)
    )
    curated = urgent.project({key: typed[key] for key in typed})
    ordered = curated.order_by((typed["order_total"], "desc"))
    return ordered.fetch_typed()


def incremental_fact_ingest(
    conn: DuckConnection,
) -> tuple[int, List[tuple[int, str, int]]]:
    """Insert only unseen rows into a fact table and return the stored snapshot."""

    typed = _typed_columns(["order_id", "region", "order_total"])
    orders = _typed_orders(conn)
    fact_payload = cast(Relation[AnyRow], orders.project({key: typed[key] for key in typed}))

    conn.raw.execute("DROP TABLE IF EXISTS analytics_orders_fact")
    conn.raw.execute(
        """
        CREATE TABLE analytics_orders_fact (
            order_id INTEGER,
            region VARCHAR,
            order_total INTEGER
        )
        """
    )

    table = DuckTable(conn, "analytics_orders_fact")
    primer = cast(Relation[AnyRow], fact_payload.filter(typed["order_id"] <= 3))
    table.append(primer)

    inserted = table.insert_antijoin(fact_payload, keys=["order_id"])

    stored = Relation(conn.raw.table("analytics_orders_fact")).project(
        {key: typed[key] for key in typed}
    )
    ordered = stored.order_by((typed["order_id"], "asc"))
    return inserted, ordered.fetch_typed()


def customer_spike_detector(conn: DuckConnection) -> List[tuple[str, int, int]]:
    """Flag customers whose maximum order value surpasses aggressive targets."""

    typed = _typed_columns(["customer", "order_id", "order_total"])
    orders = _typed_orders(conn)
    metrics = orders.aggregate(
        typed["customer"],
        order_count=AggregateExpression.count(typed["order_id"]),
        peak_order=AggregateExpression.max(typed["order_total"]),
    )
    flagged = metrics.filter(col("peak_order", duck_type=ducktypes.Integer) >= 150)
    ordered = flagged.order_by((col("peak_order", duck_type=ducktypes.Integer), "desc"))
    return ordered.fetch_typed()


def regional_order_kpis(conn: DuckConnection) -> List[tuple[str, int, int, int]]:
    """Summarize key metrics per region using reusable typed expressions."""

    typed = _typed_columns(["region", "order_id", "order_total", "priority"])
    orders = _typed_orders(conn)
    priority_filter = typed["priority"] == True  # noqa: E712
    summary = orders.aggregate(
        typed["region"],
        total_orders=AggregateExpression.count(typed["order_id"]),
        priority_orders=AggregateExpression.count(
            typed["order_id"],
            filter=priority_filter,
        ),
        total_revenue=AggregateExpression.sum(typed["order_total"]),
    )
    ordered = summary.order_by((typed["region"], "asc"))
    return ordered.fetch_typed()


def arrow_priority_snapshot(conn: DuckConnection) -> List[tuple[int, str]]:
    """Materialise a typed Arrow payload for caching layers or webhooks."""

    typed = _typed_columns(["order_id", "customer", "order_total", "placed_at"])
    orders = _typed_orders(conn)
    snapshot = (
        orders
        .filter(
            (typed["order_total"] >= 100)
            & (col("priority", duck_type=ducktypes.Boolean) == True)  # noqa: E712
        )
        .project({key: typed[key] for key in ("order_id", "customer", "placed_at")})
        .order_by((typed["placed_at"], "asc"))
        .materialize(strategy=ArrowMaterializeStrategy())
    )
    table = snapshot.require_table()
    return list(zip(table.column("order_id").to_pylist(), table.column("customer").to_pylist()))


def lean_projection_shortcut(conn: DuckConnection) -> List[tuple[int, str, str]]:
    """Showcase minimal code for curated views while preserving typing."""

    orders = _typed_orders(conn)
    typed = _typed_columns(["order_id", "region", "customer"])
    curated = orders.project({
        "order_id": typed["order_id"],
        "region": typed["region"],
        "customer": typed["customer"],
    })
    transformed = curated.transform_columns(region="UPPER({column})")
    typed_transformed = transformed.project({
        "order_id": col("order_id", duck_type=ducktypes.Integer),
        "region": col("region", duck_type=ducktypes.Varchar),
        "customer": col("customer", duck_type=ducktypes.Varchar),
    })
    ordered = typed_transformed.order_by((col("order_id", duck_type=ducktypes.Integer), "asc"))
    return ordered.fetch_typed()

