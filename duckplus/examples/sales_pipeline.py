"""Sales analytics pipeline demo showcasing DuckPlus primitives.

This module mirrors the walkthrough that used to live in
``docs/versions/1.1/sales_pipeline_demo.md``.  It seeds a managed
:class:`~duckplus.duckcon.DuckCon` with deterministic ``orders`` and ``returns``
relations, derives enriched metrics, and aggregates the results for leadership
reporting.  The helpers return a
:class:`~duckplus.examples.sales_pipeline.SalesDemoReport` dataclass so that
tests and documentation can embed the generated artefacts directly.

Running the walkthrough
-----------------------

Execute the module to build the in-memory dataset and print the captured
summaries::

    python -m duckplus.examples.sales_pipeline

The command prints region-level and channel-level results followed by a sample
``SELECT`` statement emitted by the typed builder.

.. tip::

   The demo requires no external data sources—the dataset is synthesised from
   Python literals so it runs identically on every machine.  This makes it
   ideal for onboarding sessions or quick smoke tests when you upgrade DuckDB.

Preview rows
------------

The helper stores a compact preview to make doc examples reproducible.  The
first five enriched rows are::

    (1, 2024-06-01, 'north', 'acme', 'online', False, 120.0, 18.5, None,
     101.5, 7.105, 94.395, False, 'starter', False)
    (2, 2024-06-01, 'north', 'acme', 'field', True, 240.0, 22.0,
     'Damaged packaging', 218.0, 15.26, 202.74, False, 'growth', True)
    (3, 2024-06-02, 'west', 'venture', 'field', False, 310.0, 35.0, None,
     275.0, 19.25, 255.75, True, 'growth', False)
    (4, 2024-06-02, 'west', 'venture', 'online', False, 180.0, 15.0, None,
     165.0, 11.55, 153.45, False, 'starter', False)
    (5, 2024-06-03, 'south', 'nomad', 'online', True, 95.0, 9.0,
     'Late delivery', 86.0, 6.02, 79.98, False, 'starter', True)

The values mirror the tuples stored in
:attr:`SalesDemoReport.preview_rows
<duckplus.examples.sales_pipeline.SalesDemoReport.preview_rows>`.
Because the dataclass captures both the enriched relation and its metadata, you
can assert on ``report.preview_columns`` in tests to confirm column order and
retain deterministic docs.

Region performance
------------------

``SalesDemoReport.region_rows`` summarises return rates and revenue by sales
region.  The deterministic output enables the documentation and tests to agree
on the same numbers.  The aggregation uses typed expressions for ``sum`` and
``count_if`` to demonstrate how numeric helpers compose:

.. list-table:: Region metrics produced by :func:`summarise_by_region`.
   :header-rows: 1

   * - region
     - total_orders
     - net_revenue
     - high_value_orders
     - return_rate
   * - east
     - 2
     - 301.0
     - 1
     - 0.50
   * - north
     - 2
     - 319.5
     - 0
     - 0.50
   * - south
     - 2
     - 448.0
     - 1
     - 0.50
   * - west
     - 2
     - 440.0
     - 1
     - 0.00

Channel performance
-------------------

The channel summary surfaces repeat behaviour and contribution averages::

    ('field', 2, 1, 229.245)
    ('online', 4, 1, 166.12125)
    ('partner', 2, 1, 139.965)

These rows correspond to
:attr:`SalesDemoReport.channel_rows
<duckplus.examples.sales_pipeline.SalesDemoReport.channel_rows>`.
Call :func:`summarise_by_channel` when you need to recompute the relation for
exploratory analysis.

Typed projection example
------------------------

The demo emits the typed ``SELECT`` used to showcase ``if_exists`` clauses.  It
replaces the ``service_tier`` column with a computed label while falling back to
``fulfilled`` when ``return_reason`` is absent::

    SELECT * REPLACE (
        CASE WHEN "is_returned" THEN 'service'
             WHEN "is_high_value" THEN 'priority'
             ELSE "service_tier" END AS "service_tier",
        CASE WHEN "return_reason" IS NULL THEN 'fulfilled'
             ELSE "return_reason" END AS "return_reason"
    ),
    sum("net_revenue") AS "cumulative_net"
    FROM enriched_orders

Because the ``SELECT`` builder is dependency-aware, the optional clauses
disappear if an upstream relation omits ``return_reason`` or ``net_revenue``.
Reuse :func:`build_enriched_orders` in your own scripts when you want to add new
metrics or persist the intermediate relation to disk.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from duckplus.duckcon import DuckCon
from duckplus.relation import Relation
from duckplus.static_typed import ducktype
from duckplus.static_typed.expression import TypedExpression
from duckplus.static_typed.expressions.numeric import NumericExpression

__all__ = [
    "SalesDemoData",
    "SalesDemoReport",
    "load_demo_relations",
    "build_enriched_orders",
    "summarise_by_region",
    "summarise_by_channel",
    "render_projection_sql",
    "run_sales_demo",
]


@dataclass(frozen=True)
class SalesDemoData:
    """Container holding the seed relations for the sales demo.

    The relations produced by :func:`load_demo_relations` mirror the literal
    values embedded in the original documentation so every execution yields the
    same results.  The ``orders`` table includes regions, customers, and
    shipping metadata while ``returns`` captures the subset of orders that were
    refunded.
    """

    orders: Relation
    returns: Relation


@dataclass(frozen=True)
class SalesDemoReport:
    """Structured output produced by :func:`run_sales_demo`.

    The dataclass mirrors the data embedded in the legacy Markdown guide.  Its
    attributes store both the relation schemas (for regression testing) and the
    deterministic tuples showcased in the module-level examples.
    """

    region_columns: tuple[str, ...]
    region_rows: list[tuple[object, ...]]
    channel_columns: tuple[str, ...]
    channel_rows: list[tuple[object, ...]]
    preview_columns: tuple[str, ...]
    preview_rows: list[tuple[object, ...]]
    projection_sql: str


def load_demo_relations(manager: DuckCon) -> SalesDemoData:
    """Seed the demo database with deterministic orders and returns.

    The helper materialises two relations inside the provided connection:

    - ``orders`` mirrors the preview rows shown in the module documentation with
      repeat flags, order totals, and shipping costs.
    - ``returns`` captures the subset of orders that were refunded so
      :func:`build_enriched_orders` can showcase dependency-aware joins.

    The literal SQL keeps the walkthrough portable—no external CSV files are
    required and the generated :class:`SalesDemoReport` is stable across
    platforms.
    """

    connection = manager.connection

    orders_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            (1::INTEGER, DATE '2024-06-01', 'north'::VARCHAR, 'acme'::VARCHAR,
             120.00::DOUBLE, 18.50::DOUBLE, 'online'::VARCHAR, FALSE),
            (2::INTEGER, DATE '2024-06-01', 'north'::VARCHAR, 'acme'::VARCHAR,
             240.00::DOUBLE, 22.00::DOUBLE, 'field'::VARCHAR, TRUE),
            (3::INTEGER, DATE '2024-06-02', 'west'::VARCHAR, 'venture'::VARCHAR,
             310.00::DOUBLE, 35.00::DOUBLE, 'field'::VARCHAR, FALSE),
            (4::INTEGER, DATE '2024-06-02', 'west'::VARCHAR, 'venture'::VARCHAR,
             180.00::DOUBLE, 15.00::DOUBLE, 'online'::VARCHAR, FALSE),
            (5::INTEGER, DATE '2024-06-03', 'south'::VARCHAR, 'nomad'::VARCHAR,
             95.00::DOUBLE, 9.00::DOUBLE, 'online'::VARCHAR, TRUE),
            (6::INTEGER, DATE '2024-06-03', 'south'::VARCHAR, 'nomad'::VARCHAR,
             410.00::DOUBLE, 48.00::DOUBLE, 'online'::VARCHAR, FALSE),
            (7::INTEGER, DATE '2024-06-04', 'east'::VARCHAR, 'zenith'::VARCHAR,
             275.00::DOUBLE, 32.00::DOUBLE, 'partner'::VARCHAR, FALSE),
            (8::INTEGER, DATE '2024-06-04', 'east'::VARCHAR, 'zenith'::VARCHAR,
             65.00::DOUBLE, 7.00::DOUBLE, 'partner'::VARCHAR, TRUE)
        ) AS orders(
            order_id, order_date, region, customer,
            order_total, shipping_cost, channel, is_repeat
        )
        """.strip()
    )

    returns_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            (2::INTEGER, DATE '2024-06-02', 'Damaged packaging'::VARCHAR),
            (5::INTEGER, DATE '2024-06-04', 'Late delivery'::VARCHAR),
            (8::INTEGER, DATE '2024-06-05', 'Changed mind'::VARCHAR)
        ) AS returns(returned_order_id, returned_at, return_reason)
        """.strip()
    )

    return SalesDemoData(
        orders=Relation.from_relation(manager, orders_relation),
        returns=Relation.from_relation(manager, returns_relation),
    )


# pylint: disable=too-many-locals
def build_enriched_orders(orders: Relation, returns: Relation) -> Relation:
    """Join orders with return metadata and compute derived metrics.

    The enriched relation preserves the columns surfaced in
    :attr:`SalesDemoReport.preview_rows` and introduces additional metrics used
    by the summaries:

    - ``net_revenue`` subtracts shipping costs from the order total.
    - ``tax_amount`` and ``contribution`` demonstrate how typed arithmetic keeps
      dependency information intact for downstream ``sum`` aggregations.
    - ``is_high_value`` and ``service_tier`` showcase ``CASE`` helpers with
      dependency tracking for optional ``REPLACE`` clauses.
    - ``is_returned`` flags rows present in the ``returns`` table so the return
      rate calculation matches the published walkthrough.
    """

    if orders.duckcon is not returns.duckcon:
        msg = "Orders and returns must originate from the same DuckCon"
        raise ValueError(msg)

    joined = orders.left_join(returns, on={"order_id": "returned_order_id"})

    total = ducktype.Numeric("order_total")
    shipping = ducktype.Numeric("shipping_cost")
    net_revenue = total - shipping
    tax_amount = net_revenue * ducktype.Numeric.literal(0.07)
    contribution = net_revenue - tax_amount
    high_value = total >= ducktype.Numeric.literal(250)
    enterprise_condition = total >= ducktype.Numeric.literal(350)
    growth_condition = total >= ducktype.Numeric.literal(200)
    service_tier = (
        ducktype.Varchar.case()
        .when(enterprise_condition, "enterprise")
        .when(growth_condition, "growth")
        .else_("starter")
        .end()
    )
    is_returned = ducktype.Generic("return_reason").is_not_null()

    enriched = joined.add(
        net_revenue=net_revenue,
        tax_amount=tax_amount,
        contribution=contribution,
        is_high_value=high_value,
        service_tier=service_tier,
        is_returned=is_returned,
    )

    return enriched.keep(
        "order_id",
        "order_date",
        "region",
        "customer",
        "channel",
        "is_repeat",
        "order_total",
        "shipping_cost",
        "return_reason",
        "net_revenue",
        "tax_amount",
        "contribution",
        "is_high_value",
        "service_tier",
        "is_returned",
    )


def _count(expression: TypedExpression) -> NumericExpression:
    return ducktype.Numeric.Aggregate.count(expression)


def _count_if(expression: TypedExpression) -> NumericExpression:
    return ducktype.Numeric.Aggregate.count_if(expression)


def summarise_by_region(enriched: Relation) -> Relation:
    """Aggregate the enriched dataset by sales region.

    ``SalesDemoReport.region_rows`` stores the output so tests and documentation
    share the same deterministic numbers.  The list-table in the module docstring
    mirrors the rendered tuples and demonstrates how ``count`` and ``count_if``
    compose with manually constructed dependency graphs.
    """

    total_orders = _count(ducktype.Numeric("order_id"))
    returned_orders = _count_if(ducktype.Boolean("is_returned"))
    denominator = total_orders.nullif(ducktype.Numeric.literal(0))
    return_rate = returned_orders / denominator

    return (
        enriched.aggregate()
        .start_agg()
        .agg(total_orders, alias="total_orders")
        .agg(ducktype.Numeric("net_revenue").sum(), alias="net_revenue")
        .agg(
            _count_if(ducktype.Boolean("is_high_value")),
            alias="high_value_orders",
        )
        .agg(return_rate, alias="return_rate")
        .by("region")
    )


def summarise_by_channel(enriched: Relation) -> Relation:
    """Aggregate contribution and repeat metrics by channel.

    The three tuples quoted in the module documentation correspond to this
    relation when ordered by ``channel``.  ``repeat_orders`` and
    ``average_contribution`` demonstrate how derived boolean and numeric
    expressions can feed aggregation helpers without losing type information.
    """

    return (
        enriched.aggregate()
        .start_agg()
        .agg(_count(ducktype.Numeric("order_id")), alias="total_orders")
        .agg(
            _count_if(ducktype.Boolean("is_repeat")),
            alias="repeat_orders",
        )
        .agg(
            ducktype.Numeric("contribution").avg(),
            alias="average_contribution",
        )
        .by("channel")
    )


def render_projection_sql(enriched: Relation) -> str:
    """Render a ``SELECT`` projection that exercises optional clauses.

    The string output matches the snippet reproduced in the module docstring.
    ``REPLACE`` entries use dependency-aware expressions so optional clauses are
    omitted automatically when upstream columns are missing.  ``run_sales_demo``
    exposes the rendered SQL via :attr:`SalesDemoReport.projection_sql` for easy
    assertions in tests and docs.
    """

    builder = ducktype.select()
    builder.star(
        replace={
            "service_tier": (
                ducktype.Varchar.case()
                .when(ducktype.Boolean("is_returned"), "service")
                .when(ducktype.Boolean("is_high_value"), "priority")
                .else_(ducktype.Varchar("service_tier"))
                .end()
            )
        },
        replace_if_exists={
            "return_reason": (
                ducktype.Varchar.case()
                .when(
                    ducktype.Generic("return_reason").is_null(),
                    "fulfilled",
                )
                .else_(ducktype.Varchar("return_reason"))
                .end()
            )
        },
        exclude_if_exists=["returned_order_id"],
    )
    builder.column(
        ducktype.Numeric("net_revenue").sum().alias("cumulative_net"),
        if_exists=True,
    )
    builder.from_("enriched_orders")
    return builder.build(available_columns=enriched.columns)


def _capture_rows(
    relation: Relation,
    *,
    order_by: Iterable[str] | None = None,
) -> list[tuple[object, ...]]:
    if order_by is None:
        ordered = relation
    else:
        ordered = relation.order_by(*order_by)
    return list(ordered.relation.fetchall())


# pylint: disable=too-many-locals
def run_sales_demo() -> SalesDemoReport:
    """Execute the full sales pipeline and capture summary artefacts.

    The returned :class:`SalesDemoReport` matches the walkthrough embedded in
    this module.  ``preview_rows`` stores the first five enriched tuples, while
    ``region_rows`` and ``channel_rows`` capture the tables rendered in the
    documentation.  ``projection_sql`` mirrors the ``SELECT`` emitted by
    :func:`render_projection_sql` so consumers can assert on the generated text.
    """

    manager = DuckCon()
    with manager:
        data = load_demo_relations(manager)
        enriched = build_enriched_orders(data.orders, data.returns)
        region_summary = summarise_by_region(enriched)
        channel_summary = summarise_by_channel(enriched)
        projection_sql = render_projection_sql(enriched)

        region_rows = _capture_rows(region_summary, order_by=["region"])
        channel_rows = _capture_rows(channel_summary, order_by=["channel"])
        preview_rows = _capture_rows(enriched, order_by=["order_id"])

        return SalesDemoReport(
            region_columns=region_summary.columns,
            region_rows=region_rows,
            channel_columns=channel_summary.columns,
            channel_rows=channel_rows,
            preview_columns=enriched.columns,
            preview_rows=preview_rows[:5],
            projection_sql=projection_sql,
        )
