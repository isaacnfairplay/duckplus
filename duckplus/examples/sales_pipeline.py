"""Comprehensive sales analytics demo showcasing DuckPlus primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from duckplus.duckcon import DuckCon
from duckplus.relation import Relation
from duckplus.typed import ExpressionDependency, ducktype
from duckplus.typed.expression import TypedExpression

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
    """Container holding the seed relations for the sales demo."""

    orders: Relation
    returns: Relation


@dataclass(frozen=True)
class SalesDemoReport:
    """Structured output produced by :func:`run_sales_demo`."""

    region_columns: tuple[str, ...]
    region_rows: list[tuple[object, ...]]
    channel_columns: tuple[str, ...]
    channel_rows: list[tuple[object, ...]]
    preview_columns: tuple[str, ...]
    preview_rows: list[tuple[object, ...]]
    projection_sql: str


def load_demo_relations(manager: DuckCon) -> SalesDemoData:
    """Seed the demo database with deterministic orders and returns."""

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


def build_enriched_orders(orders: Relation, returns: Relation) -> Relation:
    """Join orders with return metadata and compute derived metrics."""

    if orders.duckcon is not returns.duckcon:
        msg = "Orders and returns must originate from the same DuckCon"
        raise ValueError(msg)

    joined = orders.left_join(returns, on={"order_id": "returned_order_id"})

    total = ducktype.Numeric("order_total")
    shipping = ducktype.Numeric("shipping_cost")
    net_revenue = total - shipping
    tax_amount = net_revenue * ducktype.Numeric.literal(0.07)
    contribution = net_revenue - tax_amount
    total_sql = total.render()
    dependency_tuple = tuple(total.dependencies)
    threshold_200 = ducktype.Numeric.literal(200).render()
    threshold_250 = ducktype.Numeric.literal(250).render()
    threshold_350 = ducktype.Numeric.literal(350).render()
    high_value = ducktype.Boolean.raw(
        f"{total_sql} >= {threshold_250}",
        dependencies=dependency_tuple,
    )
    enterprise_condition = ducktype.Boolean.raw(
        f"{total_sql} >= {threshold_350}",
        dependencies=dependency_tuple,
    )
    growth_condition = ducktype.Boolean.raw(
        f"{total_sql} >= {threshold_200}",
        dependencies=dependency_tuple,
    )
    service_tier = (
        ducktype.Varchar.case()
        .when(enterprise_condition, "enterprise")
        .when(growth_condition, "growth")
        .else_("starter")
        .end()
    )
    return_dependency = ExpressionDependency.column("return_reason")
    is_returned = ducktype.Boolean.raw(
        '"return_reason" IS NOT NULL',
        dependencies=(return_dependency,),
    )

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


def _count(expression: TypedExpression) -> TypedExpression:
    return ducktype.Functions.Aggregate.Numeric.count(expression)


def _count_if(expression: TypedExpression) -> TypedExpression:
    return ducktype.Functions.Aggregate.Numeric.count_if(expression)


def summarise_by_region(enriched: Relation) -> Relation:
    """Aggregate the enriched dataset by sales region."""

    total_orders = _count(ducktype.Numeric("order_id"))
    returned_orders = _count_if(ducktype.Boolean("is_returned"))
    rate_dependencies = tuple(
        returned_orders.dependencies.union(total_orders.dependencies)
    )
    return_rate = ducktype.Numeric.raw(
        f"({returned_orders.render()} / NULLIF({total_orders.render()}, 0))",
        dependencies=rate_dependencies,
    )

    return enriched.aggregate(
        "region",
        total_orders=total_orders,
        net_revenue=ducktype.Numeric("net_revenue").sum(),
        high_value_orders=_count_if(ducktype.Boolean("is_high_value")),
        return_rate=return_rate,
    )


def summarise_by_channel(enriched: Relation) -> Relation:
    """Aggregate contribution and repeat metrics by channel."""

    return enriched.aggregate(
        "channel",
        total_orders=_count(ducktype.Numeric("order_id")),
        repeat_orders=_count_if(ducktype.Boolean("is_repeat")),
        average_contribution=ducktype.Numeric("contribution").avg(),
    )


def render_projection_sql(enriched: Relation) -> str:
    """Render a SELECT projection that exercises optional clauses."""

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
                    ducktype.Boolean.raw(
                        '"return_reason" IS NULL',
                        dependencies=(ExpressionDependency.column("return_reason"),),
                    ),
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


def _capture_rows(relation: Relation, *, order_by: Iterable[str] | None = None) -> list[tuple[object, ...]]:
    if order_by is None:
        ordered = relation.relation
    else:
        ordered = relation.relation.order(", ".join(order_by))
    return list(ordered.fetchall())


def run_sales_demo() -> SalesDemoReport:
    """Execute the full sales pipeline and capture summary artifacts."""

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
