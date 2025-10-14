from __future__ import annotations

"""Integration-style exploratory tests exercising DuckRel flows."""

from datetime import datetime
from decimal import Decimal

import pytest

from duckplus import (
    AggregateExpression,
    ArrowMaterializeStrategy,
    AsofOrder,
    DuckConnection,
    DuckRel,
    FilterExpression,
    ExpressionPredicate,
    JoinSpec,
    ParquetMaterializeStrategy,
    Relation,
    col,
    column,
    connect,
    ducktypes,
)
from duckplus.relation.core import Expression

import pyarrow as pa


pytestmark = pytest.mark.mutable_with_approval


@pytest.fixture()
def connection() -> DuckConnection:
    with connect() as conn:
        yield conn


def table_rows(table: pa.Table) -> list[tuple[object, ...]]:
    """Return ordered row tuples from an Arrow table."""

    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]
    if not columns:
        return [tuple() for _ in range(table.num_rows)]
    return list(zip(*columns, strict=True))


def test_column_expression_end_to_end(connection: DuckConnection) -> None:
    """ColumnExpression integrates with filtering, projections, aggregates, and ordering."""

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                ('a', 1),
                ('a', 2),
                ('b', 3)
            ) AS t(category, value)
            """
        )
    )

    category = col("category", duck_type=ducktypes.Varchar)
    value = col("value", duck_type=ducktypes.Integer)

    aggregated = (
        rel
        .filter(value > 1)
        .project({"category": category, "value": value})
        .add_columns(category_copy=col("category"))
        .transform_columns(value=col("value"))
        .aggregate(
            category,
            total=AggregateExpression.sum(value),
            largest=AggregateExpression.function("first", value).with_order_by((value, "desc")),
            category_label=category,
        )
        .order_by((col("total"), "desc"))
    )

    assert aggregated.columns == ["category", "total", "largest", "category_label"]
    assert aggregated.relation.fetchall() == [("b", 3, 3, "b"), ("a", 2, 2, "a")]
    assert aggregated.column_type_markers == [
        ducktypes.Varchar,
        ducktypes.Integer,
        ducktypes.Unknown,
        ducktypes.Varchar,
    ]


def test_relation_fluent_pipeline(connection: DuckConnection) -> None:
    rel = Relation(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                ('a', 2),
                ('b', 4),
                ('b', 2)
            ) AS t(category, score)
            """
        )
    )

    result = (
        rel
        .select("category", doubled=lambda c: c["score"] * 2)
        .where(lambda c: c.doubled >= 4)
        .mutate(doubled=lambda c: c.doubled + 1, flagged=lambda c: c.doubled > 4)
        .aggregate(
            "category",
            total=lambda agg: agg.sum("doubled"),
            observed=lambda agg: agg.count("doubled", distinct=True),
        )
        .order_by(lambda c: c.total.desc())
    )

    assert result.columns == ["category", "total", "observed"]
    assert result.relation.fetchall() == [("b", 14, 2), ("a", 5, 1)]


def test_relation_column_descriptor_access(connection: DuckConnection) -> None:
    rel = Relation(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                (1, 'alpha', 10),
                (2, 'alpha', 5)
            ) AS t(id, category, score)
            """
        )
    ).select(
        id=col("id", duck_type=ducktypes.Integer),
        category=col("category", duck_type=ducktypes.Varchar),
        score=col("score", duck_type=ducktypes.Integer),
    )

    columns = rel.columns
    assert list(columns) == ["id", "category", "score"]

    score_expr = columns.score
    assert isinstance(score_expr, Expression)
    assert score_expr.marker is ducktypes.Integer
    assert rel.c["score"].sql == score_expr.sql
    assert rel.columns == ["id", "category", "score"]

    with pytest.raises(KeyError):
        _ = columns["missing"]
    with pytest.raises(AttributeError):
        _ = getattr(columns, "missing")

    aggregated = (
        rel.aggregate(
            "category",
            total=lambda agg: agg.sum(agg.c.score),
            passing=lambda agg: agg.count(agg.c.score).with_filter(
                agg.c.score > agg.columns.literal(6)
            ),
        )
        .order_by(lambda c: c.total.desc())
    )

    assert aggregated.columns == ["category", "total", "passing"]
    assert aggregated.relation.fetchall() == [("alpha", 15, 1)]


def test_expression_arithmetic_comparisons_and_lambda_projection(
    connection: DuckConnection,
) -> None:
    relation = Relation(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                (2, 3, 1),
                (4, 2, 2),
                (3, 1, 3)
            ) AS t(amount, factor, offset_value)
            """
        )
    ).select(
        amount=col("amount", duck_type=ducktypes.Integer),
        factor=col("factor", duck_type=ducktypes.Integer),
        offset=col("offset_value", duck_type=ducktypes.Integer),
    )

    columns = relation.columns
    combined = columns.amount + columns.factor
    shifted = columns.amount - columns.literal(1)
    assert isinstance(combined, Expression)
    assert isinstance(shifted, Expression)

    predicate = combined >= columns.literal(4)
    assert isinstance(predicate, FilterExpression)

    projected = (
        relation
        .select(
            lambda c: {
                "amount": c.amount,
                "sum": c.amount + c.factor,
                "difference": c.amount - c.offset,
                "product": c.amount * c.factor,
                "remainder": (c.amount + c.literal(5)) % (c.factor + c.literal(1)),
            }
        )
        .where(lambda c: (c.product >= c.sum) & (c.difference > c.literal(0)))
        .order_by(lambda c: c.product.desc())
    )

    assert projected.columns == ["amount", "sum", "difference", "product", "remainder"]
    assert projected.relation.fetchall() == [(4, 6, 2, 8, 0), (2, 5, 1, 6, 3)]


def test_fetch_typed_uses_column_markers(connection: DuckConnection) -> None:
    """fetch_typed() projects typed tuples derived from column metadata."""

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                ('a', 1),
                ('b', 2),
                ('a', 3)
            ) AS t(category, quantity)
            """
        )
    )

    category = col("category", duck_type=ducktypes.Varchar)
    quantity = col("quantity", duck_type=ducktypes.Integer)

    typed = rel.project({"category": category, "quantity": quantity})
    assert typed.fetch_typed() == [("a", 1), ("b", 2), ("a", 3)]

    summary = (
        typed
        .aggregate(category, total=AggregateExpression.sum(quantity))
        .order_by((col("category", duck_type=ducktypes.Varchar), "asc"))
    )
    assert summary.fetch_typed() == [("a", 4), ("b", 2)]


def test_fetch_typed_rejects_column_arguments(connection: DuckConnection) -> None:
    """fetch_typed() requires callers to project columns ahead of time."""

    rel = DuckRel(connection.raw.sql("SELECT 1 AS id"))

    with pytest.raises(TypeError):
        rel.fetch_typed("id")  # type: ignore[arg-type]


def test_project_validates_declared_type(connection: DuckConnection) -> None:
    """Explicit column typing is validated against the relation schema."""

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES (1, 'x')) AS t(identifier, label)
            """
        )
    )

    with pytest.raises(
        TypeError,
        match=r"Projection column 'identifier' is typed as INTEGER but was declared as VARCHAR",
    ):
        rel.project({"identifier": col("identifier", duck_type=ducktypes.Varchar)})


def test_column_markers_propagate_through_pipeline(connection: DuckConnection) -> None:
    """Column type markers follow projections, transforms, aggregates, and additions."""

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT *
            FROM (VALUES
                ('basic', 5, 1.5),
                ('basic', 7, 2.0)
            ) AS source(category, quantity, ratio)
            """
        )
    )

    assert rel.column_type_markers == [ducktypes.Unknown, ducktypes.Unknown, ducktypes.Unknown]

    category = col("category", duck_type=ducktypes.Varchar)
    quantity = col("quantity", duck_type=ducktypes.Integer)

    typed = rel.project({"category": category, "quantity": quantity})
    assert typed.column_type_markers == [ducktypes.Varchar, ducktypes.Integer]

    transformed = typed.transform_columns(quantity=col("quantity"))
    assert transformed.column_type_markers == [ducktypes.Varchar, ducktypes.Integer]

    augmented = transformed.add_columns(
        quantity_copy=col("quantity"),
        total=col("quantity", duck_type=ducktypes.Integer),
    )
    assert augmented.column_type_markers == [
        ducktypes.Varchar,
        ducktypes.Integer,
        ducktypes.Integer,
        ducktypes.Integer,
    ]

    aggregated = augmented.aggregate(
        category,
        quantity_sum=AggregateExpression.sum(col("quantity")),
        observed=AggregateExpression.count(),
    )

    assert aggregated.columns == ["category", "quantity_sum", "observed"]
    assert aggregated.column_type_markers == [
        ducktypes.Varchar,
        ducktypes.Numeric,
        ducktypes.BigInt,
    ]


def test_sum_rejects_text_column() -> None:
    """Typed column expressions reject invalid aggregate input types."""

    text_column = col("category", duck_type=ducktypes.Varchar)

    with pytest.raises(TypeError, match=r"SUM aggregate requires numeric arguments"):
        AggregateExpression.sum(text_column)


def test_order_by_rejects_non_comparable(connection: DuckConnection) -> None:
    """order_by() validates type information when provided."""

    rel = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (VALUES (BLOB 'abc')) AS t(payload)
            """
        )
    )

    blob_column = col("payload", duck_type=ducktypes.Blob)

    with pytest.raises(TypeError, match=r"order_by\(\) requires comparable column types"):
        rel.order_by((blob_column, "asc"))


def test_exploratory_feature_engineering_pipeline(connection: DuckConnection) -> None:
    """Integration-style DuckRel feature engineering flow covering joins and projections."""

    events = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (501, 2001, 'viewed', 'web', TIMESTAMP '2024-01-10 08:00:00'),
                    (502, 2002, 'viewed', 'email', TIMESTAMP '2024-01-11 09:15:00'),
                    (503, 2003, 'clicked', 'web', TIMESTAMP '2024-01-13 10:45:00'),
                    (504, 2004, 'purchased', 'retail', TIMESTAMP '2024-01-18 12:30:00')
            ) AS events(event_id, user_id, event_type, channel, occurred_at)
            """
        )
    )

    users = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (2001, 'active', TIMESTAMP '2023-12-20 10:00:00', 'NA'),
                    (2002, 'inactive', TIMESTAMP '2023-12-01 11:30:00', 'NA'),
                    (2003, 'active', TIMESTAMP '2023-11-05 15:15:00', 'EMEA'),
                    (2004, 'active', TIMESTAMP '2023-10-01 09:30:00', 'LATAM')
            ) AS profiles(user_id, status, first_purchase_at, region)
            """
        )
    )

    loyalty = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (2001, 'gold'),
                    (2003, 'silver'),
                    (2004, 'platinum'),
                    (2005, 'gold')
            ) AS loyalty(user_id, tier)
            """
        )
    )

    channel_map = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    ('web', 'Web App'),
                    ('email', 'Lifecycle Email'),
                    ('retail', 'Retail Store')
            ) AS mapping(channel, source_name)
            """
        )
    )

    eligible_loyalty = loyalty.filter('"tier" IN (?, ?)', "gold", "platinum").project(
        {
            "user_id": '"user_id"',
            "loyalty_tier": 'upper("tier")',
        }
    )

    active_profiles = users.project(
        {
            "user_id": '"user_id"',
            "status": 'upper("status")',
            "first_purchase_at": '"first_purchase_at"',
            "region": '"region"',
        }
    ).filter('"status" = ?', "ACTIVE")

    curated = (
        events
        .semi_join(eligible_loyalty)
        .natural_inner(eligible_loyalty)
        .natural_inner(active_profiles)
        .natural_inner(channel_map)
        .project(
            {
                "user_id": '"user_id"',
                "event_id": '"event_id"',
                "loyalty_tier": 'upper("loyalty_tier")',
                "engagement_label": "upper(\"event_type\") || ' - ' || \"source_name\"",
                "tenure_days": "datediff('day', \"first_purchase_at\", \"occurred_at\")",
                "region": '"region"',
                "occurred_at": '"occurred_at"',
            }
        )
        .order_by(occurred_at="asc", event_id="desc")
        .limit(5)
    )

    materialized = curated.materialize(strategy=ParquetMaterializeStrategy(cleanup=True))
    table = materialized.require_table()

    assert materialized.path is None
    assert table.schema.names == [
        "user_id",
        "event_id",
        "loyalty_tier",
        "engagement_label",
        "tenure_days",
        "region",
        "occurred_at",
    ]

    assert table_rows(table) == [
        (
            2001,
            501,
            "GOLD",
            "VIEWED - Web App",
            21,
            "NA",
            datetime(2024, 1, 10, 8, 0, 0),
        ),
        (
            2004,
            504,
            "PLATINUM",
            "PURCHASED - Retail Store",
            109,
            "LATAM",
            datetime(2024, 1, 18, 12, 30, 0),
        ),
    ]


def test_exploratory_backlog_snapshot(connection: DuckConnection) -> None:
    """Integration-style DuckRel snapshot building flow covering anti-joins and materialize targets."""

    orders = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (9001, 'ACME', 'open', 120.50, DATE '2024-01-05'),
                    (9002, 'ZENITH', 'closed', 200.00, DATE '2024-01-02'),
                    (9003, 'ACME', 'backorder', 320.00, DATE '2024-01-07'),
                    (9004, 'OMEGA', 'open', 75.00, DATE '2024-01-09'),
                    (9005, 'LUMEN', 'open', 60.00, DATE '2024-01-11')
            ) AS orders(order_id, account, status, amount, placed_at)
            """
        )
    )

    shipments = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (9001, DATE '2024-01-06')
            ) AS shipments(order_id, shipped_at)
            """
        )
    )

    risk = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    ('ACME', 'high'),
                    ('OMEGA', 'medium')
            ) AS risk(account, risk_level)
            """
        )
    )

    risk_profiles = risk.project(
        {
            "account": '"account"',
            "risk_level": 'upper("risk_level")',
        }
    )

    backlog = (
        orders
        .filter('"status" IN (?, ?)', "open", "backorder")
        .anti_join(shipments)
        .natural_left(risk_profiles)
    )

    prioritized = backlog.project(
        {
            "order_id": '"order_id"',
            "account": '"account"',
            "status": '"status"',
            "amount": 'cast("amount" AS DOUBLE)',
            "placed_at": 'cast("placed_at" AS TIMESTAMP)',
            "risk_level": "coalesce(upper(\"risk_level\"), 'LOW')",
            "priority_bucket": "CASE\n                WHEN coalesce(upper(\"risk_level\"), 'LOW') = 'HIGH' THEN 'Expedite'\n                WHEN \"amount\" >= 300 THEN 'Review'\n                WHEN coalesce(upper(\"risk_level\"), 'LOW') = 'MEDIUM' THEN 'Monitor'\n                ELSE 'Observe'\n            END",
            "priority_score": "CASE\n                WHEN coalesce(upper(\"risk_level\"), 'LOW') = 'HIGH' THEN 1\n                WHEN \"amount\" >= 300 THEN 2\n                WHEN coalesce(upper(\"risk_level\"), 'LOW') = 'MEDIUM' THEN 3\n                ELSE 4\n            END",
        }
    )

    ordered = prioritized.order_by(priority_score="asc", amount="desc")
    limited = ordered.limit(2)
    final_snapshot = limited.project_columns(
        "order_id",
        "account",
        "risk_level",
        "status",
        "amount",
        "placed_at",
        "priority_bucket",
    )

    with connect() as snapshot:
        materialized = final_snapshot.materialize(
            strategy=ArrowMaterializeStrategy(retain_table=False),
            into=snapshot.raw,
        )

        assert materialized.table is None
        rel_snapshot = materialized.require_relation()
        assert rel_snapshot.columns == [
            "order_id",
            "account",
            "risk_level",
            "status",
            "amount",
            "placed_at",
            "priority_bucket",
        ]

        snapshot_table = rel_snapshot.materialize().require_table()

    assert table_rows(snapshot_table) == [
        (
            9003,
            "ACME",
            "HIGH",
            "backorder",
            320.0,
            datetime(2024, 1, 7, 0, 0),
            "Expedite",
        ),
        (
            9004,
            "OMEGA",
            "MEDIUM",
            "open",
            75.0,
            datetime(2024, 1, 9, 0, 0),
            "Monitor",
        ),
    ]


def test_multi_stage_event_budget_enrichment(connection: DuckConnection) -> None:
    """Complex dataflow blending natural ASOF joins with explicit JoinSpec pipelines."""

    events = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (1001, 201, 'email', TIMESTAMP '2024-01-10 09:00:00', 25.0),
                    (1002, 202, 'web', TIMESTAMP '2024-01-10 11:00:00', 40.0),
                    (1003, 203, 'email', TIMESTAMP '2024-01-10 15:00:00', 55.0),
                    (1004, 204, 'web', TIMESTAMP '2024-01-11 13:30:00', 15.0)
            ) AS events(event_id, user_id, channel, occurred_at, revenue)
            """
        )
    )

    budgets = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    ('email', TIMESTAMP '2024-01-10 08:00:00', 1000),
                    ('email', TIMESTAMP '2024-01-10 12:00:00', 1200),
                    ('web', TIMESTAMP '2024-01-10 07:30:00', 1500),
                    ('web', TIMESTAMP '2024-01-11 09:00:00', 1600)
            ) AS budgets(channel, effective_at, budget_amount)
            """
        )
    )

    segments = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (201, 'NA', TIMESTAMP '2023-12-01 00:00:00'),
                    (202, 'EMEA', TIMESTAMP '2024-01-09 00:00:00'),
                    (203, 'NA', TIMESTAMP '2024-01-05 00:00:00'),
                    (204, 'APAC', TIMESTAMP '2024-01-12 00:00:00')
            ) AS segments(user_id, region, segment_start)
            """
        )
    )

    targets = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    ('NA', 'email', 900),
                    ('NA', 'web', 1400),
                    ('EMEA', 'web', 1300),
                    ('APAC', 'web', 1700)
            ) AS targets(region, channel, quota_amount)
            """
        )
    )

    events_with_budget = events.natural_asof(
        budgets,
        order=AsofOrder(left="occurred_at", right="effective_at"),
    )

    events_with_segments = events_with_budget.left_outer(
        segments,
        JoinSpec(
            equal_keys=[("user_id", "user_id")],
            predicates=[column("occurred_at") >= column("segment_start")],
        ),
    )

    enriched = events_with_segments.left_outer(
        targets,
        JoinSpec(
            equal_keys=[("channel", "channel"), ("region", "region")],
            predicates=[ExpressionPredicate('r."quota_amount" >= 1000')],
        ),
    ).project(
        {
            "event_id": '"event_id"',
            "user_id": '"user_id"',
            "channel": 'upper("channel")',
            "occurred_at": '"occurred_at"',
            "revenue": '"revenue"',
            "budget_refresh_at": '"effective_at"',
            "budget_amount": '"budget_amount"',
            "region": '"region"',
            "segment_started_at": '"segment_start"',
            "quota_amount": '"quota_amount"',
            "budget_gap": '"budget_amount" - coalesce("quota_amount", 0)',
        }
    ).order_by(event_id="asc")

    table = enriched.materialize(strategy=ArrowMaterializeStrategy()).require_table()

    assert table.schema.names == [
        "event_id",
        "user_id",
        "channel",
        "occurred_at",
        "revenue",
        "budget_refresh_at",
        "budget_amount",
        "region",
        "segment_started_at",
        "quota_amount",
        "budget_gap",
    ]

    assert table_rows(table) == [
        (
            1001,
            201,
            "EMAIL",
            datetime(2024, 1, 10, 9, 0, 0),
            Decimal("25.0"),
            datetime(2024, 1, 10, 8, 0, 0),
            1000,
            "NA",
            datetime(2023, 12, 1, 0, 0, 0),
            None,
            1000,
        ),
        (
            1002,
            202,
            "WEB",
            datetime(2024, 1, 10, 11, 0, 0),
            Decimal("40.0"),
            datetime(2024, 1, 10, 7, 30, 0),
            1500,
            "EMEA",
            datetime(2024, 1, 9, 0, 0, 0),
            1300,
            200,
        ),
        (
            1003,
            203,
            "EMAIL",
            datetime(2024, 1, 10, 15, 0, 0),
            Decimal("55.0"),
            datetime(2024, 1, 10, 12, 0, 0),
            1200,
            "NA",
            datetime(2024, 1, 5, 0, 0, 0),
            None,
            1200,
        ),
        (
            1004,
            204,
            "WEB",
            datetime(2024, 1, 11, 13, 30, 0),
            Decimal("15.0"),
            datetime(2024, 1, 11, 9, 0, 0),
            1600,
            None,
            None,
            None,
            1600,
        ),
    ]
