from __future__ import annotations

"""Integration-style exploratory tests exercising DuckRel flows."""

from datetime import datetime

import pytest

from duckplus import (
    ArrowMaterializeStrategy,
    DuckConnection,
    DuckRel,
    ParquetMaterializeStrategy,
    connect,
)

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
        .semi_join(eligible_loyalty, on=["user_id"])
        .inner_join(eligible_loyalty, on=["user_id"])
        .inner_join(active_profiles, on=["user_id"])
        .inner_join(channel_map, on=["channel"])
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
        .anti_join(shipments, on=["order_id"])
        .left_join(risk_profiles, on=["account"])
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
