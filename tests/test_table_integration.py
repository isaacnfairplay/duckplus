from __future__ import annotations

"""Integration-style exploratory tests exercising DuckTable flows."""

from datetime import datetime

import pytest

from duckplus import DuckConnection, DuckRel, DuckTable, connect


pytestmark = pytest.mark.mutable_with_approval


@pytest.fixture()
def connection() -> DuckConnection:
    with connect() as conn:
        yield conn


def fetch_table_rows(conn: DuckConnection, table: str) -> list[tuple[object, ...]]:
    """Return ordered rows from *table* for assertion-friendly comparisons."""

    rows = conn.raw.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    return [tuple(row) for row in rows]



def test_exploratory_dimension_ingestion_flow(connection: DuckConnection) -> None:
    """Integration-style dimension pipeline covering append and anti-join."""

    # Exploratory integration context (mutable with approval): the scenario
    # intentionally mirrors a warehouse landing step followed by a deduplicated
    # incremental feed so maintainers can evolve the flow if future helpers are
    # introduced.

    # An integration-style scenario that simulates a dimension table pipeline.
    #
    # The test loads an initial landing relation containing messy casing and
    # derived segment data, normalizes it through DuckRel transformations, and
    # appends it into the warehouse dimension. It then stages another relation
    # that mixes duplicate and truly new business keys and ensures the anti-join
    # insert only brings in the unseen entity. The assertions confirm the
    # persisted table matches the curated expectations rather than the raw
    # landing order or casing.

    connection.raw.execute(
        """
        CREATE TABLE dim_customers(
            customer_id INTEGER,
            email VARCHAR,
            is_active BOOLEAN,
            first_order_id INTEGER,
            segment VARCHAR
        )
        """
    )
    table = DuckTable(connection, "dim_customers")

    landing = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (1, 'ALPHA@example.com', 'active', 10),
                    (2, 'BETA@example.com', 'inactive', 20)
            ) AS landing(customer_id, email, status, first_order_id)
            """
        )
    )
    curated = landing.project(
        {
            "customer_id": "customer_id",
            "email": "lower(email)",
            "is_active": "status = 'active'",
            "first_order_id": "first_order_id",
            "segment": "CASE WHEN first_order_id < 15 THEN 'early' ELSE 'standard' END",
        }
    )
    table.append(curated)

    restage = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (2, 'beta@example.com', 'active', 20),
                    (3, 'GAMMA@example.com', 'active', 22)
            ) AS stage(customer_id, email, status, first_order_id)
            """
        )
    )
    deduplicated = restage.project(
        {
            "customer_id": "customer_id",
            "email": "lower(email)",
            "is_active": "status = 'active'",
            "first_order_id": "first_order_id",
            "segment": "CASE WHEN first_order_id < 15 THEN 'early' ELSE 'standard' END",
        }
    )
    inserted = table.insert_antijoin(deduplicated, keys=["customer_id"])
    assert inserted == 1

    assert fetch_table_rows(connection, "dim_customers") == [
        (1, "alpha@example.com", True, 10, "early"),
        (2, "beta@example.com", False, 20, "standard"),
        (3, "gamma@example.com", True, 22, "standard"),
    ]



def test_exploratory_streaming_fact_ingestion_flow(
    connection: DuckConnection,
) -> None:
    """Integration-style fact ingestion run covering continuous ID helpers."""

    # Exploratory integration context (mutable with approval): this sequence
    # emulates a streaming catch-up pipeline to validate how helpers compose.

    # A fact table ingestion walk-through that chains multiple DuckRel
    # features.
    #
    # The flow builds a curated seed dataset by joining to a reference relation
    # that standardizes source names before appending into the fact table. A
    # second staging relation reuses the join, adds filtering, and is ingested
    # through the continuous-ID helper to mimic streaming catch-up behaviour.
    # A final relation exercises the inclusive branch of the ID filter to
    # ensure duplicates at the boundary are ignored while newly observed IDs
    # land.

    connection.raw.execute(
        """
        CREATE TABLE fact_orders(
            event_id INTEGER,
            order_id INTEGER,
            amount DOUBLE,
            source VARCHAR,
            processed_at TIMESTAMP
        )
        """
    )
    table = DuckTable(connection, "fact_orders")

    origin_map = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    ('web', 'WEB'),
                    ('store', 'STORE'),
                    ('kiosk', 'KIOSK')
            ) AS mapping(origin, source)
            """
        )
    )

    seed_events = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (100, 9001, 25.50, 'web', TIMESTAMP '2024-01-01 12:00:00'),
                    (101, 9002, 40.00, 'store', TIMESTAMP '2024-01-01 13:30:00')
            ) AS events(event_id, order_id, amount, origin, processed_at)
            """
        )
    )
    normalized_seed = seed_events.natural_inner(origin_map).project_columns(
        "event_id",
        "order_id",
        "amount",
        "source",
        "processed_at",
    )
    table.append(normalized_seed)

    incremental_events = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (101, 9002, 40.00, 'store', TIMESTAMP '2024-01-01 13:30:00'),
                    (102, 9003, 55.75, 'web', TIMESTAMP '2024-01-01 14:45:00'),
                    (103, 9004, 60.00, 'kiosk', TIMESTAMP '2024-01-01 16:00:00')
            ) AS events(event_id, order_id, amount, origin, processed_at)
            """
        )
    )
    incremental_curated = (
        incremental_events.natural_inner(origin_map)
        .filter("amount >= ?", 40)
        .project_columns("event_id", "order_id", "amount", "source", "processed_at")
    )
    inserted = table.insert_by_continuous_id(incremental_curated, id_column="event_id")
    assert inserted == 2

    catch_up_events = DuckRel(
        connection.raw.sql(
            """
            SELECT * FROM (
                VALUES
                    (103, 9004, 60.00, 'kiosk', TIMESTAMP '2024-01-01 16:00:00'),
                    (104, 9005, 20.00, 'web', TIMESTAMP '2024-01-01 17:15:00')
            ) AS events(event_id, order_id, amount, origin, processed_at)
            """
        )
    )
    catch_up_curated = catch_up_events.natural_inner(origin_map).project_columns(
        "event_id",
        "order_id",
        "amount",
        "source",
        "processed_at",
    )
    inserted_catch_up = table.insert_by_continuous_id(
        catch_up_curated, id_column="event_id", inclusive=True
    )
    assert inserted_catch_up == 1

    assert fetch_table_rows(connection, "fact_orders") == [
        (100, 9001, 25.5, "WEB", datetime(2024, 1, 1, 12, 0, 0)),
        (101, 9002, 40.0, "STORE", datetime(2024, 1, 1, 13, 30, 0)),
        (102, 9003, 55.75, "WEB", datetime(2024, 1, 1, 14, 45, 0)),
        (103, 9004, 60.0, "KIOSK", datetime(2024, 1, 1, 16, 0, 0)),
        (104, 9005, 20.0, "WEB", datetime(2024, 1, 1, 17, 15, 0)),
    ]
