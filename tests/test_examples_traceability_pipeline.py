"""Guided tests for the sanitised traceability pipeline demo."""

from __future__ import annotations

from datetime import datetime

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import traceability_pipeline
from duckplus.relation import Relation


@pytest.fixture()
def demo_data() -> traceability_pipeline.TraceabilityDemoData:
    """Seed the demo relations for each test case."""

    manager = DuckCon()
    with manager:
        yield traceability_pipeline.load_demo_relations(manager)


def test_rank_program_candidates_prioritises_longest_match(
    demo_data: traceability_pipeline.TraceabilityDemoData,
) -> None:
    """The ranking CTE prefers longer fragments and recent activity."""

    catalog = demo_data.program_catalog
    log = demo_data.activity_log
    relation = traceability_pipeline.rank_program_candidates(catalog, log, "XYZ1-001")
    rows = relation.relation.fetchall()
    assert relation.columns == (
        "program_name",
        "line_label",
        "fragment_length",
        "seen_count",
        "last_seen",
    )
    assert rows == [
        ("alpha_run", "LINE_A", 4, 3, datetime(2024, 5, 3, 9, 10)),
    ]


def test_collect_panel_companions_returns_panel_scope(
    demo_data: traceability_pipeline.TraceabilityDemoData,
) -> None:
    """Panel companions include alternates from matching sources."""

    panel = demo_data.panel_events
    alternate = demo_data.alternate_events
    relation = traceability_pipeline.collect_panel_companions(panel, alternate, "XYZ1-001")
    rows = relation.relation.fetchall()
    assert relation.columns == (
        "scan_code",
        "panel_token",
        "board_slot",
        "source_kind",
    )
    assert rows == [
        ("XYZ1-001", "panel-001", 1, "primary"),
        ("XYZ1-001", None, None, "alternate"),
        ("XYZ1-002", "panel-001", 2, "primary"),
    ]


def test_repair_unit_costs_replaces_zero_cost_rows(
    demo_data: traceability_pipeline.TraceabilityDemoData,
) -> None:
    """The cost-repair pipeline recomputes values using recent prices."""

    events = demo_data.unit_events
    prices = demo_data.price_snapshots
    relation = traceability_pipeline.repair_unit_costs(events, prices)
    rows = relation.relation.fetchall()
    assert relation.columns == (
        "record_id",
        "item_token",
        "quantity",
        "final_cost",
        "route_hint",
        "station_hint",
    )
    assert rows == [
        (1, "widget", 3, pytest.approx(8.1), "route-1", "station-7"),
        (2, "widget", 2, pytest.approx(6.0), "route-1", "station-7"),
        (3, "gadget", 1, pytest.approx(4.0), None, None),
        (4, "gadget", 5, pytest.approx(22.5), None, None),
    ]


def _build_large_traceability_demo(
    demo_data: traceability_pipeline.TraceabilityDemoData, copies: int
) -> traceability_pipeline.TraceabilityDemoData:
    duckcon = demo_data.program_catalog.duckcon
    connection = duckcon.connection

    program_catalog_sql = demo_data.program_catalog.relation.sql_query()
    activity_log_sql = demo_data.activity_log.relation.sql_query()
    panel_events_sql = demo_data.panel_events.relation.sql_query()
    alternate_events_sql = demo_data.alternate_events.relation.sql_query()
    unit_events_sql = demo_data.unit_events.relation.sql_query()
    price_snapshots_sql = demo_data.price_snapshots.relation.sql_query()

    expanded_program_catalog = connection.sql(
        f"""
        WITH base AS ({program_catalog_sql})
        SELECT
            CASE WHEN copy_index = 0 THEN program_name
                 ELSE program_name || '_' || copy_index::VARCHAR END AS program_name,
            CASE WHEN copy_index = 0 THEN line_label
                 ELSE line_label || '_' || copy_index::VARCHAR END AS line_label,
            CASE WHEN copy_index = 0 THEN code_fragment
                 ELSE 'copy_' || copy_index::VARCHAR END AS code_fragment
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_activity_log = connection.sql(
        f"""
        WITH base AS ({activity_log_sql})
        SELECT
            CASE WHEN copy_index = 0 THEN program_name
                 ELSE program_name || '_' || copy_index::VARCHAR END AS program_name,
            CASE WHEN copy_index = 0 THEN line_label
                 ELSE line_label || '_' || copy_index::VARCHAR END AS line_label,
            CASE WHEN copy_index = 0 THEN recorded_at
                 ELSE recorded_at + (copy_index * INTERVAL 1 MINUTE) END AS recorded_at
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_panel_events = connection.sql(
        f"""
        WITH base AS ({panel_events_sql})
        SELECT
            CASE WHEN copy_index = 0 THEN source_line
                 ELSE source_line || '_' || copy_index::VARCHAR END AS source_line,
            CASE WHEN copy_index = 0 THEN panel_token
                 ELSE panel_token || '_' || copy_index::VARCHAR END AS panel_token,
            board_slot + (copy_index * 10) AS board_slot,
            CASE WHEN copy_index = 0 THEN scan_code
                 ELSE 'copy_' || copy_index::VARCHAR || '_' || scan_code END AS scan_code
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_alternate_events = connection.sql(
        f"""
        WITH base AS ({alternate_events_sql})
        SELECT
            CASE WHEN copy_index = 0 THEN source_line
                 ELSE source_line || '_' || copy_index::VARCHAR END AS source_line,
            CASE WHEN copy_index = 0 THEN scan_code
                 ELSE 'copy_' || copy_index::VARCHAR || '_' || scan_code END AS scan_code
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_unit_events = connection.sql(
        f"""
        WITH base AS ({unit_events_sql})
        SELECT
            event_id + (copy_index * 1000) AS event_id,
            CASE WHEN copy_index = 0 THEN item_token
                 ELSE item_token || '_' || copy_index::VARCHAR END AS item_token,
            quantity,
            CASE WHEN copy_index = 0 THEN raw_cost
                 ELSE raw_cost + (copy_index * 0.5) END AS raw_cost,
            CASE WHEN copy_index = 0 THEN route_hint
                 ELSE COALESCE(route_hint, 'route') || '_' || copy_index::VARCHAR END AS route_hint,
            CASE WHEN copy_index = 0 THEN station_hint
                 ELSE COALESCE(station_hint, 'station') || '_' || copy_index::VARCHAR END AS station_hint
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    expanded_price_snapshots = connection.sql(
        f"""
        WITH base AS ({price_snapshots_sql})
        SELECT
            CASE WHEN copy_index = 0 THEN item_token
                 ELSE item_token || '_' || copy_index::VARCHAR END AS item_token,
            CASE WHEN copy_index = 0 THEN route_hint
                 ELSE COALESCE(route_hint, 'route') || '_' || copy_index::VARCHAR END AS route_hint,
            CASE WHEN copy_index = 0 THEN station_hint
                 ELSE COALESCE(station_hint, 'station') || '_' || copy_index::VARCHAR END AS station_hint,
            CASE WHEN copy_index = 0 THEN unit_cost
                 ELSE unit_cost + (copy_index * 0.1) END AS unit_cost,
            CASE WHEN copy_index = 0 THEN captured_at
                 ELSE captured_at + (copy_index * INTERVAL 1 MINUTE) END AS captured_at
        FROM base
        CROSS JOIN generate_series(0, {copies} - 1) AS copies(copy_index)
        """
    )

    return traceability_pipeline.TraceabilityDemoData(
        program_catalog=Relation.from_relation(duckcon, expanded_program_catalog),
        activity_log=Relation.from_relation(duckcon, expanded_activity_log),
        panel_events=Relation.from_relation(duckcon, expanded_panel_events),
        alternate_events=Relation.from_relation(duckcon, expanded_alternate_events),
        unit_events=Relation.from_relation(duckcon, expanded_unit_events),
        price_snapshots=Relation.from_relation(duckcon, expanded_price_snapshots),
    )


def test_traceability_demo_handles_high_volume_relations(
    demo_data: traceability_pipeline.TraceabilityDemoData,
) -> None:
    copies = 30
    expanded_demo = _build_large_traceability_demo(demo_data, copies)

    ranked = traceability_pipeline.rank_program_candidates(
        expanded_demo.program_catalog, expanded_demo.activity_log, "XYZ1-001"
    )
    ranked_rows = ranked.relation.fetchall()
    assert ranked_rows == [
        ("alpha_run", "LINE_A", 4, 3, datetime(2024, 5, 3, 9, 10)),
    ]

    companions = traceability_pipeline.collect_panel_companions(
        expanded_demo.panel_events,
        expanded_demo.alternate_events,
        "XYZ1-001",
    )
    companion_rows = (
        companions.order_by("scan_code", "panel_token", "board_slot")
        .relation.fetchall()
    )
    assert companion_rows == [
        ("XYZ1-001", "panel-001", 1, "primary"),
        ("XYZ1-001", None, None, "alternate"),
        ("XYZ1-002", "panel-001", 2, "primary"),
    ]

    repaired = traceability_pipeline.repair_unit_costs(
        expanded_demo.unit_events, expanded_demo.price_snapshots
    )
    repaired_rows = repaired.order_by("record_id").relation.fetchall()
    assert repaired_rows[:4] == [
        (1, "widget", 3, pytest.approx(8.1), "route-1", "station-7"),
        (2, "widget", 2, pytest.approx(6.0), "route-1", "station-7"),
        (3, "gadget", 1, pytest.approx(4.0), None, None),
        (4, "gadget", 5, pytest.approx(22.5), None, None),
    ]
