"""Guided tests for the sanitised traceability pipeline demo."""

from __future__ import annotations

from datetime import datetime

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import traceability_pipeline


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
