"""Guided tests for the sanitised traceability pipeline demo."""

from __future__ import annotations

from datetime import datetime

import pytest

from duckplus.duckcon import DuckCon
from duckplus.examples import traceability_pipeline
from duckplus.static_typed import ducktype


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
    if copies <= 0:
        msg = "copies must be a positive integer"
        raise ValueError(msg)

    def _timestamp_with_offset(column: str, minutes: int) -> object:
        base_timestamp = ducktype.Timestamp(column)
        if minutes == 0:
            return base_timestamp
        interval_sql = f"INTERVAL '{minutes}' MINUTE"
        return ducktype.Timestamp._raw(
            f"({base_timestamp.render()} + {interval_sql})",
            dependencies=base_timestamp.dependencies,
        )

    def _token_with_suffix(column: str, index: int) -> object:
        expression = ducktype.Varchar(column)
        if index == 0:
            return expression
        return expression + ducktype.Varchar.literal(f"_{index}")

    expanded_program_catalog = demo_data.program_catalog
    expanded_activity_log = demo_data.activity_log
    expanded_panel_events = demo_data.panel_events
    expanded_alternate_events = demo_data.alternate_events
    expanded_unit_events = demo_data.unit_events
    expanded_price_snapshots = demo_data.price_snapshots

    for index in range(1, copies):
        program_catalog_copy = (
            demo_data.program_catalog.select()
            .star(
                replace={
                    "program_name": _token_with_suffix("program_name", index),
                    "line_label": _token_with_suffix("line_label", index),
                    "code_fragment": ducktype.Varchar.literal(f"copy_{index}"),
                }
            )
            .from_()
        )
        expanded_program_catalog = expanded_program_catalog.union(program_catalog_copy)

        activity_log_copy = (
            demo_data.activity_log.select()
            .star(
                replace={
                    "program_name": _token_with_suffix("program_name", index),
                    "line_label": _token_with_suffix("line_label", index),
                    "recorded_at": _timestamp_with_offset("recorded_at", index),
                }
            )
            .from_()
        )
        expanded_activity_log = expanded_activity_log.union(activity_log_copy)

        panel_events_copy = (
            demo_data.panel_events.select()
            .star(
                replace={
                    "source_line": _token_with_suffix("source_line", index),
                    "panel_token": _token_with_suffix("panel_token", index),
                    "board_slot": (
                        ducktype.Numeric("board_slot")
                        + ducktype.Numeric.literal(index * 10)
                    ),
                    "scan_code": (
                        ducktype.Varchar.literal(f"copy_{index}_")
                        + ducktype.Varchar("scan_code")
                    ),
                }
            )
            .from_()
        )
        expanded_panel_events = expanded_panel_events.union(panel_events_copy)

        alternate_events_copy = (
            demo_data.alternate_events.select()
            .star(
                replace={
                    "source_line": _token_with_suffix("source_line", index),
                    "scan_code": (
                        ducktype.Varchar.literal(f"copy_{index}_")
                        + ducktype.Varchar("scan_code")
                    ),
                }
            )
            .from_()
        )
        expanded_alternate_events = expanded_alternate_events.union(alternate_events_copy)

        unit_events_copy = (
            demo_data.unit_events.select()
            .star(
                replace={
                    "event_id": (
                        ducktype.Numeric("event_id")
                        + ducktype.Numeric.literal(index * 1000)
                    ),
                    "item_token": _token_with_suffix("item_token", index),
                    "raw_cost": (
                        ducktype.Numeric("raw_cost")
                        + ducktype.Numeric.literal(index * 0.5)
                    ),
                    "route_hint": (
                        ducktype.Varchar("route_hint").coalesce("route")
                        + ducktype.Varchar.literal(f"_{index}")
                    ),
                    "station_hint": (
                        ducktype.Varchar("station_hint").coalesce("station")
                        + ducktype.Varchar.literal(f"_{index}")
                    ),
                }
            )
            .from_()
        )
        expanded_unit_events = expanded_unit_events.union(unit_events_copy)

        price_snapshots_copy = (
            demo_data.price_snapshots.select()
            .star(
                replace={
                    "item_token": _token_with_suffix("item_token", index),
                    "route_hint": (
                        ducktype.Varchar("route_hint").coalesce("route")
                        + ducktype.Varchar.literal(f"_{index}")
                    ),
                    "station_hint": (
                        ducktype.Varchar("station_hint").coalesce("station")
                        + ducktype.Varchar.literal(f"_{index}")
                    ),
                    "unit_cost": (
                        ducktype.Numeric("unit_cost")
                        + ducktype.Numeric.literal(index * 0.1)
                    ),
                    "captured_at": _timestamp_with_offset("captured_at", index),
                }
            )
            .from_()
        )
        expanded_price_snapshots = expanded_price_snapshots.union(price_snapshots_copy)

    return traceability_pipeline.TraceabilityDemoData(
        program_catalog=expanded_program_catalog,
        activity_log=expanded_activity_log,
        panel_events=expanded_panel_events,
        alternate_events=expanded_alternate_events,
        unit_events=expanded_unit_events,
        price_snapshots=expanded_price_snapshots,
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
