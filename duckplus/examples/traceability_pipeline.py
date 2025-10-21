# pylint: disable=cyclic-import

"""Traceability data-flow demo with sanitised sample relations.

The helpers in this module showcase three patterns extracted from an internal
traceability investigation:

* ranking program candidates from a catalogue and recent execution log entries;
* gathering companion barcodes from panel events with an optional fallback to
  alternate capture sources; and
* repairing zero-cost material events by aggregating the latest reference
  prices.

All datasets are synthesised in-memory with anonymised column names so the
examples can be published safely.  Each helper returns an immutable
:class:`~duckplus.relation.Relation` so tests and documentation can embed the
resulting SQL behaviour without exposing production schemas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from duckplus.duckcon import DuckCon  # pylint: disable=cyclic-import
from duckplus.relation import Relation  # pylint: disable=cyclic-import
from duckplus.static_typed import ducktype

__all__ = [
    "TraceabilityDemoData",
    "load_demo_relations",
    "rank_program_candidates",
    "collect_panel_companions",
    "repair_unit_costs",
]


@dataclass(frozen=True)
class TraceabilityDemoData:
    """Container holding the synthetic relations for the traceability demo."""

    program_catalog: Relation
    activity_log: Relation
    panel_events: Relation
    alternate_events: Relation
    unit_events: Relation
    price_snapshots: Relation


def load_demo_relations(manager: DuckCon) -> TraceabilityDemoData:
    """Materialise deterministic relations that mirror the private pipelines.

    The demo relations deliberately hide production-specific details while
    preserving the control flow found in the internal scripts.  They enable the
    tests to document how DuckDB syntax such as CTEs, window functions, and
    conditional joins compose without revealing proprietary schemas.
    """

    connection = manager.connection

    program_catalog = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('alpha_run', 'LINE_A', 'XYZ1'),
                    ('alpha_run', 'LINE_A', 'XYZ'),
                    ('beta_scan', 'LINE_B', 'QR'),
                    ('beta_scan', 'LINE_B', 'Q'),
                    ('gamma_pass', 'LINE_C', 'LMN'),
                    ('gamma_pass', 'LINE_C', 'L')
            ) AS t(program_name, line_label, code_fragment)
            """,
        ),
    )

    activity_log = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('alpha_run', 'LINE_A', TIMESTAMP '2024-05-03 08:05:00'),
                    ('alpha_run', 'LINE_A', TIMESTAMP '2024-05-03 09:10:00'),
                    ('alpha_run', 'LINE_A', TIMESTAMP '2024-05-02 07:55:00'),
                    ('beta_scan', 'LINE_B', TIMESTAMP '2024-05-01 10:15:00'),
                    ('beta_scan', 'LINE_B', TIMESTAMP '2024-05-01 12:45:00'),
                    ('gamma_pass', 'LINE_C', TIMESTAMP '2024-04-30 06:20:00')
            ) AS t(program_name, line_label, recorded_at)
            """,
        ),
    )

    panel_events = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('LINE_A', 'panel-001', 1, 'XYZ1-001'),
                    ('LINE_A', 'panel-001', 2, 'XYZ1-002'),
                    ('LINE_A', 'panel-002', 1, 'XYZ1-ALT'),
                    ('LINE_B', 'panel-100', 1, 'QR9-001'),
                    ('LINE_B', 'panel-100', 2, 'QR9-002'),
                    ('LINE_ALT', 'panel-900', 1, 'ALT-PRIMARY')
            ) AS t(source_line, panel_token, board_slot, scan_code)
            """,
        ),
    )

    alternate_events = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('LINE_A', 'XYZ1-001'),
                    ('LINE_A', 'XYZ1-ALT'),
                    ('LINE_ALT', 'ALT-PRIMARY'),
                    ('LINE_ALT', 'ALT-FALLBACK')
            ) AS t(source_line, scan_code)
            """,
        ),
    )

    unit_events = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    (1, 'widget', 3, 0.0, 'route-1', 'station-7'),
                    (2, 'widget', 2, 6.0, 'route-1', 'station-7'),
                    (3, 'gadget', 1, 0.0, NULL, NULL),
                    (4, 'gadget', 5, 22.5, NULL, NULL)
            ) AS t(event_id, item_token, quantity, raw_cost, route_hint, station_hint)
            """,
        ),
    )

    price_snapshots = Relation.from_relation(
        manager,
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('widget', 'route-1', 'station-7', 2.5, TIMESTAMP '2024-05-01 08:00:00'),
                    ('widget', 'route-1', 'station-7', 2.7, TIMESTAMP '2024-05-03 08:00:00'),
                    ('widget', NULL, NULL, 1.4, TIMESTAMP '2024-05-01 07:00:00'),
                    ('widget', NULL, NULL, 1.5, TIMESTAMP '2024-05-02 07:00:00'),
                    ('gadget', NULL, NULL, 4.0, TIMESTAMP '2024-05-01 12:00:00')
            ) AS t(item_token, route_hint, station_hint, unit_cost, captured_at)
            """,
        ),
    )

    return TraceabilityDemoData(
        program_catalog=program_catalog,
        activity_log=activity_log,
        panel_events=panel_events,
        alternate_events=alternate_events,
        unit_events=unit_events,
        price_snapshots=price_snapshots,
    )

def rank_program_candidates(
    catalog: Relation, activity_log: Relation, scanned_code: str
) -> Relation:
    """Rank program candidates by fragment length and recent activity."""

    literal_code = ducktype.Varchar.literal(scanned_code)

    fragment_length = ducktype.Varchar("code_fragment").length()
    matches = (
        catalog
        .add(scanned_code=literal_code)
        .add(fragment_length=fragment_length)
        .filter(
            ducktype.Varchar("scanned_code").contains(
                ducktype.Varchar("code_fragment")
            )
        )
        .drop("scanned_code")
    )

    activity_summary = (
        activity_log.aggregate()
        .start_agg()
        .agg(ducktype.Numeric.Aggregate.count(), alias="seen_count")
        .agg(
            ducktype.Generic.Aggregate.max(ducktype.Generic("recorded_at")),
            alias="last_seen",
        )
        .by("program_name", "line_label")
    )

    scored = matches.left_join(
        activity_summary,
        on={"program_name": "program_name", "line_label": "line_label"},
    )
    scored = scored.rename(seen_count="_joined_seen_count").add(
        seen_count=ducktype.Numeric("_joined_seen_count").coalesce(0)
    ).drop("_joined_seen_count")

    ranked = scored.add(
        rn=(
            ducktype.row_number().over(
                partition_by=ducktype.Varchar("line_label"),
                order_by=[
                    (ducktype.Numeric("fragment_length"), "DESC"),
                    (ducktype.Numeric("seen_count"), "DESC"),
                    (ducktype.Generic("last_seen"), "DESC"),
                ],
            )
        )
    )

    filtered = (
        ranked.filter(ducktype.Numeric("rn") == 1)
        .drop("rn")
        .drop("code_fragment")
        .keep(
            "program_name",
            "line_label",
            "fragment_length",
            "seen_count",
            "last_seen",
        )
    )

    return filtered.order_by(
        "fragment_length DESC",
        "seen_count DESC",
        "last_seen DESC",
    )


def collect_panel_companions(
    panel_events: Relation, alternate_events: Relation, scanned_code: str
) -> Relation:
    """Gather companion barcodes from panel events with alternate fallbacks."""

    literal_code = ducktype.Varchar.literal(scanned_code)

    panel_with_target = panel_events.add(target_code=literal_code)
    target_panel_filtered = (
        panel_with_target
        .filter(
            ducktype.Varchar("scan_code")
            == ducktype.Varchar("target_code")
        )
        .keep("source_line", "panel_token")
    )
    target_panel = target_panel_filtered.distinct().materialize()

    panel_matches = (
        panel_events
        .join(target_panel)
        .add(source_kind=ducktype.Varchar.literal("primary"))
        .keep("scan_code", "panel_token", "board_slot", "source_kind")
    )

    alternate_with_target = alternate_events.add(target_code=literal_code)
    alternate_filtered = (
        alternate_with_target
        .filter(
            ducktype.Varchar("scan_code")
            == ducktype.Varchar("target_code")
        )
        .join(target_panel)
        .drop_if_exists("panel_token")
        .add(
            panel_token=ducktype.Generic.null(),
            board_slot=ducktype.Generic.null(),
            source_kind=ducktype.Varchar.literal("alternate"),
        )
        .keep("scan_code", "panel_token", "board_slot", "source_kind")
    )
    alternate_matches = alternate_filtered.distinct()

    return panel_matches.union(alternate_matches).order_by("scan_code")


def repair_unit_costs(events: Relation, price_snapshots: Relation) -> Relation:
    """Replace zero-cost events using the latest price snapshots."""

    source_events = events.rename(event_id="record_id")

    zero_cost = ducktype.Numeric("raw_cost").coalesce(0).abs() <= 0.0001
    non_zero_quantity = ducktype.Numeric("quantity") != 0
    split_events = source_events.add(needs_repair=zero_cost & non_zero_quantity)

    untouched = (
        split_events
        .filter(~ducktype.Boolean("needs_repair"))
        .rename(raw_cost="final_cost")
        .drop("needs_repair")
    )

    recent_prices = (
        price_snapshots.aggregate()
        .start_agg()
        .agg(
            ducktype.Numeric.Aggregate.max_by(
                ducktype.Numeric("unit_cost"),
                ducktype.Generic("captured_at"),
            ),
            alias="recent_unit_cost",
        )
        .by("item_token", "route_hint", "station_hint")
        .add(
            route_key=ducktype.Varchar("route_hint").coalesce("__NULL__"),
            station_key=ducktype.Varchar("station_hint").coalesce("__NULL__"),
        )
    )

    item_prices = (
        price_snapshots.aggregate()
        .start_agg()
        .agg(ducktype.Numeric("unit_cost").avg(), alias="fallback_unit_cost")
        .by("item_token")
    )

    repairs_needed = (
        split_events
        .filter(ducktype.Boolean("needs_repair"))
        .add(
            route_key=ducktype.Varchar("route_hint").coalesce("__NULL__"),
            station_key=ducktype.Varchar("station_hint").coalesce("__NULL__"),
        )
        .drop("needs_repair")
    )

    repairs_with_recent = repairs_needed.left_join(
        recent_prices,
        on={
            "item_token": "item_token",
            "route_key": "route_key",
            "station_key": "station_key",
        },
    )

    repairs_joined = repairs_with_recent.left_join(
        item_prices,
        on={"item_token": "item_token"},
    )

    repaired = (
        repairs_joined
        .add(
            final_cost=(
                ducktype.Numeric("recent_unit_cost")
                .coalesce(ducktype.Numeric("fallback_unit_cost"), 0)
                * ducktype.Numeric("quantity")
            )
        )
        .keep(
            "record_id",
            "item_token",
            "quantity",
            "final_cost",
            "route_hint",
            "station_hint",
        )
    )

    untouched_view = untouched.keep(
        "record_id",
        "item_token",
        "quantity",
        "final_cost",
        "route_hint",
        "station_hint",
    )

    return (
        untouched_view
        .union(repaired)
        .order_by("record_id")
    )


def iter_traceability_helpers() -> Iterable[Relation]:
    """Yield helper functions for documentation builds.

    Sphinx pulls code snippets via ``literalinclude`` using the tests.  This
    generator mirrors the structure of :mod:`duckplus.examples.sales_pipeline`
    so future guides can introspect the helper outputs without inspecting the
    implementation directly.
    """

    manager = DuckCon()
    with manager:
        demo = load_demo_relations(manager)
        yield rank_program_candidates(demo.program_catalog, demo.activity_log, "XYZ1-001")
        yield collect_panel_companions(demo.panel_events, demo.alternate_events, "XYZ1-001")
        yield repair_unit_costs(demo.unit_events, demo.price_snapshots)
