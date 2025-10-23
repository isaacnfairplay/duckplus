from __future__ import annotations

from typing import get_type_hints

from duckplus.typed import (
    as_boolean_proto,
    as_numeric_proto,
    as_string_proto,
    as_temporal_proto,
    integer,
    predicate,
    timestamp,
    varchar,
)
from duckplus.typed.protocols import (
    BooleanExprProto,
    NumericExprProto,
    StringExprProto,
    TemporalExprProto,
)
from duckplus.typed.protocols.numeric import IntegerExprProto
from spec import boolean_spec, string_spec, temporal_spec


def test_protocol_methods_exposed() -> None:
    expr = as_string_proto(varchar("name"))
    result = expr.strip().split_part(" ", 1)
    length_expr = result.length()
    predicate_expr = expr.like("Jo%")
    timestamp_expr = as_temporal_proto(timestamp("created_at"))
    truncated = timestamp_expr.date_trunc("day")
    formatted = timestamp_expr.strftime("%Y-%m-%d")
    extracted = timestamp_expr.extract("year")
    range_check = timestamp_expr.between(truncated, truncated)
    assert hasattr(expr, "strip")
    assert hasattr(expr, "split_part")
    assert hasattr(expr, "length")
    assert hasattr(expr, "like")
    # length() returns a numeric protocol-compatible expression
    assert hasattr(length_expr, "abs")
    # like() returns a boolean protocol-compatible expression
    assert hasattr(predicate_expr, "negate")
    # temporal helpers bridge to the proper Protocol surfaces
    assert hasattr(truncated, "date_trunc")
    assert hasattr(formatted, "strip")
    assert hasattr(extracted, "abs")
    assert hasattr(range_check, "and_")


def test_spec_protocol_alignment() -> None:
    proto_methods = {
        name for name, value in StringExprProto.__dict__.items() if callable(value)
    }
    assert set(string_spec.SPEC) <= proto_methods


def test_boolean_spec_protocol_alignment() -> None:
    proto_methods = {
        name for name, value in BooleanExprProto.__dict__.items() if callable(value)
    }
    assert set(boolean_spec.SPEC) <= proto_methods


def test_temporal_spec_protocol_alignment() -> None:
    proto_methods = {
        name for name, value in TemporalExprProto.__dict__.items() if callable(value)
    }
    assert set(temporal_spec.SPEC) <= proto_methods


def test_protocol_signatures_cover_return_types() -> None:
    strip_hints = get_type_hints(StringExprProto.strip)
    split_hints = get_type_hints(StringExprProto.split_part)
    length_hints = get_type_hints(StringExprProto.length)
    like_hints = get_type_hints(StringExprProto.like)
    and_hints = get_type_hints(BooleanExprProto.and_)
    date_trunc_hints = get_type_hints(TemporalExprProto.date_trunc)
    extract_hints = get_type_hints(TemporalExprProto.extract)
    strftime_hints = get_type_hints(TemporalExprProto.strftime)
    between_hints = get_type_hints(TemporalExprProto.between)
    assert strip_hints["return"] is StringExprProto
    assert split_hints["return"] is StringExprProto
    assert length_hints["return"] is IntegerExprProto
    assert like_hints["return"] is BooleanExprProto
    assert and_hints["return"] is BooleanExprProto
    assert date_trunc_hints["return"] is TemporalExprProto
    assert extract_hints["return"] is NumericExprProto
    assert strftime_hints["return"] is StringExprProto
    assert between_hints["return"] is BooleanExprProto


def test_boolean_factory_roundtrip() -> None:
    raw = predicate("flag")
    bool_expr = as_boolean_proto(raw)
    chained = bool_expr.and_(bool_expr).negate()
    assert isinstance(chained, BooleanExprProto)


def test_temporal_factory_roundtrip() -> None:
    raw = timestamp("created_at")
    temporal_expr = as_temporal_proto(raw)
    chained = temporal_expr.date_trunc("day").strftime("%Y-%m-%d")
    assert isinstance(temporal_expr, TemporalExprProto)
    assert isinstance(chained, StringExprProto)


def test_numeric_factory_roundtrip() -> None:
    raw = integer("count")
    numeric_expr = as_numeric_proto(raw)
    assert isinstance(numeric_expr, NumericExprProto)
    assert isinstance(numeric_expr.abs(), NumericExprProto)

