"""Temporal expression specification for metaclass-driven method synthesis."""

from __future__ import annotations

from typing import Final


SPEC: Final[dict[str, dict[str, object]]] = {
    "date_trunc": {
        "arguments": [
            {"name": "part", "kind": "positional_or_keyword", "annotation": "str"}
        ],
        "return": "TemporalExpr",
        "doc": "Truncate the timestamp expression to the specified date part.",
    },
    "extract": {
        "arguments": [
            {"name": "field", "kind": "positional_or_keyword", "annotation": "str"}
        ],
        "return": "NumericExpr",
        "doc": "Extract a numeric component (such as year or month) from the timestamp.",
    },
    "strftime": {
        "arguments": [
            {"name": "fmt", "kind": "positional_or_keyword", "annotation": "str"}
        ],
        "return": "StringExpr",
        "doc": "Render the timestamp expression using the supplied strftime format string.",
    },
    "between": {
        "arguments": [
            {"name": "lower", "kind": "positional_or_keyword", "annotation": "TemporalExpr"},
            {"name": "upper", "kind": "positional_or_keyword", "annotation": "TemporalExpr"},
        ],
        "return": "BooleanExpr",
        "doc": "Return whether the timestamp lies between the supplied bounds (inclusive).",
    },
}


__all__ = ["SPEC"]

