"""String expression specification for metaclass-driven method synthesis."""

from __future__ import annotations

from typing import Final

SPEC: Final[dict[str, dict[str, object]]] = {
    "strip": {
        "arguments": [
            {"name": "chars", "kind": "positional_or_keyword", "default": None, "annotation": "str | None"}
        ],
        "return": "StringExpr",
        "doc": "Return a string with leading and trailing characters removed.",
    },
    "split_part": {
        "arguments": [
            {"name": "separator", "kind": "positional_or_keyword", "annotation": "str"},
            {"name": "field", "kind": "positional_or_keyword", "annotation": "int"},
        ],
        "return": "StringExpr",
        "doc": "Split the string and return the requested field (1-indexed).",
    },
    "length": {
        "arguments": [],
        "return": "IntegerExpr",
        "doc": "Return the number of characters in the string expression.",
    },
    "like": {
        "arguments": [
            {"name": "pattern", "kind": "positional_or_keyword", "annotation": "str"}
        ],
        "return": "BooleanExpr",
        "doc": "Return whether the string matches the SQL LIKE pattern.",
    },
}

__all__ = ["SPEC"]
